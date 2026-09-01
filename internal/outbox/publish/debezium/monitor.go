// Package debezium implements the outbox Publisher that does not publish
// anything itself.
//
// When LEDGER_OUTBOX_PUBLISHER=debezium, the actual work -- reading the
// write-ahead log, routing outbox rows onto Kafka topics -- is done entirely
// by the Debezium connector registered against Kafka Connect
// (deploy/debezium/outbox-connector.json, applied by the connect-init compose
// service). That happens whether or not this package's Monitor is running.
// What Monitor supplies is the other half of "config flag to switch": a
// process that reports, honestly, on the health of the mechanism actually
// responsible, so choosing this arm of the flag does not mean choosing to fly
// blind. See docs/DECISIONS.md D31.
package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"sync/atomic"
	"time"
)

// atomicError is an atomically-swappable error, nil-safe in both directions.
// sync/atomic has no built-in error-shaped type, and wrapping this in one
// small type here beats reaching for atomic.Pointer[error] and its
// nil-vs-nil-pointer-to-nil-interface footgun at every call site.
type atomicError struct {
	v atomic.Pointer[error]
}

func (a *atomicError) Store(err error) { a.v.Store(&err) }

func (a *atomicError) Load() error {
	p := a.v.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Config points Monitor at the connector it watches.
type Config struct {
	// ConnectURL is Kafka Connect's REST API base, e.g. http://connect:8083.
	ConnectURL string

	// ConnectorName is the connector to watch -- "ledger-outbox" by
	// convention, matching the name connect-init registers it under.
	ConnectorName string

	// PollInterval between status checks.
	PollInterval time.Duration
}

// connectorStatus is the subset of Kafka Connect's
// GET /connectors/{name}/status response this package reads.
type connectorStatus struct {
	Name      string `json:"name"`
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
	} `json:"tasks"`
}

// Monitor watches one Debezium connector's health.
//
// It also implements the ledgerhttp.Checker shape (Name, Check) so it can be
// registered on /readyz alongside the database and any other dependency: a
// process configured for the Debezium arm that reports itself ready while the
// connector it depends on is down would be lying about the one thing that
// matters for this configuration.
type Monitor struct {
	client *nethttp.Client
	cfg    Config
	logger *slog.Logger

	// lastErr is read by Check and written by Run's poll loop. It is not
	// protected by a mutex because both sides do a single atomic pointer-sized
	// read or write of an interface value -- Go does not guarantee atomicity
	// for that without sync/atomic, so this is upgraded to atomic.Pointer
	// rather than left as a plain field the moment a data race detector
	// objects, which it will, on purpose, so it does not go unnoticed.
	lastErr atomicError
}

// New builds a Monitor. A default *http.Client with a bounded per-request
// timeout, not http.DefaultClient: a status check that can hang forever is
// worse than one that fails fast and retries next tick.
func New(cfg Config, logger *slog.Logger) *Monitor {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Monitor{
		client: &nethttp.Client{Timeout: 5 * time.Second},
		cfg:    cfg,
		logger: logger,
	}
}

// Name identifies this dependency in readiness output.
func (m *Monitor) Name() string { return "debezium:" + m.cfg.ConnectorName }

// Check reports the connector's last-observed health. It does not make its
// own HTTP request -- Run's poll loop already does that on its own schedule,
// and a readiness probe firing a second, independent network call on top of
// it would mean two different code paths deciding whether Kafka Connect is
// reachable, which can disagree.
func (m *Monitor) Check(context.Context) error {
	return m.lastErr.Load()
}

// Run polls the connector's status until ctx is cancelled.
//
// A connector reported DOWN, or an unreachable Kafka Connect, is logged and
// recorded for Check -- never fatal to this process. The Debezium connector's
// own restart policy (deploy/docker-compose.yml's connect-init sets
// restart: on-failure) is what recovers it; this process's job is only to say
// honestly whether that has happened yet.
func (m *Monitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	m.logger.Info("debezium connector monitor started",
		slog.String("connect_url", m.cfg.ConnectURL),
		slog.String("connector", m.cfg.ConnectorName),
		slog.Duration("interval", m.cfg.PollInterval))

	m.checkOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("debezium connector monitor stopped")
			return nil
		case <-ticker.C:
			m.checkOnce(ctx)
		}
	}
}

func (m *Monitor) checkOnce(ctx context.Context) {
	err := m.fetchStatus(ctx)

	wasHealthy := m.lastErr.Load() == nil
	m.lastErr.Store(err)

	switch {
	case err != nil && wasHealthy:
		m.logger.WarnContext(ctx, "debezium connector unhealthy",
			slog.String("connector", m.cfg.ConnectorName),
			slog.String("error", err.Error()))
	case err == nil && !wasHealthy:
		m.logger.InfoContext(ctx, "debezium connector recovered",
			slog.String("connector", m.cfg.ConnectorName))
	}
}

func (m *Monitor) fetchStatus(ctx context.Context) error {
	url := m.cfg.ConnectURL + "/connectors/" + m.cfg.ConnectorName + "/status"

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build connector status request: %w", err)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("reach kafka connect at %s: %w", m.cfg.ConnectURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != nethttp.StatusOK {
		return fmt.Errorf("connector %s status endpoint returned %d", m.cfg.ConnectorName, resp.StatusCode)
	}

	var status connectorStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return fmt.Errorf("decode connector status: %w", err)
	}

	if status.Connector.State != "RUNNING" {
		return fmt.Errorf("connector %s is %s", m.cfg.ConnectorName, status.Connector.State)
	}
	for _, task := range status.Tasks {
		if task.State != "RUNNING" {
			return fmt.Errorf("connector %s task %d is %s", m.cfg.ConnectorName, task.ID, task.State)
		}
	}

	return nil
}
