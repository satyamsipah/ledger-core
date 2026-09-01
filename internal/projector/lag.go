package projector

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/satyamsipah/ledger-core/internal/observability"
)

// LagReporter polls consumer group lag and exports it as a gauge.
//
// A separate loop from Consumer.Run rather than computed inline there, because
// lag is fundamentally an outside observer's measurement -- "how far behind is
// this group according to the broker's own bookkeeping" -- and computing it
// from PollFetches' own view of what has been consumed would answer a related
// but different question that happens to be cheaper to derive and easier to
// get quietly wrong.
type LagReporter struct {
	admin    *kadm.Client
	group    string
	interval time.Duration
	logger   *slog.Logger
	metrics  *observability.Metrics
}

// NewLagReporter builds a LagReporter. admin should be built over the same
// brokers the Consumer it is reporting on is reading from.
func NewLagReporter(admin *kadm.Client, group string, interval time.Duration, logger *slog.Logger, metrics *observability.Metrics) *LagReporter {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &LagReporter{admin: admin, group: group, interval: interval, logger: logger, metrics: metrics}
}

// Run polls until ctx is cancelled.
func (r *LagReporter) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.reportOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.reportOnce(ctx)
		}
	}
}

func (r *LagReporter) reportOnce(ctx context.Context) {
	lags, err := r.admin.Lag(ctx, r.group)
	if err != nil {
		r.logger.WarnContext(ctx, "calculate consumer lag", slog.String("error", err.Error()))
		return
	}

	described, ok := lags[r.group]
	if !ok {
		return
	}
	if err := described.Error(); err != nil {
		// A group with no committed offsets yet -- the projector's first run,
		// before it has consumed anything -- reports here rather than as a
		// hard failure, which is the correct read of "not ready" rather than
		// "broken."
		r.logger.DebugContext(ctx, "consumer group lag unavailable", slog.String("error", err.Error()))
		return
	}

	for topic, partitions := range described.Lag {
		for partition, memberLag := range partitions {
			if memberLag.Lag < 0 {
				continue // commit or list-offset error for this partition; skip rather than report a nonsense negative
			}
			r.metrics.ProjectorConsumerLag.
				WithLabelValues(topic, strconv.Itoa(int(partition))).
				Set(float64(memberLag.Lag))
		}
	}
}
