package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// promInstantQuery runs one PromQL instant query and returns the first
// result's value, or 0 if the query matched nothing -- an empty result is a
// legitimate answer for a counter that never incremented (e.g. zero
// serialization retries), not an error.
func promInstantQuery(ctx context.Context, promURL, query string) (float64, error) {
	u := promURL + "/api/v1/query?" + url.Values{"query": {query}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, fmt.Errorf("build prometheus query: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("query prometheus: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read prometheus response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus query %q: status %d: %s", query, resp.StatusCode, body)
	}

	var parsed struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, fmt.Errorf("parse prometheus response: %w", err)
	}
	if parsed.Status != "success" {
		return 0, fmt.Errorf("prometheus query %q did not succeed: %s", query, body)
	}
	if len(parsed.Data.Result) == 0 {
		return 0, nil
	}

	str, ok := parsed.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("prometheus query %q: unexpected value shape", query)
	}
	return strconv.ParseFloat(str, 64)
}

// waitForPipelineDrained polls the outbox backlog and the projector's
// consumer lag until both read zero, so the correctness proof that follows
// (in particular internal/projector.Rebuild, which diffs journal_entries
// against the KAFKA-DRIVEN projection) is comparing against a projection
// that has actually caught up with the load just generated -- not reporting
// a false mismatch for every event still in flight.
//
// Deliberately NOT gated on ledger_outbox_lag_seconds, despite that metric's
// own doc comment ("zero when nothing is unpublished") suggesting it belongs
// here. Under the Debezium arm (this stack's default, D31) that gauge is
// pg_stat_replication.replay_lag on the outbox replication slot -- the WAL
// PUBLISHER's confirmation lag, not a queue depth -- and it is bounded below
// by Kafka Connect's own `offset.flush.interval.ms` (60s by default), which
// governs how often Debezium's own offset commit advances the slot's
// confirmed position. It was observed, running this exact wait against a
// live stack, to sit at 40-80s for a full minute after every event had
// already been produced to Kafka AND consumed by the projector -- confirmed
// by cross-checking pg_stat_replication and the connector's own /status
// directly. Gating a timeout on it would make this function time out on a
// pipeline that had, in fact, already drained. ledger_projector_consumer_lag
// (real Kafka consumer-group lag on the topic the projector reads) is the
// signal that actually answers "has every event this run produced been
// consumed" for both publisher arms, and is what this function gates on
// instead. ledger_outbox_lag_seconds is still reported in the benchmark
// output -- it is a real and useful number, just not a drain signal.
func waitForPipelineDrained(ctx context.Context, promURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		backlog, err := promInstantQuery(ctx, promURL, `sum(ledger_outbox_backlog{job="outbox-publisher"})`)
		if err != nil {
			return fmt.Errorf("query outbox backlog: %w", err)
		}
		consumerLag, err := promInstantQuery(ctx, promURL, `sum(ledger_projector_consumer_lag{job="projector"})`)
		if err != nil {
			return fmt.Errorf("query consumer lag: %w", err)
		}
		if backlog == 0 && consumerLag == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pipeline did not drain within %s (outbox backlog=%.0f, consumer lag=%.0f)", timeout, backlog, consumerLag)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
