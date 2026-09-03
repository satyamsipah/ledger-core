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

// promScrapeInterval mirrors deploy/prometheus/prometheus.yml's
// `global.scrape_interval`. Not read from that file -- there is no live
// config-sharing path between it and this binary -- but load-bearing enough
// (see waitForPipelineDrained's own comment) that it is named here rather
// than left as a bare literal.
const promScrapeInterval = 10 * time.Second

// waitForPipelineDrained polls the outbox backlog and the projector's
// consumer lag until both read zero TWICE, with a gap longer than
// Prometheus's own scrape interval between the two readings, so the
// correctness proof that follows (in particular internal/projector.Rebuild,
// which diffs journal_entries against the KAFKA-DRIVEN projection) is
// comparing against a projection that has actually caught up with the load
// just generated -- not reporting a false mismatch for an event still in
// flight.
//
// The double-check with a scrape-interval gap is not defensive
// over-engineering -- it fixes a real failure observed running this exact
// function against a live stack. A single zero reading is not proof of
// nothing in flight: Prometheus refreshes ledger_projector_consumer_lag only
// once per scrape_interval (10s), so a reading taken between scrapes can be
// up to 10s stale. mixed_realistic's own payouts (saga_heavy.js's shape,
// each ending in a SETTLE transaction whose BalanceUpdated event still has
// to travel outbox -> Kafka -> projector) kept producing new events for
// several seconds after the LAST scrape this function happened to poll,
// so a single zero read passed while the freshest SETTLE event was still
// unconsumed -- internal/projector.Rebuild then reported a real, reproducible
// mismatch on the exact two accounts (payout suspense and the customer
// wallet) that saga's SETTLE touches, off by exactly its own payout amount.
// Re-running cmd/projector -rebuild ~45s later, with no new load, showed a
// clean match -- confirming the pipeline HAD caught up and the earlier
// report was a race in this function, not real drift. Requiring the SAME
// zero result to hold across a gap longer than one scrape interval is what
// actually closes that race, rather than polling faster (which samples the
// same stale scrape value more often, not a fresher one).
//
// Deliberately NOT gated on ledger_outbox_lag_seconds, despite that metric's
// own doc comment ("zero when nothing is unpublished") suggesting it belongs
// here. Under the Debezium arm (this stack's default, D31) that gauge is
// pg_stat_replication.replay_lag on the outbox replication slot -- the WAL
// PUBLISHER's confirmation lag, not a queue depth -- and it is bounded below
// by Kafka Connect's own `offset.flush.interval.ms` (60s by default), which
// governs how often the connector's own offset commit advances the slot's
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
	consecutiveZero := 0
	const requiredConsecutiveZero = 2 // spans > one scrape interval; see doc comment

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
			consecutiveZero++
			if consecutiveZero >= requiredConsecutiveZero {
				return nil
			}
		} else {
			consecutiveZero = 0
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("pipeline did not drain within %s (outbox backlog=%.0f, consumer lag=%.0f)", timeout, backlog, consumerLag)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(promScrapeInterval + 2*time.Second):
		}
	}
}
