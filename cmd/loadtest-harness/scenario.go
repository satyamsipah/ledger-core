package main

// scenario names one k6 script this harness knows how to run, plus what it
// needs to find that scenario's own tagged latency metric in k6's summary
// export. New scenarios are added here and nowhere else -- everything from
// this point on (running k6, waiting for the pipeline to drain, proving
// correctness, writing the report) is generic over the list.
type scenario struct {
	Name        string
	Script      string
	EndpointTag string
	Description string
}

// scenarios is the full registry. Phase 7 adds to this list one scenario at
// a time, each verified end to end before the next is written, rather than
// building all five blind and discovering which ones work only once every
// script exists.
var scenarios = []scenario{
	{
		Name:        "baseline_simple_transfer",
		Script:      "test/load/baseline_simple_transfer.js",
		EndpointTag: "post_transaction",
		Description: "Two fixed accounts, moderate steady-state rate. Every other scenario is read against this one.",
	},
	{
		Name:        "hot_account",
		Script:      "test/load/hot_account.js",
		EndpointTag: "post_transaction",
		Description: "Same rate and shape as baseline_simple_transfer; 90% of traffic credits one account instead of spreading out, isolating row-lock contention's cost (D11).",
	},
	{
		Name:        "idempotent_retry_storm",
		Script:      "test/load/idempotent_retry_storm.js",
		EndpointTag: "post_transaction",
		Description: "30% of requests replay an exact earlier (key, body) pair from the same VU, exercising the idempotency read path (D20) instead of PostTransaction.",
	},
	{
		Name:        "saga_heavy",
		Script:      "test/load/saga_heavy.js",
		EndpointTag: "start_payout",
		Description: "Full RESERVE/GATEWAY/SETTLE marketplace payouts against a gateway failing 5% of calls ambiguously; each iteration polls the saga to a settled state rather than trusting the 202.",
	},
	{
		Name:   "mixed_realistic",
		Script: "test/load/mixed_realistic.js",
		// Empty: this scenario tags every request by which of its four
		// sub-shapes produced it (mixed_transfer/mixed_hot/mixed_retry/
		// mixed_payout), so there is no single tagged submetric that
		// represents "the mix" -- the untagged http_req_duration, which pools
		// all four, is the honest answer for a blended scenario's own summary
		// row.
		EndpointTag: "",
		Description: "A weighted concurrent blend of the other four scenarios (60% transfers, 20% hot-account, 15% idempotent retries, 5% payouts) via k6's own multi-scenario executor.",
	},
}

func findScenario(name string) (scenario, bool) {
	for _, s := range scenarios {
		if s.Name == name {
			return s, true
		}
	}
	return scenario{}, false
}
