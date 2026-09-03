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
}

func findScenario(name string) (scenario, bool) {
	for _, s := range scenarios {
		if s.Name == name {
			return s, true
		}
	}
	return scenario{}, false
}
