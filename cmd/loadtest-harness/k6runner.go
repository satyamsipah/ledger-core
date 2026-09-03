package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

// k6MetricValues is the union of every shape k6's --summary-export gives a
// metric, keyed loosely because which fields are populated depends on the
// metric's own type (trend vs. rate vs. counter) and k6 does not tag that
// distinction anywhere else in the document.
type k6MetricValues struct {
	Value *float64 `json:"value"`
	Count *float64 `json:"count"`
	Rate  *float64 `json:"rate"`
	Avg   *float64 `json:"avg"`
	Min   *float64 `json:"min"`
	Med   *float64 `json:"med"`
	Max   *float64 `json:"max"`
	P50   *float64 `json:"p(50)"`
	P95   *float64 `json:"p(95)"`
	P99   *float64 `json:"p(99)"`
}

type k6Summary struct {
	Metrics map[string]k6MetricValues `json:"metrics"`
}

func (v k6MetricValues) f(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// runK6 runs one scenario's script to completion and returns its parsed
// summary plus whether every threshold passed.
//
// Threshold pass/fail is read from k6's own EXIT CODE, not from the
// "thresholds" object --summary-export embeds per metric: that object's
// boolean does not mean what its name suggests (a passing run's own
// thresholds are reported as false, confirmed by running this scenario
// against a live stack and comparing the JSON to k6's own "✓ PASSED" CLI
// output for the identical run) and trusting it silently would make a
// regressed run report itself clean. The exit code is what k6's own
// documentation commits to, and it is what CI would act on if this harness
// were wired into one.
//
// A non-zero exit is not treated as fatal to the whole harness run: k6 still
// writes --summary-export on a threshold failure (data collection is not
// what failed), so the summary is parsed and returned alongside the failure
// rather than discarded, and the caller decides what a failed scenario means
// for the rest of the report.
func runK6(ctx context.Context, k6Bin string, s scenario, env map[string]string, summaryPath string) (k6Summary, bool, error) {
	args := []string{"run", s.Script, "--summary-export", summaryPath}
	for k, v := range env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	//nolint:gosec // G204: k6Bin and args come from this process's own CLI
	// flags and scenario registry, never from network or user-request input --
	// this binary is a developer-run benchmarking tool, not a network-facing
	// service.
	cmd := exec.CommandContext(ctx, k6Bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	runErr := cmd.Run()

	//nolint:gosec // G304: summaryPath is built by this process itself
	// (filepath.Join of its own temp dir and scenario name), never external
	// input.
	data, err := os.ReadFile(summaryPath)
	if err != nil {
		// Both %w: Go 1.20+ allows more than one, and both are genuinely
		// this failure's causes -- the summary read failing IS the error
		// returned, but k6's own exit (if non-nil) is what makes it
		// diagnosable rather than a bare "file not found".
		return k6Summary{}, false, fmt.Errorf("read k6 summary %s: %w (k6 run error: %w)", summaryPath, err, runErr)
	}

	var summary k6Summary
	if err := json.Unmarshal(data, &summary); err != nil {
		return k6Summary{}, false, fmt.Errorf("parse k6 summary %s: %w", summaryPath, err)
	}

	return summary, runErr == nil, nil
}
