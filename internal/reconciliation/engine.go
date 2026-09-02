package reconciliation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/observability"
)

// DefaultTimingWindow is how far apart a ledger post and a PSP settlement may
// land and still be treated as the same event observed at two different
// clocks, rather than a real discrepancy. Two hours is generous enough to
// absorb ordinary settlement-file latency without hiding a same-day timing
// bug behind it.
const DefaultTimingWindow = 2 * time.Hour

// DefaultLookback bounds how far back Match considers ledger transactions
// when looking for a reference the PSP statement never mentions. A week is
// generous for a daily job: even several missed runs in a row still land
// inside it, while the scan stays bounded rather than rescanning the whole
// journal's external_ref history on every run.
const DefaultLookback = 7 * 24 * time.Hour

// Engine runs the three-way match and turns its output into a persisted run
// and its exceptions.
type Engine struct {
	store        Store
	logger       *slog.Logger
	metrics      *observability.Metrics
	timingWindow time.Duration
	lookback     time.Duration

	// now is a seam, not a feature: every timestamp this package produces
	// goes through it so a future fault-injection harness can skew this
	// process's clock without touching the OS clock, which would affect every
	// other process on the machine too. Defaults to time.Now and is never
	// overridden outside a test.
	now func() time.Time
}

// NewEngine wires an Engine to its store. timingWindow <= 0 uses
// DefaultTimingWindow; lookback <= 0 uses DefaultLookback.
func NewEngine(store Store, logger *slog.Logger, metrics *observability.Metrics, timingWindow, lookback time.Duration) *Engine {
	if timingWindow <= 0 {
		timingWindow = DefaultTimingWindow
	}
	if lookback <= 0 {
		lookback = DefaultLookback
	}
	return &Engine{
		store:        store,
		logger:       logger,
		metrics:      metrics,
		timingWindow: timingWindow,
		lookback:     lookback,
		now:          time.Now,
	}
}

// Run performs one three-way match against psp and persists the outcome:
// a completed run plus every exception it raised, or a failed run with no
// exceptions if the match itself could not be performed.
//
// source is recorded on the run for audit -- see migration 000017's comment
// on reconciliation_runs.source -- and is normally the file path this batch
// was read from.
//
// An empty psp is refused outright, before a run row is even created: a
// three-way match against zero PSP rows would report every ledger
// transaction in the lookback window as MISSING_IN_PSP, which is almost
// always a sign the wrong file was read, not a genuinely empty settlement day
// -- the same reasoning ParsePSPStatement already applies to the file itself,
// repeated here because a caller can hand Run a slice built by hand rather
// than through the parser.
func (e *Engine) Run(ctx context.Context, source string, psp []PSPRecord) (*Run, error) {
	if len(psp) == 0 {
		return nil, ErrEmptyStatement
	}

	runID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate reconciliation run id: %w", err)
	}

	startedAt := e.now()
	run := Run{
		ID:          runID,
		Source:      source,
		StartedAt:   startedAt,
		Status:      RunStatusRunning,
		PSPRowCount: len(psp),
	}
	if err := e.store.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("create reconciliation run: %w", err)
	}

	since := startedAt.Add(-e.lookback)
	matches, err := e.store.Match(ctx, psp, since)
	if err != nil {
		e.fail(ctx, runID, fmt.Errorf("three-way match: %w", err))
		return nil, fmt.Errorf("three-way match: %w", err)
	}

	var (
		exceptions   []Exception
		matchedCount int
		autoResolved int
	)
	for _, m := range matches {
		exc, clean := classify(m, e.timingWindow, e.now())
		if clean {
			matchedCount++
			continue
		}

		id, err := uuid.NewV7()
		if err != nil {
			e.fail(ctx, runID, fmt.Errorf("generate exception id: %w", err))
			return nil, fmt.Errorf("generate exception id: %w", err)
		}
		exc.ID = id
		exc.RunID = runID
		exc.CreatedAt = e.now()

		exceptions = append(exceptions, *exc)
		if exc.Status == ExceptionStatusAutoResolved {
			autoResolved++
		}
	}

	if err := e.store.SaveExceptions(ctx, exceptions); err != nil {
		e.fail(ctx, runID, fmt.Errorf("save reconciliation exceptions: %w", err))
		return nil, fmt.Errorf("save reconciliation exceptions: %w", err)
	}

	if err := e.store.FinishRun(ctx, runID, RunStatusCompleted, "", matchedCount, autoResolved, len(exceptions)); err != nil {
		return nil, fmt.Errorf("finish reconciliation run: %w", err)
	}

	byCategory := map[Category]int{}
	for _, exc := range exceptions {
		byCategory[exc.Category]++
	}

	if e.metrics != nil {
		e.metrics.ReconciliationRuns.WithLabelValues(string(RunStatusCompleted)).Inc()
		for category, count := range byCategory {
			e.metrics.ReconciliationExceptions.WithLabelValues(string(category)).Add(float64(count))
		}
	}

	finishedAt := e.now()
	e.logger.InfoContext(ctx, "reconciliation run completed",
		slog.String("run_id", runID.String()),
		slog.Int("psp_rows", len(psp)),
		slog.Int("matched", matchedCount),
		slog.Int("auto_resolved", autoResolved),
		slog.Int("exceptions", len(exceptions)))

	return &Run{
		ID:                runID,
		Source:            source,
		StartedAt:         startedAt,
		FinishedAt:        &finishedAt,
		Status:            RunStatusCompleted,
		PSPRowCount:       len(psp),
		MatchedCount:      matchedCount,
		AutoResolvedCount: autoResolved,
		ExceptionCount:    len(exceptions),
		ByCategory:        byCategory,
	}, nil
}

// fail records a run as FAILED and logs if even that could not be recorded.
// Called on every early return from Run past CreateRun, so a run that started
// never lingers in RUNNING forever -- migration 000017's check constraint
// requires exactly one of finished_at/RUNNING, and a run this function never
// finishes would otherwise be indistinguishable from one still in progress.
func (e *Engine) fail(ctx context.Context, runID uuid.UUID, cause error) {
	if err := e.store.FinishRun(ctx, runID, RunStatusFailed, cause.Error(), 0, 0, 0); err != nil {
		e.logger.ErrorContext(ctx, "record failed reconciliation run",
			slog.String("run_id", runID.String()), slog.String("error", err.Error()))
	}
	if e.metrics != nil {
		e.metrics.ReconciliationRuns.WithLabelValues(string(RunStatusFailed)).Inc()
	}
}
