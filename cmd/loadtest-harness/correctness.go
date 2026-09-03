package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/consistency"
	"github.com/satyamsipah/ledger-core/internal/projector"
)

// correctnessReport is this harness's own proof that a load run did not
// corrupt anything, run against the SAME functions cmd/reconciler -check and
// cmd/projector -rebuild wrap -- called directly here rather than shelled
// out to, since the harness already holds the pool this needs and a
// subprocess would only add a process boundary around the identical call.
//
// Four checks, not three: the three internal structural checks
// (docs/DECISIONS.md D49) plus the async-pipeline rebuild (D33) that D49's
// own text says is a DIFFERENT comparison -- CheckProjectionDrift diffs the
// journal against the synchronous account_balances the write path updates
// under lock, while Rebuild diffs it against balance_projections, the
// Kafka-driven read model. A load run exercises both write paths, so both
// need proving.
type correctnessReport struct {
	GlobalInvariant consistency.GlobalInvariantResult `json:"global_invariant"`
	ProjectionDrift consistency.DriftResult           `json:"projection_drift"`
	Orphans         consistency.OrphanResult          `json:"orphans"`
	Rebuild         projector.RebuildResult           `json:"async_pipeline_rebuild"`
	OK              bool                              `json:"ok"`
}

func runCorrectnessProof(ctx context.Context, pool *pgxpool.Pool) (correctnessReport, error) {
	invariant, err := consistency.CheckGlobalInvariant(ctx, pool)
	if err != nil {
		return correctnessReport{}, fmt.Errorf("check global invariant: %w", err)
	}
	drift, err := consistency.CheckProjectionDrift(ctx, pool)
	if err != nil {
		return correctnessReport{}, fmt.Errorf("check projection drift: %w", err)
	}
	orphans, err := consistency.CheckOrphans(ctx, pool)
	if err != nil {
		return correctnessReport{}, fmt.Errorf("check orphans: %w", err)
	}
	rebuild, err := projector.Rebuild(ctx, pool)
	if err != nil {
		return correctnessReport{}, fmt.Errorf("rebuild projection: %w", err)
	}

	return correctnessReport{
		GlobalInvariant: invariant,
		ProjectionDrift: drift,
		Orphans:         orphans,
		Rebuild:         rebuild,
		OK:              invariant.OK() && drift.OK() && orphans.OK() && rebuild.OK(),
	}, nil
}
