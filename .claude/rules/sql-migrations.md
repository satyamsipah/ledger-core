---
paths:
  - "migrations/**"
---

# Migration rules

- Every migration has a matching .down.sql that fully reverses it
- Never ALTER TABLE ... ADD COLUMN NOT NULL DEFAULT on a large table:
  add nullable, backfill in batches, then add the constraint
- Every index gets a comment explaining the query pattern it serves
- journal_entries stays append-only: no migration may grant UPDATE or DELETE
- The balance invariant trigger is DEFERRABLE INITIALLY DEFERRED. Do not
  change this without explaining why intermediate states would still pass
