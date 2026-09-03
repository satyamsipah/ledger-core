---
name: Feature request
about: Propose new behavior or a new subsystem
title: ""
labels: enhancement
assignees: ""
---

## What problem does this solve

Describe the gap, not the solution yet. If it's one of the documented open
gaps (see README's "What this does NOT do, and why"), link it and explain
why it now matters for your use case.

## Proposed approach

Per [CONTRIBUTING.md](../../CONTRIBUTING.md): if there's a real trade-off
here (isolation level, locking strategy, partitioning key, and similar),
sketch at least two options with their costs rather than just the one you
prefer.

## What invariants does this touch, if any

Balanced transactions, append-only journal, integer money, negative-balance
protection, idempotent writes, no dual writes — call out explicitly if
this change interacts with any of them, and how it preserves them.

## Alternatives considered

What else could solve this, and why is it worse for this codebase
specifically?
