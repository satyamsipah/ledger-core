---
name: Bug report
about: Something behaves differently than the code or the docs say it should
title: ""
labels: bug
assignees: ""
---

## What happened

A clear description of the actual behavior.

## What you expected

What the correct behavior should have been, and why — cite a
`docs/DECISIONS.md` entry or `CLAUDE.md` invariant if this is a
correctness bug, not just a preference.

## Is this a correctness bug?

If this touches balanced transactions, the append-only journal, integer
money, negative-balance protection, idempotent writes, or the outbox —
say so explicitly and mark it as high priority. These are the invariants
this project exists to protect.

## Steps to reproduce

```bash
# exact commands — make up / make seed / curl calls / test names — that
# reproduce this from a clean checkout
```

## Environment

- Commit / branch:
- `docker compose` stack or `go test` locally:
- OS / arch:

## Logs, error output, or a failing test

Paste the relevant `slog` JSON lines, the RFC 9457 problem document, or a
`go test` failure. If you can turn this into a failing test, that's worth
more than a description — see [CONTRIBUTING.md](../../CONTRIBUTING.md).
