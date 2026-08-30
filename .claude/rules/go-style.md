---
paths:
  - "**/*.go"
---

# Go conventions

- Wrap errors with context: fmt.Errorf("credit account %s: %w", id, err)
- Domain errors are typed sentinels, never bare strings
- context.Context threaded everywhere; every DB call has a timeout
- No SQL in handlers. Layering is handler -> service -> repository
- Money is always int64 minor units plus a currency code. Never float64
- No global state. Dependencies injected via constructors
- Exported functions have doc comments explaining WHY, not what
