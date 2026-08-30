---
paths:
  - "**/*_test.go"
---

# Testing rules

- Use Testcontainers with a real Postgres. Never mock the database:
  the bugs that matter here live in DB behaviour
- Every write-path test asserts the global invariant after running
- Concurrency tests use at least 100 goroutines and run with -race
- Table-driven tests, named "should <expected> when <condition>"
- Failure tests actually kill things (Kafka, publisher, gateway); they do
  not simulate failure with a boolean flag
