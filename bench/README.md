# HTTP Benchmark Baseline

`bench/http` is a dependency-free Go client for a repeatable HTTP latency baseline. It does not use real model providers, Kafka, or a production database. The default `/ping` profile isolates HTTP middleware and gateway overhead; API profiles can be supplied with a JWT and request body when a local service is running.

## Run

Start GoAI with the local infrastructure configured, then run:

```bash
go run ./bench/http -url http://127.0.0.1:8080/ping -requests 1000 -concurrency 10
```

The command emits JSON with:

- `requests`, `concurrency`
- `successes`, `failures`, `status_counts`
- `elapsed_ns`
- `requests_per_second`
- `p50_ns`, `p95_ns`, `p99_ns`

The command exits non-zero if any request fails or returns a non-2xx status. `-header` is repeatable, so a protected API can be tested without putting credentials in source control:

```bash
go run ./bench/http \
  -url http://127.0.0.1:8080/api/runs \
  -method POST \
  -body '{"agent_code":"planner","workflow_version":1,"input":{"prompt":"bench"}}' \
  -header 'Authorization=Bearer <jwt>' \
  -header 'Idempotency-Key=bench-001' \
  -requests 100 -concurrency 5
```

Use a new `Idempotency-Key` per request when benchmarking Run creation semantics, or benchmark `/ping` for a stable transport-only baseline. Do not use real secrets in command history or committed files.

## Baseline Interpretation

Record the JSON output together with:

- Go version and commit SHA
- host CPU/memory
- request URL, method, body profile and concurrency
- whether the target used local loopback HTTP or a remote HTTPS endpoint
- whether the request exercised only the Gateway or also DB/Kafka/Agent execution

The numbers are environment-specific and are not CI pass/fail thresholds. The reproducible artifact is the command and JSON schema; rerunning the same profile on the same environment provides the comparison baseline for future optimizations.

Protocol endpoints should be benchmarked separately:

- `/ping` measures HTTP middleware overhead.
- AG-UI requires a seeded Agent and emits an SSE stream, so report time-to-first-event and stream duration separately in a future gateway benchmark.
- A2A `message:send` includes persistence and asynchronous child Run creation; it must not be compared directly with `/ping`.
