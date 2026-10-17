# AegisLLM 🛡️

[![Go Report Card](https://goreportcard.com/badge/github.com/LinuxTerminali/aegisllm)](https://goreportcard.com/report/github.com/LinuxTerminali/aegisllm)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/LinuxTerminali/aegisllm)](go.mod)

A high-performance, single-binary HTTP proxy written in Go that shields LLM providers from multi-agent workloads using bounded queues, priority scheduling, hierarchical Weighted Deficit Round-Robin (WDRR) fairness across runs, RPM/TPM capacity pacing, token estimation, coordinated retries, and provider adapters.

---

## Architecture Overview

```
 [AI Agents / Callers]
      │  POST /v1/chat/completions (X-Run-ID, X-Workload-Class, X-Priority)
      ▼
┌────────────────────────────────────────────────────────────────────────┐
│                                AegisLLM                                │
│                                                                        │
│  ┌──────────────────────┐    ┌──────────────────┐    ┌───────────────┐ │
│  │  Admission Control   │───▶│ Fair Queue (WDRR)│───▶│ Concurrency & │ │
│  │ (Cap: MaxQueueSize)  │    │or Priority Heap  │    │ RPM/TPM Pacer │ │
│  └──────────────────────┘    └──────────────────┘    └───────────────┘ │
│                                                              │         │
│  ┌──────────────────────┐    ┌──────────────────┐            │         │
│  │  Bounded Observability│◀───│ Coordinated Retry│◀───────────┘         │
│  │  (/status, /metrics) │    │ & Reconciliation │                      │
│  └──────────────────────┘    └──────────────────┘                      │
└───────────────────────────────────────┬────────────────────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │    Upstream LLM Provider    │
                         │ (OpenAI, Anthropic, Azure,  │
                         │  vLLM, Ollama, Mock Server) │
                         └─────────────────────────────┘
```

---

## Core Scheduling Semantics

1. **Priority Ordering**:
   Higher integer value indicates higher scheduling precedence. Default mapping from `X-Workload-Class`:
   - `production`: Priority **100** (Weight: 8x)
   - `interactive`: Priority **75** (Weight: 4x, default if unspecified)
   - `background`: Priority **50** (Weight: 2x)
   - `batch`: Priority **25** (Weight: 1x)
   Explicit `X-Priority` headers override class defaults.

2. **Hierarchical WDRR Fairness Across Runs**:
   When configured with `-scheduling-policy fair` (default), requests are scheduled by priority class weights. Within each priority class, distinct `X-Run-ID` values receive fair Deficit Round-Robin service, preventing high-volume agent runs from starving concurrent runs.

3. **RPM and TPM Capacity Pacing**:
   Dual token-bucket rate limiter tracks both Requests Per Minute (RPM) and Tokens Per Minute (TPM). Incoming requests reserve estimated tokens (via prompt heuristic estimation). When quota is depleted, requests are paced up to `-max-pacing-delay`. If deadline cannot be met, request is rejected with HTTP 429 (`rate_limit_exceeded`).

4. **Coordinated Retries & Token Reconciliation**:
   Transient errors (HTTP 429, 502, 503, 504) are retried with exponential backoff and full random jitter, honoring upstream `Retry-After` headers. Non-retryable client errors (400, 401, 403, 404) fail fast without retry. On successful response, provider token usage (`usage.total_tokens`) reconciles initial reservations.

5. **Security & Credential Safety**:
   - Prompts, completion messages, and API keys are **never logged**.
   - Upstream error messages are scrubbed to prevent leaking secret keys (`sk-...`, `Bearer ...`).
   - Hop-by-hop and internal headers (`X-Run-ID`, `X-Priority`, `X-Scheduler-*`) are stripped before forwarding.

---

## Provider Compatibility Layer

The proxy provides explicit provider adapters via `-provider-type`:

| Provider Type | Identifier | Default Auth Header | Quota Headers Parsed |
| :--- | :--- | :--- | :--- |
| **OpenAI** (default) | `openai` | `Authorization: Bearer <key>` | `x-ratelimit-remaining-*`, `x-ratelimit-reset-*` |
| **Anthropic** | `anthropic` | `x-api-key: <key>`, `anthropic-version` | `anthropic-ratelimit-*` |
| **Azure OpenAI** | `azure` | `api-key: <key>` | `x-ratelimit-*`, Azure standards |
| **Generic / Local** | `generic` | Configurable (`-auth-header-name`) | Standard IETF `RateLimit-*` & OpenAI-style |

### Defensive Quota Handling & Shared-Key Limits
- Quota headers are treated as **advisory signals**, not authoritative truth.
- Contradictory, malformed, or absurd quota headers are safely ignored without corrupting capacity tracking.
- **Shared-Key Notice**: In multi-replica deployments or shared accounts, upstream rate limits apply globally to the API key or organization, not exclusively to this proxy instance. Downward synchronization hints adjust local buckets when upstream reports lower remaining quota.

---

## Operational Observability

### 1. Health Liveness (`GET /healthz`)
```bash
curl -s http://127.0.0.1:8080/healthz
# {"status":"ok"}
```

### 2. Operational Status (`GET /status`)
Returns JSON runtime snapshot of queues, active concurrency, token buckets, and latency:
```bash
curl -s http://127.0.0.1:8080/status | jq .
```
Example fields:
- `queued_requests`, `active_requests`, `max_concurrency`, `max_queue_capacity`
- `total_requests`, `total_admitted`, `total_completed`, `total_rejected`
- `metrics.queue_wait_by_class`: breakdown of wait times by priority class
- `metrics.rejections_by_reason`: `queue_full`, `capacity_exceeded`, `queue_timeout`
- `capacity`: available request tokens and token tokens

### 3. Prometheus Metrics (`GET /metrics`)
Exposes standard Prometheus exposition format with **strictly bounded cardinality** (no run IDs, prompt texts, user IDs, or API keys):
- `scheduler_requests_total{status="accepted|completed|failed|canceled|rejected"}`
- `scheduler_rejections_total{reason="queue_full|capacity_exceeded|queue_timeout"}`
- `scheduler_retries_total`
- `upstream_responses_total{status_class="2xx|4xx|5xx", code="429|502|503|504"}`
- `scheduler_active_concurrency`, `scheduler_max_concurrency`
- `scheduler_queued_requests`, `scheduler_max_queue_size`
- `scheduler_capacity_requests_available`, `scheduler_capacity_tokens_available`
- `scheduler_queue_wait_ms_total{priority="production|interactive|background|batch"}`

---

## Configuration Reference

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-listen-addr` | `SCHEDULER_LISTEN_ADDR` | `:8080` | Proxy listen address |
| `-upstream-url` | `SCHEDULER_UPSTREAM_URL` | `http://127.0.0.1:8081` | Upstream provider base URL |
| `-upstream-api-key` | `SCHEDULER_UPSTREAM_API_KEY` | `""` | Upstream provider secret key |
| `-provider-type` | `SCHEDULER_PROVIDER_TYPE` | `openai` | Adapter: `openai`, `anthropic`, `azure`, `generic` |
| `-auth-header-name`| `SCHEDULER_AUTH_HEADER_NAME` | `""` | Custom auth header name (generic adapter) |
| `-default-model` | `SCHEDULER_DEFAULT_MODEL` | `""` | Fallback model if omitted in request |
| `-upstream-timeout`| `SCHEDULER_UPSTREAM_TIMEOUT` | `60s` | Upstream HTTP request timeout |
| `-max-concurrency` | `SCHEDULER_MAX_CONCURRENCY` | `5` | In-flight execution concurrency limit |
| `-max-queue-size` | `SCHEDULER_MAX_QUEUE_SIZE` | `100` | Max queued requests before rejection (503) |
| `-max-queue-wait` | `SCHEDULER_MAX_QUEUE_WAIT` | `30s` | Max queue wait deadline before timeout (504) |
| `-scheduling-policy`| `SCHEDULER_SCHEDULING_POLICY`| `fair` | Queue policy: `fair` (WDRR), `priority`, `fifo` |
| `-rpm` | `SCHEDULER_RPM` | `0` | Max requests per minute quota (0 = unconstrained) |
| `-request-burst` | `SCHEDULER_REQUEST_BURST` | `0` | RPM token bucket burst allowance |
| `-tpm` | `SCHEDULER_TPM` | `0` | Max tokens per minute quota (0 = unconstrained) |
| `-token-burst` | `SCHEDULER_TOKEN_BURST` | `0` | TPM token bucket burst allowance |
| `-max-pacing-delay`| `SCHEDULER_MAX_PACING_DELAY` | `500ms` | Max sleep pacing delay before 429 rejection |
| `-max-retries` | `SCHEDULER_MAX_RETRIES` | `3` | Coordinated retries on transient errors |
| `-initial-backoff` | `SCHEDULER_INITIAL_BACKOFF` | `50ms` | Base retry backoff |
| `-max-backoff` | `SCHEDULER_MAX_BACKOFF` | `2s` | Max retry backoff ceiling |
| `-run-mock` | `SCHEDULER_RUN_MOCK` | `false` | Run embedded mock upstream |
| `-mock-addr` | `SCHEDULER_MOCK_ADDR` | `:8081` | Address for embedded mock provider |
| `-mock-latency` | `SCHEDULER_MOCK_LATENCY` | `30ms` | Mock provider inference latency |

---

## Quickstart & Local Execution

### 1. Build Binaries
```bash
go build -o bin/aegis ./cmd/aegis
go build -o bin/mockupstream ./cmd/mockupstream
go build -o bin/benchmark ./cmd/benchmark
```

### 2. Run with Embedded Mock Upstream
```bash
./bin/aegis -run-mock -listen-addr :8080 -mock-addr :8081 -max-concurrency 5 -max-queue-size 100 -rpm 300 -request-burst 30
```

### 3. Run with Real OpenAI Provider
```bash
export OPENAI_API_KEY="sk-..."
./bin/aegis \
  -listen-addr :8080 \
  -upstream-url https://api.openai.com/v1 \
  -upstream-api-key "$OPENAI_API_KEY" \
  -provider-type openai \
  -max-concurrency 10 \
  -max-queue-size 200 \
  -rpm 500 \
  -tpm 200000
```

### 4. Running via Docker
```bash
docker build -t aegisllm .
docker run -p 8080:8080 aegisllm -run-mock -mock-addr :8081
```

---

## Real Provider Smoke Test

An opt-in integration test validates live communication with a real LLM provider. It sends minimal tokens (`max_tokens: 3`) to verify:
- Successful completion and usage token reporting
- Error propagation and secret redaction
- Cancellation propagation

To execute against OpenAI:
```bash
OPENAI_API_KEY="sk-..." go test -v -run TestRealProvider_Smoke ./internal/upstream
```
If `OPENAI_API_KEY` (or `PROVIDER_TEST_API_KEY`) is not set, the test skips cleanly in 0.00s.

---

## Running Benchmarks

Run the deterministic benchmark harness across supported scenarios:

```bash
# Mixed priority competition
go run ./cmd/benchmark -scenario mixed-priorities -requests 100 -burst 40

# Competing agent runs with heavy vs light workloads
go run ./cmd/benchmark -scenario competing-runs -requests 100 -burst 40

# Client deadline cancellation under queue load
go run ./cmd/benchmark -scenario cancellation -requests 100 -burst 40

# Token bucket RPM rate-limit enforcement
go run ./cmd/benchmark -scenario rpm -requests 100 -burst 20

# Token bucket TPM rate-limit enforcement
go run ./cmd/benchmark -scenario tpm -requests 100 -burst 20

# Mixed prompt lengths (short vs long tokens)
go run ./cmd/benchmark -scenario token-mix -requests 100 -burst 20
```

---

## Production Limitations & Deployment Notice

1. **Single-Instance In-Memory State**:
   - Queues, rate-limit token buckets, and WDRR run deficit counters reside strictly in memory.
   - All queue state and fairness histories are lost upon restart.
2. **Multi-Replica Isolation**:
   - Multiple scheduler replicas do **not** coordinate rate limits or queues across instances. If deploying behind a load balancer, each replica operates independently. Upstream quota is shared across replicas only via downward response header synchronization.
3. **Streaming Unsupported**:
   - Streaming completions (`"stream": true` SSE) cannot be safely replayed on retry once initial chunks have been delivered to the client, and streaming token reconciliation is not yet supported. The proxy currently coordinates unary requests.
