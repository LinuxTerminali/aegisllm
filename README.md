# LLM Workload Scheduler

A high-performance, single-binary HTTP proxy written in Go that coordinates competing AI agent requests using bounded queues, priority scheduling, per-run fairness, capacity tracking, pacing, and retry handling.

---

## Architecture Overview

```
 [AI Agents]
      │  POST /v1/chat/completions (X-Run-ID, X-Workload-Class, X-Priority)
      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LLM Workload Scheduler                        │
│                                                                 │
│  ┌──────────────────┐    ┌─────────────────┐    ┌────────────┐  │
│  │ Admission Control│───▶│ Priority Queue  │───▶│ Concurrency│  │
│  │  (Cap: MaxQueue) │    │(Heap + FIFO tie)│    │  Limiter   │  │
│  └──────────────────┘    └─────────────────┘    └────────────┘  │
│                                                        │        │
└────────────────────────────────────────────────────────┼────────┘
                                                         │
                                                         ▼
                                       ┌───────────────────────────┐
                                       │   LLM Upstream Provider   │
                                       │   (Mock or Real OpenAI)   │
                                       └───────────────────────────┘
```

### Scheduling Semantics

1. **Priority Ordering**:
   Higher integer value indicates higher scheduling precedence. Default mapping from `X-Workload-Class`:
   - `production`: Priority **100**
   - `interactive`: Priority **75** (default if unspecified)
   - `background`: Priority **50**
   - `batch`: Priority **25**
   Callers may explicitly supply `X-Priority` (e.g. `X-Priority: 120`) to override or fine-tune priority.

2. **Strict FIFO Tie-Breaking**:
   When two requests share identical priority, a monotonic atomic sequence number assigned at enqueue time ensures strict FIFO ordering.

3. **Bounded Queuing & Admission Control**:
   - `max-concurrency`: Maximum active requests executing against upstream simultaneously.
   - `max-queue-size`: Maximum requests allowed in the wait queue. Requests arriving when the queue is at capacity are rejected immediately with HTTP 503 (`capacity_exceeded`).
   - `max-queue-wait`: Maximum wait deadline for a queued request. Exceeding this duration returns HTTP 504 (`timeout_error`).

4. **Cancellation Propagation & Zero Slot Leakage**:
   - Client disconnects while queued trigger immediate $O(1)$ removal from the priority heap via heap index tracking.
   - If a slot grant races with client cancellation, the slot is safely reclaimed and transferred to the next waiting request.

5. **Privacy & Security**:
   - Prompts, completion bodies, and API keys are **never logged by default**. Only structured operational metadata (`req_id`, `run_id`, `priority`, `wait_ms`, `upstream_ms`, `status`) is logged.

---

## Quickstart

### 1. Build the Binary

```bash
go build -o bin/scheduler ./cmd/scheduler
go build -o bin/mockupstream ./cmd/mockupstream
```

### 2. Start the Scheduler with Embedded Mock Upstream

Start both the proxy (port `8080`) and the embedded mock provider (port `8081`) with a single command:

```bash
./bin/scheduler -run-mock -listen-addr :8080 -mock-addr :8081 -max-concurrency 5 -max-queue-size 100
```

### 3. (Alternative) Run Standalone Mock Upstream

If you prefer running the mock upstream as a separate process:

```bash
# Terminal 1: Start mock upstream
./bin/mockupstream -listen-addr :8081 -latency 50ms

# Terminal 2: Start scheduler proxy pointing to mock
./bin/scheduler -listen-addr :8080 -upstream-url http://127.0.0.1:8081 -max-concurrency 5
```

---

## Verifying the Endpoints

### Health Check (`GET /healthz`)
```bash
curl -s http://127.0.0.1:8080/healthz
# {"status":"ok"}
```

### Operational Status (`GET /status`)
```bash
curl -s http://127.0.0.1:8080/status | jq .
# {
#   "queued_requests": 0,
#   "active_requests": 0,
#   "max_queue_capacity": 100,
#   "max_concurrency": 5,
#   "total_requests": 14,
#   "total_admitted": 14,
#   "total_completed": 14,
#   "total_rejected": 0,
#   "total_canceled": 0,
#   "total_timed_out": 0
# }
```

### Chat Completions (`POST /v1/chat/completions`)
```bash
curl -i -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-Run-ID: agent-run-abc" \
  -H "X-Workload-Class: production" \
  -d '{
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

Notice the response headers injected by the scheduler:
```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
X-Scheduler-Run-ID: agent-run-abc
X-Scheduler-Priority: 100
X-Scheduler-Wait-Ms: 0
```

---

## Running Tests

Run the full unit and integration test suite with race detection enabled:

```bash
go test -v -race ./...
```

Run code formatting and static analysis:

```bash
gofmt -s -w .
go vet ./...
```
