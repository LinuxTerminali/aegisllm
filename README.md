# AegisLLM

**One quota, many agents, no 429s.**

When a fleet of AI agents shares one provider account, they all hit the rate limit at the same moment, retry together, and fail again. Work already done is wasted; customer-facing runs die. AegisLLM is a small proxy that sits between your agents and OpenAI or Anthropic, holds one view of the account's remaining quota, and lets requests through at the rate the provider actually allows – queued by priority, fair across runs, retried centrally. Agents never see a rate-limit error the scheduler could have avoided.

- Single Go binary, runs on your own machine or cluster. Prompts never leave your network except to the provider you already use.
- Setup is one URL change in your agent's config.
- Limits are learned from the provider's response headers. You don't need to know your RPM/TPM.
- Streaming supported.

## 60-second start

```bash
go install github.com/LinuxTerminali/aegisllm/cmd/aegis@latest   # or: docker build -t aegisllm .

# Anthropic
aegis -provider-type anthropic -upstream-url https://api.anthropic.com -upstream-api-key "$ANTHROPIC_API_KEY"

# OpenAI
aegis -provider-type openai -upstream-url https://api.openai.com -upstream-api-key "$OPENAI_API_KEY"
```

Then point your agents at `http://localhost:8080` instead of the provider. Per tool:

| Tool | Guide |
|---|---|
| Claude Code (many parallel sessions) | [docs/claude-code.md](docs/claude-code.md) |
| OpenAI SDK and OpenAI Agents SDK | [docs/openai-sdk.md](docs/openai-sdk.md) |
| Anthropic SDK | [docs/anthropic-sdk.md](docs/anthropic-sdk.md) |
| LangChain / LangGraph / CrewAI | [docs/langchain-langgraph.md](docs/langchain-langgraph.md) |
| Behind or in front of LiteLLM | [docs/litellm.md](docs/litellm.md) |

Watch it work: `curl localhost:8080/status | jq .` shows queue depth, who is waiting, quota left, and the limits it learned.

Try it with no API key: `aegis -run-mock` starts an embedded fake provider with a tiny rate limit; fire 50 requests at it and watch them all complete.

## What it does with a request

1. Reads two optional headers: `X-Run-ID` (which task this belongs to) and `X-Workload-Class` (`production`, `interactive`, `background`, `batch`) or an explicit `X-Priority`.
2. Estimates the request's token cost from the prompt and `max_tokens`.
3. If quota and a concurrency slot are free, forwards immediately. Otherwise queues it: higher classes first; within a class, fair round-robin across run IDs so one run with 40 subagents can't starve a run with 2.
4. A single dispatcher admits from the head of that queue, waiting for quota only for the head. A production request arriving mid-burst therefore waits for at most one pacing interval, not behind every batch request already in line.
5. Forwards to the provider with the proxy's API key. Retries 429/502/503/504 centrally with backoff and `Retry-After`. An upstream 429 drains the shared bucket so nothing else piles on.
6. On the response, reads the provider's rate-limit headers to correct its view of the quota, and reconciles the token estimate with real usage.

Streams are relayed event by event. A stream that has started is never re-issued.

## Results

Rate-limit benchmark: 100 requests, provider limited to 15 requests/second with burst 5, scheduler **not** told the limit.

| Mode | Success | Upstream 429s | Retries | Wall clock |
|---|---|---|---|---|
| Direct calls with retries and jitter | 57% | 43 | 213 | 3.8s |
| Through AegisLLM | 100% | 0 | 1 | 7.3s |

The theoretical minimum for this workload is 6.3s. With priority scheduling, `production` requests finish at p50 ≈ 2.1s while `batch` waits (p50 ≈ 6.6s). These numbers come from the embedded mock provider; reproduce with `go run ./cmd/benchmark -scenario rpm`.

## Why not LiteLLM?

LiteLLM solves "one app, many providers": routing, failover, virtual keys, spend. AegisLLM solves "many agents, one quota": learning the provider's limits, queuing by priority, fairness across runs, pacing so the limit is never hit, and holding the whole queue when the provider says `Retry-After`. LiteLLM counts and rejects; it does not queue, and its shared counters are per instance. The two stack: see [docs/litellm.md](docs/litellm.md).

## Configuration

Most deployments need only the first three.

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `-provider-type` | `SCHEDULER_PROVIDER_TYPE` | `openai` | `openai`, `anthropic`, `azure`, `generic` |
| `-upstream-url` | `SCHEDULER_UPSTREAM_URL` | `http://127.0.0.1:8081` | Provider base URL (`/v1` suffix optional) or a full Azure deployment URL |
| `-upstream-api-key` | `SCHEDULER_UPSTREAM_API_KEY` | | Provider key. When set, it replaces whatever key the client sends |
| `-listen-addr` | `SCHEDULER_LISTEN_ADDR` | `:8080` | Where agents connect |
| `-max-concurrency` | `SCHEDULER_MAX_CONCURRENCY` | `32` | In-flight requests to the provider |
| `-max-queue-size` | `SCHEDULER_MAX_QUEUE_SIZE` | `100` | Queued requests before new ones get 503 |
| `-max-queue-wait` | `SCHEDULER_MAX_QUEUE_WAIT` | `30s` | Longest a request waits for a slot and quota before 504 |
| `-rpm`, `-tpm` | `SCHEDULER_RPM`, `SCHEDULER_TPM` | learned | Override the limits learned from headers (required for Azure, which sends none). Burst defaults to the full minute; `-request-burst`/`-token-burst` to lower it |
| `-scheduling-policy` | `SCHEDULER_SCHEDULING_POLICY` | `fair` | `fair`, `priority`, `fifo` |
| `-max-pacing-delay` | `SCHEDULER_MAX_PACING_DELAY` | `0` | Set positive to reject with 429 instead of waiting when quota is short |

`aegis -help` lists the rest (retry backoff, burst sizes, class weights, mock provider).

## Operations

- `GET /healthz` liveness.
- `GET /status` JSON snapshot: queue, active, rejections by reason, wait by class, capacity, learned limits.
- `GET /metrics` Prometheus, bounded cardinality (no run IDs or prompts).
- Logs carry request ID, run ID, class, wait and upstream time. Never prompts, responses, or keys.

## Limits of this version

- **No authentication on the listener.** Anyone who can reach the port spends your provider key. Bind to localhost or a private network until client keys land (next milestone).
- **One instance.** Queue and quota state are in memory. Two instances behind a load balancer do not share a count; each sees the provider's `remaining` headers and backs off, but they will over-admit together. Multi-instance coordination is the next milestone.
- **Streams aren't retried after the first byte.** If the provider drops mid-stream, the client gets the truncated stream.
- **Token estimate is a heuristic** (≈4 chars/token plus `max_tokens`), corrected after each response. Good enough to pace; not exact.
- **Azure** needs `-rpm`/`-tpm` set by hand; its headers carry no limits.

## How this was built

With heavy AI assistance, and every line reviewed. The concurrency paths run under the race detector in CI, the benchmark is reproducible from the repo, and the bugs found so far are recorded in the commit history rather than hidden. Judge the code, not the tooling.

## Development

```bash
go test -race ./...
go run ./cmd/benchmark -scenario rpm      # rate-limit scenario
go run ./cmd/benchmark -scenario competing-runs
```

Against a real provider. Modes run one after another on the same account, so the harness pauses 65s between them (`-cooldown`) to let the provider's minute window refill; without that, each mode inherits the previous one's exhaustion. Use `-modes A,C` to compare just the baseline and the priority scheduler, and `-verbose` for a per-request timeline:

```bash
# Gemini free tier, OpenAI-compatible endpoint
BENCH_API_KEY=$GEMINI_API_KEY go run ./cmd/benchmark -scenario rpm -requests 25 -burst 10 \
  -provider-type generic -upstream-url https://generativelanguage.googleapis.com/v1beta/openai -model gemini-2.5-flash \
  -auto-learn=false -rate-limit-rps 0.25      # = 15 RPM; set to your tier's limit if the provider sends no limit headers

# OpenAI / Anthropic: limits are learned from headers
BENCH_API_KEY=$OPENAI_API_KEY    go run ./cmd/benchmark -scenario rpm -requests 50 -provider-type openai    -upstream-url https://api.openai.com    -model gpt-4o-mini
BENCH_API_KEY=$ANTHROPIC_API_KEY go run ./cmd/benchmark -scenario rpm -requests 50 -provider-type anthropic -upstream-url https://api.anthropic.com -model claude-3-5-haiku-latest
```

Provider smoke test against a real key: `OPENAI_API_KEY=sk-... go test -run TestRealProvider_Smoke ./internal/upstream`.

Apache 2.0. Contributions: see [CONTRIBUTING.md](CONTRIBUTING.md).
