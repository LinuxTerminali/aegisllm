# Claude Code through AegisLLM

Use this when you run several Claude Code sessions or subagents at once on one API key and they trip Anthropic's rate limits together.

This path requires **API-key billing**. A Claude subscription login authenticates differently and is not covered here.

## 1. Start the proxy

```bash
aegis -provider-type anthropic \
      -upstream-url https://api.anthropic.com \
      -upstream-api-key "$ANTHROPIC_API_KEY"
```

The real key stays with the proxy. Sessions get a placeholder.

## 2. Point Claude Code at it

```bash
export ANTHROPIC_BASE_URL=http://localhost:8080
export ANTHROPIC_API_KEY=placeholder          # any non-empty value; the proxy swaps in the real key
export ANTHROPIC_CUSTOM_HEADERS=$'X-Run-ID: nightly-refactor\nX-Workload-Class: background'
claude -p "Refactor the auth module and run the tests"
```

`ANTHROPIC_CUSTOM_HEADERS` takes newline-separated `Name: Value` pairs; Claude Code adds them to every API call, including those from subagents it spawns, because child processes inherit the environment. That is what groups a whole session, with all its subagents, under one run ID.

Put the same three variables in `.claude/settings.json` under `"env"` if you prefer them per project.

## 3. Run many sessions

```bash
for task in "fix flaky tests" "add OpenAPI docs" "upgrade deps"; do
  ANTHROPIC_CUSTOM_HEADERS="X-Run-ID: $(echo "$task" | tr ' ' '-')"$'\n'"X-Workload-Class: background" \
    claude -p "$task" > "out-$(echo "$task" | tr ' ' '-').log" 2>&1 &
done
wait
```

Each session is its own run. The proxy shares the account's quota across them fairly and finishes them instead of letting them crash into the limit. Give a session you're interacting with `X-Workload-Class: interactive` so it jumps ahead of background batches.

See the proxy's view while they run:

```bash
watch -n1 'curl -s localhost:8080/status | jq "{queued: .queued_requests, active: .active_requests, rpm: .capacity.requests_per_minute, tpm: .capacity.tokens_per_minute, learned: .capacity.limits_learned}"'
```

## Notes

- Everything Claude Code sends under `/v1/` (messages, `count_tokens`) goes through the scheduler.
- Streaming is relayed as-is; you'll see output as it is generated.
- If `limits_learned` stays `false`, the proxy hasn't seen a response yet. The first request runs unpaced, after which limits are known.
