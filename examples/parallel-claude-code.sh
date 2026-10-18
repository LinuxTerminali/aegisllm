#!/usr/bin/env bash
# Run several Claude Code tasks in parallel through AegisLLM, one run ID each.
# Prereqs: aegis running on :8080 with -provider-type anthropic and your API key.
set -euo pipefail

export ANTHROPIC_BASE_URL=http://localhost:8080
export ANTHROPIC_API_KEY=placeholder

tasks=(
  "fix the flaky integration tests"
  "add docstrings to the public API"
  "upgrade dependencies and fix breakages"
)

for task in "${tasks[@]}"; do
  id=$(echo "$task" | tr ' ' '-' | cut -c1-40)
  ANTHROPIC_CUSTOM_HEADERS="X-Run-ID: $id"$'\n'"X-Workload-Class: background" \
    claude -p "$task" > "claude-$id.log" 2>&1 &
done

echo "Launched ${#tasks[@]} sessions. Watching the scheduler (Ctrl-C to stop watching; sessions continue):"
watch -n1 'curl -s localhost:8080/status | jq "{queued: .queued_requests, active: .active_requests, rpm: .capacity.requests_per_minute, learned: .capacity.limits_learned, rejected: .total_rejected}"'
