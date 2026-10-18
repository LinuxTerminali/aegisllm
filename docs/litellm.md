# Using AegisLLM with LiteLLM

LiteLLM gives you one API for many providers, routing, and keys. AegisLLM schedules requests against one provider's quota. They stack.

## AegisLLM behind LiteLLM (recommended)

Agents keep talking to LiteLLM. LiteLLM sends each provider's traffic to an AegisLLM instance for that provider, which paces it against that account's limits.

```bash
aegis -listen-addr :8081 -provider-type openai    -upstream-url https://api.openai.com    -upstream-api-key "$OPENAI_API_KEY"
aegis -listen-addr :8082 -provider-type anthropic -upstream-url https://api.anthropic.com -upstream-api-key "$ANTHROPIC_API_KEY"
```

`litellm_config.yaml`:

```yaml
model_list:
  - model_name: gpt-4o-mini
    litellm_params:
      model: openai/gpt-4o-mini
      api_base: http://localhost:8081/v1
      api_key: placeholder
  - model_name: claude-sonnet
    litellm_params:
      model: anthropic/claude-sonnet-4-5
      api_base: http://localhost:8082
      api_key: placeholder
```

Forward the scheduling headers from your agents through LiteLLM so runs stay distinct. LiteLLM passes unknown request headers through to the upstream; if yours does not, set `extra_headers` in `litellm_params` or send `X-Run-ID` per request.

## AegisLLM in front of LiteLLM

Only useful if LiteLLM itself fronts a single quota. Set `-provider-type generic -upstream-url http://litellm:4000` and leave `-upstream-api-key` empty so each client's LiteLLM virtual key passes through.

## What you get from each

| | LiteLLM | AegisLLM |
|---|---|---|
| Many providers, one API | Yes | No |
| Virtual keys, spend tracking | Yes | No |
| Learns provider limits from headers | No | Yes |
| Queues and paces instead of rejecting | Partial | Yes |
| Priority classes, fairness across runs | Partial | Yes |
| Central retry with upstream 429 holding the queue | No | Yes |
