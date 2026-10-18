# Anthropic SDK through AegisLLM

## 1. Start the proxy

```bash
aegis -provider-type anthropic -upstream-url https://api.anthropic.com -upstream-api-key "$ANTHROPIC_API_KEY"
```

## 2. Python

```python
import anthropic

client = anthropic.Anthropic(
    base_url="http://localhost:8080",
    api_key="placeholder",                      # proxy holds the real key
    default_headers={"X-Run-ID": "doc-extract", "X-Workload-Class": "background"},
)

with client.messages.stream(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Summarize this contract: ..."}],
) as stream:
    for text in stream.text_stream:
        print(text, end="")
```

Note the base URL has no `/v1`; the SDK adds it. (Both forms work; the proxy de-duplicates.)

## 3. Node

```ts
import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic({
  baseURL: "http://localhost:8080",
  apiKey: "placeholder",
  defaultHeaders: { "X-Run-ID": "doc-extract", "X-Workload-Class": "background" },
});
```

## Many parallel runs

Create one client per task with its own `X-Run-ID`, or set the header per call:

```python
client.messages.create(..., extra_headers={"X-Run-ID": f"doc-{doc.id}"})
```

The scheduler shares the account's quota across runs fairly and keeps `production` ahead of `batch`.
