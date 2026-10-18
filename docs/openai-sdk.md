# OpenAI SDK and OpenAI Agents SDK through AegisLLM

## 1. Start the proxy

```bash
aegis -provider-type openai -upstream-url https://api.openai.com -upstream-api-key "$OPENAI_API_KEY"
```

## 2. Python SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="placeholder",                      # proxy holds the real key
    default_headers={
        "X-Run-ID": "ingest-2026-10-17",
        "X-Workload-Class": "batch",             # production | interactive | background | batch
    },
)
resp = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": "hi"}])
```

Everything the SDK calls under `/v1/` – chat completions, responses, embeddings – is scheduled.

## 3. OpenAI Agents SDK

Give the Agents SDK a client that points at the proxy, once, at startup:

```python
from openai import AsyncOpenAI
from agents import Agent, Runner, set_default_openai_client

set_default_openai_client(AsyncOpenAI(
    base_url="http://localhost:8080/v1",
    api_key="placeholder",
    default_headers={"X-Run-ID": "support-triage", "X-Workload-Class": "production"},
))

agent = Agent(name="triage", instructions="Classify the ticket.")
result = Runner.run_sync(agent, "Customer can't log in after password reset")
```

Handoffs and tool-using sub-agents reuse the same client, so the whole run shares one run ID. To give each top-level task its own run ID, create one client per task.

## 4. Node

```ts
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:8080/v1",
  apiKey: "placeholder",
  defaultHeaders: { "X-Run-ID": "report-gen", "X-Workload-Class": "interactive" },
});
```

## Priorities

| Class | Use for |
|---|---|
| `production` | Anything a user is waiting on in your product |
| `interactive` | A person at a terminal (default if no class is sent) |
| `background` | Jobs that should finish soon but nobody is watching |
| `batch` | Nightly or bulk work; goes last when quota is tight |

Within a class, runs share quota fairly; a run with 40 parallel calls cannot starve a run with 2.
