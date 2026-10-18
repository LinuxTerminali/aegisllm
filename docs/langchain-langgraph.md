# LangChain, LangGraph and CrewAI through AegisLLM

All of these build on the OpenAI or Anthropic SDK clients, so the change is the same: a base URL and default headers on the model object.

## Start the proxy

```bash
# pick one
aegis -provider-type openai    -upstream-url https://api.openai.com    -upstream-api-key "$OPENAI_API_KEY"
aegis -provider-type anthropic -upstream-url https://api.anthropic.com -upstream-api-key "$ANTHROPIC_API_KEY"
```

## LangChain / LangGraph

```python
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic

llm = ChatOpenAI(
    model="gpt-4o-mini",
    base_url="http://localhost:8080/v1",
    api_key="placeholder",
    default_headers={"X-Run-ID": "research-graph-42", "X-Workload-Class": "interactive"},
)

# or
llm = ChatAnthropic(
    model="claude-sonnet-4-5",
    base_url="http://localhost:8080",
    api_key="placeholder",
    default_headers={"X-Run-ID": "research-graph-42", "X-Workload-Class": "interactive"},
)
```

Build your LangGraph graph with that `llm`. Every node that calls the model, including parallel branches and sub-graphs, carries the same run ID.

One graph invocation per run ID is the useful granularity. A simple pattern:

```python
def make_llm(run_id: str, workload_class: str = "background"):
    return ChatOpenAI(model="gpt-4o-mini", base_url="http://localhost:8080/v1", api_key="placeholder",
                      default_headers={"X-Run-ID": run_id, "X-Workload-Class": workload_class})

for job in jobs:
    graph = build_graph(make_llm(run_id=f"job-{job.id}"))
    graph.invoke(job.input)
```

## CrewAI

```python
from crewai import LLM

llm = LLM(
    model="openai/gpt-4o-mini",
    base_url="http://localhost:8080/v1",
    api_key="placeholder",
    extra_headers={"X-Run-ID": "crew-market-analysis", "X-Workload-Class": "background"},
)
```

Pass `llm` to your agents. All agents in a crew share the run ID.

## Checking it works

```bash
curl -s localhost:8080/status | jq '.queue_stats, .capacity'
```

Queue depth per class and the learned RPM/TPM confirm traffic is flowing through the scheduler.
