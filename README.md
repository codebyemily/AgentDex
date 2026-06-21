# AgentDex

A **speculative pre-fetching research pipeline** built on [Fetch.ai uAgents](https://docs.fetch.ai/uAgents/) and [RedisVL](https://github.com/RedisVentures/redisvl). AgentDex predicts which topics a researcher will ask about next and pre-warms them in the background — so the second query returns instantly from cache instead of waiting for a full crawl.

The analogy is CPU branch prediction or browser link prefetching: AgentDex bets on your next question before you ask it.

## Demo

```
dev_agent queries "atoms"  →  cold run  →  Wikipedia crawl + Claude classification
                                          speculative worker pre-warms: electrons, protons, periodic table

dev_agent queries "electrons"  →  *** WARM HIT ***  →  served instantly from Redis
```

## Architecture

```
dev_agent  ──TopicQuery──►  orchestrator  ──ResearchRequest──►  primary_worker
                                │                                      │
                                ├──ResearchRequest──►  speculative_worker
                                │                             │        │
                                ◄──ResearchResult────────────◄────────◄
```

| Agent | File | Role |
|---|---|---|
| `orchestrator` | `agents/orchestrator_agent.py` | Checks Redis cache; dispatches primary + speculative workers; two-stage speculation (vector KNN → Claude filter) |
| `primary_worker` | `agents/primary_worker.py` | Crawls Wikipedia via Browserbase/Playwright; classifies with Claude; populates cache |
| `speculative_worker` | `agents/speculative_worker.py` | Pre-fetches predicted follow-up topics concurrently; discarded on timeout |
| `dev_agent` | `agents/dev_agent.py` | Demo client — sends `atoms` → `electrons` to show cold run then warm hit |

### Speculation pipeline

1. **Exact cache check** — Redis hash lookup by topic key; return immediately if warm
2. **Semantic cache check** — embed query, KNN search; if nearest neighbor distance < `SEMANTIC_HIT_THRESHOLD` (default `0.15`), serve from cache without crawling
3. **Primary worker dispatch** — crawl + classify the requested topic
4. **Speculative expansion** — Redis KNN returns similar topics → Claude filters to genuine "next question" candidates → dispatch speculative workers for each

## Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Copy and fill in environment variables
cp .env.example .env

# Run the pipeline
python main.py
```

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Claude API calls (classification + speculation filter) |
| `BROWSERBASE_API_KEY` | Yes | Browserbase session for Wikipedia crawling |
| `BROWSERBASE_PROJECT_ID` | Yes | Browserbase project |
| `REDIS_URL` | Yes | Redis connection — use `redis://localhost:6379` for local Redis Stack |
| `SENTRY_DSN` | No | Error reporting in `speculative_worker` |
| `SPECULATION_BUDGET` | No | Max speculative topics per query (default `3`) |
| `SPECULATIVE_TIMEOUT_SECS` | No | Timeout for each speculative fetch (default `30`) |
| `SEMANTIC_HIT_THRESHOLD` | No | Cosine distance for a semantic cache hit (default `0.15`) |

## Local Redis Stack (required for vector search)

Redis Cloud free tier uses TLS 1.0/1.1 which is incompatible with Python 3.13 / OpenSSL 3.x. Run Redis Stack locally instead:

```bash
docker run -d --name redis-stack -p 6379:6379 redis/redis-stack-server:latest
```

Then set `REDIS_URL=redis://localhost:6379` in `.env`.

## Shared modules

| Module | Purpose |
|---|---|
| `shared/redis_client.py` | RedisVL vector index — `embed`, `set_warm`, `get_warm`, `search_nearest_with_scores`, `all_topics` |
| `shared/pipeline.py` | I/O — `crawl_topic` (Browserbase), `classify_and_structure` (Claude), `filter_speculative_candidates` (Claude), `research_topic` |
| `shared/cache.py` | Re-exports `get_warm`, `set_warm`, `all_topics` from `redis_client` |
| `shared/messages.py` | uAgents message models: `TopicQuery`, `ResearchRequest`, `ResearchResult` |
| `shared/config.py` | Seeds, address slots, and tuning constants |

## MCP server — external agent access

`mcp_server.py` exposes the AgentDex pipeline as an [MCP](https://modelcontextprotocol.io) server. Any MCP-compatible agent can use it without joining the internal uAgents Bureau.

### How it fits in

```
┌─────────────────── Bureau (main.py) ───────────────────┐
│  dev_agent ──TopicQuery──► orchestrator ──► workers    │  ← internal, uAgents protocol
└────────────────────────────────────────────────────────┘

External developer agent
  └── mcp_server.py (stdio) ──► shared/pipeline.py       ← external, MCP protocol
                             └──► shared/redis_client.py
```

The Bureau and the MCP server share the same Redis backend, so topics pre-warmed by the speculative pipeline are immediately available to external agents via `get_cached_topic` and `search_similar_topics`.

### Tools

| Tool | Parameters | What it does |
|---|---|---|
| `research_topic` | `topic: str` | Crawls Wikipedia, classifies with Claude, stores in Redis cache. Returns JSON with `summary`, `key_facts`, `related_concepts`, `mcp_tools`. |
| `get_cached_topic` | `topic: str` | Direct Redis lookup. Returns cached JSON or `{}` if not warm. |
| `search_similar_topics` | `query: str`, `k: int = 5` | Semantic KNN search. Returns `[{topic, distance}]` sorted by cosine distance. |
| `list_warm_topics` | — | Lists all topics currently in the Redis cache. |

### Claude Desktop / Claude Code

Add to `claude_desktop_config.json` (macOS: `~/Library/Application Support/Claude/`):

```json
{
  "mcpServers": {
    "agentdex": {
      "command": "python",
      "args": ["/path/to/AgentDex/mcp_server.py"],
      "env": {
        "ANTHROPIC_API_KEY": "...",
        "BROWSERBASE_API_KEY": "...",
        "BROWSERBASE_PROJECT_ID": "...",
        "REDIS_URL": "redis://localhost:6379"
      }
    }
  }
}
```

Claude will then call `research_topic`, `search_similar_topics`, etc. as native tools.

### Python agent (MCP client)

```python
from mcp import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

params = StdioServerParameters(
    command="python",
    args=["/path/to/AgentDex/mcp_server.py"],
    env={"ANTHROPIC_API_KEY": "...", "REDIS_URL": "redis://localhost:6379", ...},
)

async with stdio_client(params) as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()

        # Cold research — crawls Wikipedia + caches result
        result = await session.call_tool("research_topic", {"topic": "black holes"})

        # Warm lookup — instant if already cached
        cached = await session.call_tool("get_cached_topic", {"topic": "black holes"})

        # Semantic search across everything in the cache
        similar = await session.call_tool("search_similar_topics", {"query": "event horizon", "k": 5})
```

### Direct Python import (same repo)

If the developer's agent runs in the same Python environment, MCP is optional:

```python
from shared.pipeline import research_topic
from shared.redis_client import get_warm, search_nearest_with_scores

result = await research_topic("quantum entanglement")
similar = await search_nearest_with_scores("spooky action", k=5)
```

## Validation

```bash
python scripts/test_person_a.py
```

Tests the full Redis pipeline: upsert → warm lookup → KNN search → Claude speculation filter → import compatibility. Requires local Redis Stack and `ANTHROPIC_API_KEY`.
