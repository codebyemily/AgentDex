# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the project

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Node dependencies (Anthropic SDK + dotenv — used by test.js)
npm install

# Copy and populate environment variables
cp .env.example .env

# Run the multi-agent system
python main.py
```

## Required environment variables

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API calls in `shared/pipeline.py` |
| `BROWSERBASE_API_KEY` / `BROWSERBASE_PROJECT_ID` | Browserbase session for Wikipedia crawling |
| `REDIS_URL` | (Optional) Cache backend — currently unused, cache is in-process |
| `SENTRY_DSN` | (Optional) Error reporting in `speculative_worker` |

Optional tuning via env:
- `SPECULATION_BUDGET` (default `3`) — how many speculative topics to prefetch per query
- `SPECULATIVE_TIMEOUT_SECS` (default `30`) — max time the speculative worker waits per topic

## Architecture

AgentDex is a **speculative pre-fetching research pipeline** built on [Fetch.ai uAgents](https://docs.fetch.ai/uAgents/). Four agents run concurrently inside a single `Bureau`; addresses are wired in `main.py` before the bureau starts so agents can message each other.

```
dev_agent  ──TopicQuery──►  orchestrator  ──ResearchRequest──►  primary_worker
                │                                                      │
                │           ──ResearchRequest──►  speculative_worker   │
                │                                       │              │
                ◄──ResearchResult──────────────────────◄──────────────◄
```

### Agent responsibilities

| Agent | File | Role |
|---|---|---|
| `orchestrator` | `agents/orchestrator_agent.py` | Routes queries; checks warm cache; dispatches primary + speculative workers; forwards results back to requester |
| `primary_worker` | `agents/primary_worker.py` | Crawls the requested topic via Browserbase/Playwright, classifies it with Claude, populates cache |
| `speculative_worker` | `agents/speculative_worker.py` | Prefetches likely follow-up topics concurrently; discarded on timeout; concurrent message handling enabled |
| `dev_agent` | `agents/dev_agent.py` | Demo client — sends two queries (`atoms` → `electrons`) to show a cold run then a warm hit |

### Shared modules

- **`shared/pipeline.py`** — all I/O: `crawl_topic` (Browserbase + Playwright in a thread pool), `classify_and_structure` (Claude call to JSON), `get_speculative_candidates` (Claude call for predicted follow-ups), `research_topic` (combines both)
- **`shared/cache.py`** — in-process dict store (`get_warm` / `set_warm` / `all_topics`); keyed by lowercased topic
- **`shared/messages.py`** — uAgents `Model` classes: `TopicQuery`, `ResearchRequest`, `ResearchResult`
- **`shared/config.py`** — seeds for deterministic agent addresses; runtime address slots populated by `main.py`; speculation constants

### Key design details

- Agent addresses are deterministic from their `seed` strings — changing a seed changes the address.
- The in-process cache in `shared/cache.py` is **not persistent** across restarts and is not shared between processes.
- `classify_and_structure` and `get_speculative_candidates` both use `claude-sonnet-4-6` and expect raw JSON responses (no markdown fences).
- `ResearchResult.key_facts`, `.related_concepts`, and `.mcp_tools` are JSON-encoded strings (not native lists) because uAgents `Model` fields must be primitive types.
- Playwright runs synchronously in a `ThreadPoolExecutor` because the sync Playwright API cannot run inside an existing asyncio event loop.
