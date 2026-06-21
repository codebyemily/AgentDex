# AgentDex

A **speculative pre-fetching research pipeline** built on [Fetch.ai uAgents](https://docs.fetch.ai/uAgents/) and [RedisVL](https://github.com/RedisVentures/redisvl). AgentDex predicts which topics a researcher will ask about next and pre-warms them in the background — so the second query returns instantly from cache instead of waiting for a full crawl-and-classify cycle.

The analogy is CPU branch prediction or browser link prefetching: AgentDex bets on your next question before you ask it.

## Demo

```
dev_agent queries "atoms"  →  cold run  →  Wikipedia crawl + Claude classification
                                            speculative workers pre-warm:
                                              electrons  ✓ ready
                                              protons    ✓ ready
                                              periodic table  ✓ ready

dev_agent queries "electrons"  →  *** WARM HIT ***  →  served instantly from Redis
```

A live panel at `http://localhost:8000` shows this in real time — speculative bets pulsing as they run, turning green when ready, with a "was ready N.Ns before the question" timer on each warm hit.

## Architecture

```
dev_agent  ──TopicQuery──►  orchestrator  ──ResearchRequest──►  primary_worker
                                │                (live)                │
                                │                                      │
                                └──ResearchRequest──►  primary_worker  │
                                    (is_speculative=True, ×N)          │
                                                                       │
                                ◄──ResearchResult──────────────────────◄
                                          │
                                    shared/demo_events
                                          │
                                    panel/server.py  ──SSE──►  browser
```

### Agent responsibilities

| Agent | File | Role |
|---|---|---|
| `orchestrator` | `agents/orchestrator_agent.py` | Exact + semantic cache check; dispatches live and speculative workers; two-stage speculation (Redis KNN → Claude filter); emits panel events |
| `primary_worker` | `agents/primary_worker.py` | Handles both live and speculative `ResearchRequest`s; crawls Wikipedia via Browserbase/Playwright; classifies with Claude; populates Redis cache; timeout-guards speculative path |
| `dev_agent` | `agents/dev_agent.py` | Demo client — sends `atoms` → `electrons` to demonstrate cold run then warm hit |

### Speculation pipeline (per query)

1. **Exact cache check** — Redis hash lookup by topic key; return immediately if warm
2. **Semantic cache check** — embed query, KNN search; if nearest neighbor distance < `SEMANTIC_HIT_THRESHOLD` (default `0.15`), serve from cache without crawling
3. **Primary worker dispatch** — crawl + classify the requested topic
4. **Speculative expansion** — Redis KNN returns similar topics → Claude filters to genuine "next question" candidates → dispatch `primary_worker` with `is_speculative=True` for each candidate up to `SPECULATION_BUDGET`

After the primary worker returns, the orchestrator also pre-warms any `related_concepts` extracted by Claude from the actual crawled content — higher-signal than the pre-crawl predictions.

## Live panel

Start the panel server alongside the pipeline:

```bash
# Terminal 1 — pipeline
python main.py

# Terminal 2 — panel
uvicorn panel.server:app --reload --port 8000
```

Open `http://localhost:8000`. The panel shows two live columns:

- **Left — Live queries**: each query card shows whether it was a cold run or warm hit, with timing
- **Right — Speculative pool**: one card per candidate — pulsing while running, green when warm, red on timeout, grey/strikethrough if Claude's relevance filter rejected it

### Demo without Browserbase

To style or present the panel without running a real crawl:

```bash
python -m panel.demo_replay
```

This replays a hardcoded script of events into the same Redis stream the real pipeline uses.

## Setup

```bash
# Python dependencies
pip install -r requirements.txt

# Playwright browser (needed for Wikipedia crawling)
playwright install chromium

# Copy and populate environment variables
cp .env.example .env

# Start local Redis Stack (required — see note below)
docker run -d --name redis-stack -p 6379:6379 redis/redis-stack-server:latest

# Run
python main.py
```

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Claude API — topic classification and speculation filter |
| `BROWSERBASE_API_KEY` | Yes | Browserbase session for Wikipedia crawling |
| `BROWSERBASE_PROJECT_ID` | Yes | Browserbase project ID |
| `REDIS_URL` | Yes | Redis connection URL (e.g. `redis://localhost:6379`) |
| `SENTRY_DSN` | No | Sentry error reporting — pipeline errors and speculative timeouts |
| `AGENTDEX_ENV` | No | Sentry environment tag (default `"demo"`) |
| `SPECULATION_BUDGET` | No | Max speculative topics per query (default `3`) |
| `SPECULATIVE_TIMEOUT_SECS` | No | Timeout per speculative fetch in seconds (default `30`) |
| `SEMANTIC_HIT_THRESHOLD` | No | Cosine distance cutoff for semantic cache hit (default `0.15`) |

## Redis Stack (local)

Redis Cloud free tier uses TLS 1.0/1.1, which Python 3.13 / OpenSSL 3.x has removed support for. Run Redis Stack locally instead — it includes the Search module required for vector KNN queries:

```bash
docker run -d --name redis-stack -p 6379:6379 redis/redis-stack-server:latest
```

Set `REDIS_URL=redis://localhost:6379` in `.env`.

## Shared modules

| Module | Purpose |
|---|---|
| `shared/redis_client.py` | RedisVL vector index — `embed`, `set_warm`, `get_warm`, `search_nearest_with_scores`, `upsert_topic`, `all_topics` |
| `shared/pipeline.py` | All I/O — `crawl_topic` (Browserbase + Playwright), `classify_and_structure` (Claude), `get_speculative_candidates` (Claude cold-start), `filter_speculative_candidates` (Claude relevance filter), `research_topic` |
| `shared/cache.py` | Thin re-export of `get_warm`, `set_warm`, `all_topics` from `redis_client` |
| `shared/messages.py` | uAgents message models: `TopicQuery`, `ResearchRequest`, `ResearchResult` |
| `shared/config.py` | Seeds, address slots, and tuning constants |
| `shared/observability.py` | Sentry init (`init_sentry`) and tagged error capture (`capture_pipeline_error`) |
| `shared/demo_events.py` | Redis stream event bus — `emit_demo_event`, `read_events`, `history`, `reset`; degrades silently if Redis is unavailable |

## Validation

```bash
python scripts/test_person_a.py
```

Runs 7 checks against the Redis pipeline: seed upsert → warm lookup → field deserialization → KNN search → Claude speculation filter → cold-start path → agent import compatibility. Requires local Redis Stack and `ANTHROPIC_API_KEY`.
