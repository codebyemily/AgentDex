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

## Validation

```bash
python scripts/test_person_a.py
```

Tests the full Redis pipeline: upsert → warm lookup → KNN search → Claude speculation filter → import compatibility. Requires local Redis Stack and `ANTHROPIC_API_KEY`.
