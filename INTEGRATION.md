# Integration contract — observability/panel (Person C) ↔ A & B

**TL;DR:** The panel, Sentry, and the Redis event bus are fully self-contained
and already work standalone (`python -m panel.demo_replay`). The *only* coupling
to A's and B's code is a handful of `emit_demo_event(...)` calls living inside
your files. The merge already dropped them once. This doc is the contract so it
doesn't happen again.

Nothing here blocks A or B from building independently. These are the lines to
**keep** (or move intact) when you rewrite your internals.

---

## The event bus (don't worry about it)

- `shared/demo_events.py :: emit_demo_event(type, data)` — fire-and-forget.
  **Safe to call anywhere**: if Redis is down it's a no-op (no exception), so it
  can never crash your pipeline. It also drops a Sentry breadcrumb.
- The panel reads the Redis stream over SSE. It never imports A's or B's code.
- So: call `emit_demo_event` at the right spots and you're done. No other wiring.

---

## Person A — orchestrator + speculation

You own `agents/orchestrator_agent.py` and the speculation/vector layer. Keep
these emits when you swap the in-memory cache for RedisVL and the candidate list
for embeddings + the relevance filter. They fire at *logical* pipeline points,
so they're identical whether speculation is a plain async pool or uAgents.

| Event | Fire when | Required fields | Panel use |
|---|---|---|---|
| `query_received` | a query arrives | `topic`, `session_id` | breadcrumb |
| `warm_hit` | served from the vector cache | `topic`, `session_id`, **`matched_topic`**, **`warmed_at`**, `similarity` | **payoff** |
| `cold_dispatch` | no hit → dispatch primary | `topic`, `session_id` | left column |
| `speculation_planned` | candidates chosen | `parent`, `candidates` | breadcrumb |
| `spec_dispatch` | each bet is fired | `topic`, `parent` | makes worker card |
| `candidate_skipped` | candidate already warm | `topic`, `reason` | breadcrumb |
| `candidate_rejected` | relevance filter drops a candidate | `topic`, `reason` | greyed-out card in pool |

### ⚠️ The one that matters most: `warm_hit` and the semantic match
The panel shows "answer was ready N.Ns before the question" by matching a
`warm_hit` to the earlier `spec_warm` **for the same topic**. Your vector search
is *semantic* — a query ("electron") may hit a cached entry ("electrons"). When
that happens the strings differ and the proof is lost unless you tell the panel
what matched:

- **`matched_topic`** — the cached topic that actually satisfied the query
  (the string that was `spec_warm`-ed). The panel matches on this.
- **`warmed_at`** — epoch seconds when that entry was warmed (so the lead time
  still shows even across a panel reconnect). RedisVL must store this alongside
  the vector; return it on the hit.
- **`similarity`** — optional; if present the panel shows it ("matched
  'electrons' (0.91 sim)"), which actively *demos your vector IP*.

Keep the topic label byte-for-byte consistent between `spec_warm` and
`matched_topic`, or normalize both the same way.

### High-value: show the relevance filter working (panel ready now)
Your relevance filter rejecting an irrelevant candidate is a great "the IP is
real" visual, and **the panel already renders it** — just emit it:

```python
emit_demo_event("candidate_rejected", {
    "topic": candidate,
    "reason": f"similarity {score:.2f} < {THRESHOLD:.2f} threshold",  # any string
})
```

It shows as a greyed-out, struck-through card in the speculative pool with your
reason text (e.g. "relevance filter rejected — similarity 0.31 < 0.70
threshold"). The `reason` is freeform; put whatever's most convincing on stage
(score, threshold, "off-topic", etc.). See it live now in `demo_replay`.

---

## Person B — primary pipeline

You own `shared/pipeline.py` (crawl, classify, MCP gen). The wrapper events live
in `agents/primary_worker.py` around your `research_topic(topic) -> dict` call:
`cold_started` / `cold_done` (live path) and `spec_started` / `spec_warm` /
`spec_timeout` (speculative path).

**To keep these working, keep `research_topic(topic)` returning a dict.** If you
rename it or move the crawl/classify out of `primary_worker`, the wrappers come
with it — ping me and we re-attach in ~2 min.

### Sentry on the cold path (currently a gap)
`crawl_topic` and `classify_and_structure` **swallow** their exceptions and
return a fallback string/dict — so Browserbase and Claude failures never reach
Sentry, and "Sentry across both paths" isn't actually true for your path. One
line fixes it at each swallow point:

```python
from shared.observability import capture_pipeline_error
try:
    ...
except Exception as exc:
    capture_pipeline_error(exc, path="crawl", topic=topic)   # or path="classify"
    return f"[crawl error: {exc}]"
```

It's no-op-safe when Sentry is off and tags the error by stage + topic.

### If you want the generated MCP server shown on the panel
Put its URL in the `research_topic` result dict under `mcp_endpoint` and tell me
the key — I'll thread it into `warm_hit` / `dev_result` and show "MCP server
ready at …". Right now the panel shows the summary only.

---

## Shared message model (`shared/messages.py`)
`ResearchResult` is jointly owned. Adding fields (e.g. `mcp_endpoint`) is safe —
the dev agent and panel ignore unknown fields. Just don't rename or drop the
existing ones (`topic`, `summary`, `key_facts`, `content_type`, `warm`).

---

## How to verify after any merge (30 seconds)
```bash
python -m panel.demo_replay        # with the panel running
```
If the speculative pool fills and the warm-hit card shows "ready N.Ns before the
question", the contract held. If the pool is empty or the proof is missing, an
emit got dropped — check the table above.
```
grep -rn emit_demo_event agents/   # should show all the events in the tables
```
