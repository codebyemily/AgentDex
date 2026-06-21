import json
import time

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.cache import get_warm, all_topics
from shared.redis_client import search_nearest_with_scores
from shared.messages import TopicQuery, TopicBatch, ResearchRequest, ResearchResult, BatchResult
from shared.pipeline import get_speculative_candidates, filter_speculative_candidates
from shared.demo_events import emit_demo_event

load_dotenv()

orchestrator = Agent(name="orchestrator", seed=config.ORCHESTRATOR_SEED, handle_messages_concurrently=True)
_pending: dict[str, str] = {}

# batch_session_id → {requester, remaining, results: list[dict]}
_batch_pending: dict[str, dict] = {}


def _build_result(msg: TopicQuery, cached: dict) -> ResearchResult:
    return ResearchResult(
        topic=msg.topic,
        session_id=msg.session_id,
        summary=cached.get("summary", ""),
        content_type=cached.get("content_type", "prose"),
        key_facts=json.dumps(cached.get("key_facts", [])),
        related_concepts=json.dumps(cached.get("related_concepts", [])),
        mcp_tools=json.dumps(cached.get("mcp_tools", [])),
        warm=True,
        timestamp=cached.get("cached_at", time.time()),
    )


@orchestrator.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[orchestrator] started  address={ctx.agent.address}")


@orchestrator.on_message(model=TopicBatch)
async def on_batch(ctx: Context, sender: str, msg: TopicBatch):
    topics = json.loads(msg.topics)
    ctx.logger.info(f"[orchestrator] BATCH received — {len(topics)} topics: {topics}  session={msg.session_id}")
    emit_demo_event("batch_received", {"topics": topics, "session_id": msg.session_id})

    # ── Exact warm-cache check for every topic ────────────────────────────────
    warm_results: list[dict] = []
    cold_topics: list[str] = []

    for topic in topics:
        cached = await get_warm(topic)
        if cached:
            ctx.logger.info(f"[orchestrator] batch '{topic}' — WARM HIT, no crawl needed")
            warm_results.append({"topic": topic, "warm": True, **cached})
        else:
            ctx.logger.info(f"[orchestrator] batch '{topic}' — COLD, will research")
            cold_topics.append(topic)

    ctx.logger.info(
        f"[orchestrator] batch summary — {len(warm_results)} warm, {len(cold_topics)} cold: {cold_topics}"
    )

    # ── All warm — reply immediately ─────────────────────────────────────────
    if not cold_topics:
        ctx.logger.info(f"[orchestrator] batch: all topics warm — sending BatchResult immediately → {sender}")
        emit_demo_event("batch_done", {"session_id": msg.session_id, "all_warm": True})
        await ctx.send(sender, BatchResult(session_id=msg.session_id, results=json.dumps(warm_results)))
        return

    # ── Dispatch all cold topics to primary_worker in parallel ────────────────
    _batch_pending[msg.session_id] = {
        "requester": sender,
        "remaining": len(cold_topics),
        "results": warm_results,
    }

    ctx.logger.info(
        f"[orchestrator] batch: dispatching {len(cold_topics)} topics IN PARALLEL → {config.PRIMARY_WORKER_ADDRESS}"
    )
    emit_demo_event("batch_dispatch", {"cold_topics": cold_topics, "session_id": msg.session_id})

    for i, topic in enumerate(cold_topics):
        item_session = f"batch:{msg.session_id}:{i}"
        ctx.logger.info(f"[orchestrator] batch: dispatching '{topic}' (item_session={item_session})")
        await ctx.send(
            config.PRIMARY_WORKER_ADDRESS,
            ResearchRequest(topic=topic, session_id=item_session, is_speculative=False),
        )


@orchestrator.on_message(model=TopicQuery)
async def on_query(ctx: Context, sender: str, msg: TopicQuery):
    ctx.logger.info(f"[orchestrator] query: '{msg.topic}'  session={msg.session_id}")
    emit_demo_event("query_received", {"topic": msg.topic, "session_id": msg.session_id})

    # ── Exact warm-cache check ────────────────────────────────────────────────
    cached = await get_warm(msg.topic)
    if cached:
        ctx.logger.info(f"[orchestrator] WARM HIT (exact) — serving '{msg.topic}' instantly")
        emit_demo_event("warm_hit", {"topic": msg.topic, "session_id": msg.session_id, "warmed_at": cached.get("cached_at")})
        await ctx.send(sender, _build_result(msg, cached))
        return

    # ── Semantic cache check + speculative candidate pool ─────────────────────
    # One embed call serves both purposes: semantic hit detection and speculative
    # expansion.  Scored results let us apply a distance threshold; the topic list
    # is reused below so we never embed the same query twice.
    nearest_scored = await search_nearest_with_scores(msg.topic, k=10)

    if nearest_scored:
        best_topic, best_dist = nearest_scored[0]
        if best_dist < config.SEMANTIC_HIT_THRESHOLD:
            sem_cached = await get_warm(best_topic)
            if sem_cached:
                ctx.logger.info(
                    f"[orchestrator] SEMANTIC HIT — '{msg.topic}' ≈ '{best_topic}' "
                    f"(distance={best_dist:.3f})"
                )
                emit_demo_event("warm_hit", {"topic": msg.topic, "session_id": msg.session_id, "matched": best_topic, "distance": best_dist})
                await ctx.send(sender, _build_result(msg, sem_cached))
                return

    # ── Dispatch primary worker ───────────────────────────────────────────────
    _pending[msg.session_id] = sender
    ctx.logger.info(f"[orchestrator] dispatching primary worker for '{msg.topic}'")
    emit_demo_event("cold_dispatch", {"topic": msg.topic, "session_id": msg.session_id})
    await ctx.send(
        config.PRIMARY_WORKER_ADDRESS,
        ResearchRequest(topic=msg.topic, session_id=msg.session_id, is_speculative=False),
    )

    # ── Speculative expansion ─────────────────────────────────────────────────
    # Reuse the scored KNN results already fetched above (topics only).
    raw_candidates = [t for t, _ in nearest_scored]
    if raw_candidates:
        ctx.logger.info(
            f"[orchestrator] Redis returned {len(raw_candidates)} raw candidates "
            f"for '{msg.topic}': {raw_candidates}"
        )
        # Claude relevance filter — keeps only genuine next-question candidates.
        candidates = await filter_speculative_candidates(
            msg.topic, raw_candidates, config.SPECULATION_BUDGET
        )
    else:
        ctx.logger.info(f"[orchestrator] Redis index empty for '{msg.topic}', using Claude cold-start fallback")
        candidates = await get_speculative_candidates(msg.topic, config.SPECULATION_BUDGET)

    warm_set = set(await all_topics())
    ctx.logger.info(f"[orchestrator] speculative candidates after filtering: {candidates}")
    emit_demo_event("speculation_planned", {"parent": msg.topic, "candidates": candidates})

    for candidate in candidates:
        key = candidate.lower().strip()
        if key in warm_set:
            ctx.logger.info(f"[orchestrator] '{candidate}' already warm — skipping")
            emit_demo_event("candidate_skipped", {"topic": candidate, "reason": "already_warm"})
            continue
        ctx.logger.info(f"[orchestrator] dispatching speculative bet for '{candidate}'")
        emit_demo_event("spec_dispatch", {"topic": candidate, "parent": msg.topic})
        await ctx.send(config.PRIMARY_WORKER_ADDRESS, ResearchRequest(topic=candidate, session_id=f"spec-{candidate}", is_speculative=True))


@orchestrator.on_message(model=ResearchResult)
async def on_result(ctx: Context, sender: str, msg: ResearchResult):  # noqa: ARG001
    if msg.session_id.startswith("spec-"):
        ctx.logger.info(f"[orchestrator] speculative result for '{msg.topic}' registered as warm")
        # fall through to related-concept post-warming below
    elif msg.session_id.startswith("batch:"):
        # ── Batch item result ─────────────────────────────────────────────────
        parts = msg.session_id.split(":", 2)
        batch_sid = parts[1]
        state = _batch_pending.get(batch_sid)
        if state:
            mcp_tools = json.loads(msg.mcp_tools) if msg.mcp_tools else []
            state["results"].append({
                "topic": msg.topic,
                "warm": False,
                "summary": msg.summary,
                "content_type": msg.content_type,
                "key_facts": json.loads(msg.key_facts) if msg.key_facts else [],
                "related_concepts": json.loads(msg.related_concepts) if msg.related_concepts else [],
                "mcp_tools": mcp_tools,
            })
            state["remaining"] -= 1
            tool_names = [t.get("name") for t in mcp_tools]
            ctx.logger.info(
                f"[orchestrator] batch '{batch_sid}' — '{msg.topic}' done  "
                f"mcp_tools={tool_names}  remaining={state['remaining']}"
            )
            if state["remaining"] == 0:
                del _batch_pending[batch_sid]
                ctx.logger.info(
                    f"[orchestrator] batch '{batch_sid}' — ALL DONE, sending BatchResult → {state['requester']}"
                )
                emit_demo_event("batch_done", {"session_id": batch_sid, "count": len(state["results"])})
                await ctx.send(state["requester"], BatchResult(
                    session_id=batch_sid,
                    results=json.dumps(state["results"]),
                ))
        else:
            ctx.logger.warning(f"[orchestrator] batch result for unknown batch '{batch_sid}'")
        # fall through to related-concept post-warming below
    else:
        # ── Single query result ───────────────────────────────────────────────
        requester = _pending.pop(msg.session_id, None)
        if requester:
            ctx.logger.info(f"[orchestrator] forwarding '{msg.topic}' → dev agent")
            await ctx.send(requester, msg)
        else:
            ctx.logger.warning(
                f"[orchestrator] no pending requester for session '{msg.session_id}'"
            )

    # ── Dispatch speculative workers for related_concepts ─────────────────────
    # Claude extracted these from the actual crawled content — higher signal than
    # the pre-crawl predictions made in on_query.  Pre-warm any that aren't cached.
    try:
        related = json.loads(msg.related_concepts) if msg.related_concepts else []
    except (json.JSONDecodeError, TypeError):
        related = []

    if related:
        warm_set = set(await all_topics())
        for concept in related:
            key = concept.lower().strip()
            if key in warm_set:
                ctx.logger.info(f"[orchestrator] related '{concept}' already warm — skipping")
                continue
            ctx.logger.info(f"[orchestrator] pre-warming related concept '{concept}'")
            emit_demo_event("spec_dispatch", {"topic": concept, "parent": msg.topic})
            await ctx.send(
                config.PRIMARY_WORKER_ADDRESS,
                ResearchRequest(
                    topic=concept,
                    session_id=f"spec-{key}",
                    is_speculative=True,
                ),
            )
