import json
import time

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.cache import get_warm, all_topics
from shared.redis_client import search_nearest_with_scores
from shared.messages import TopicQuery, ResearchRequest, ResearchResult
from shared.pipeline import get_speculative_candidates, filter_speculative_candidates
from shared.demo_events import emit_demo_event

load_dotenv()

orchestrator = Agent(name="orchestrator", seed=config.ORCHESTRATOR_SEED, handle_messages_concurrently=True)
_pending: dict[str, str] = {}


def _build_result(msg: TopicQuery, cached: dict) -> ResearchResult:
    return ResearchResult(
        topic=msg.topic, session_id=msg.session_id,
        summary=cached.get("summary", ""), content_type=cached.get("content_type", "prose"),
        key_facts=json.dumps(cached.get("key_facts", [])),
        related_concepts=json.dumps(cached.get("related_concepts", [])),
        mcp_tools=json.dumps(cached.get("mcp_tools", [])),
        warm=True, timestamp=cached.get("cached_at", time.time()),
    )


@orchestrator.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[orchestrator] started  address={ctx.agent.address}")


@orchestrator.on_message(model=TopicQuery)
async def on_query(ctx: Context, sender: str, msg: TopicQuery):
    ctx.logger.info(f"[orchestrator] query: '{msg.topic}'  session={msg.session_id}")
    emit_demo_event("query_received", {"topic": msg.topic, "session_id": msg.session_id})

    cached = await get_warm(msg.topic)
    if cached:
        ctx.logger.info(f"[orchestrator] WARM HIT (exact) — serving '{msg.topic}' instantly")
        emit_demo_event("warm_hit", {"topic": msg.topic, "session_id": msg.session_id, "warmed_at": cached.get("cached_at")})
        await ctx.send(sender, _build_result(msg, cached))
        return

    nearest_scored = await search_nearest_with_scores(msg.topic, k=10)
    if nearest_scored:
        best_topic, best_dist = nearest_scored[0]
        if best_dist < config.SEMANTIC_HIT_THRESHOLD:
            sem_cached = await get_warm(best_topic)
            if sem_cached:
                ctx.logger.info(f"[orchestrator] SEMANTIC HIT — '{msg.topic}' ≈ '{best_topic}' (distance={best_dist:.3f})")
                emit_demo_event("warm_hit", {"topic": msg.topic, "session_id": msg.session_id, "matched": best_topic, "distance": best_dist})
                await ctx.send(sender, _build_result(msg, sem_cached))
                return

    _pending[msg.session_id] = sender
    ctx.logger.info(f"[orchestrator] dispatching primary worker for '{msg.topic}'")
    emit_demo_event("cold_dispatch", {"topic": msg.topic, "session_id": msg.session_id})
    await ctx.send(config.PRIMARY_WORKER_ADDRESS, ResearchRequest(topic=msg.topic, session_id=msg.session_id, is_speculative=False))

    raw_candidates = [t for t, _ in nearest_scored]
    if raw_candidates:
        ctx.logger.info(f"[orchestrator] Redis returned {len(raw_candidates)} raw candidates for '{msg.topic}': {raw_candidates}")
        candidates = await filter_speculative_candidates(msg.topic, raw_candidates, config.SPECULATION_BUDGET)
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
        await ctx.send(config.SPECULATIVE_WORKER_ADDRESS, ResearchRequest(topic=candidate, session_id=f"spec-{candidate}", is_speculative=True))


@orchestrator.on_message(model=ResearchResult)
async def on_result(ctx: Context, sender: str, msg: ResearchResult):  # noqa: ARG001
    if msg.session_id.startswith("spec-"):
        ctx.logger.info(f"[orchestrator] speculative result for '{msg.topic}' registered as warm")
        return

    requester = _pending.pop(msg.session_id, None)
    if requester:
        ctx.logger.info(f"[orchestrator] forwarding '{msg.topic}' → dev agent")
        await ctx.send(requester, msg)
    else:
        ctx.logger.warning(f"[orchestrator] no pending requester for session '{msg.session_id}'")

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
            await ctx.send(config.PRIMARY_WORKER_ADDRESS, ResearchRequest(topic=concept, session_id=f"spec-{key}", is_speculative=True))
