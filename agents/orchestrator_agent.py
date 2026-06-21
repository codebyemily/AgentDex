import json
import time

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.cache import get_warm, all_topics
from shared.messages import TopicQuery, ResearchRequest, ResearchResult
from shared.pipeline import get_speculative_candidates
from shared.demo_events import emit_demo_event

load_dotenv()

orchestrator = Agent(
    name="orchestrator",
    seed=config.ORCHESTRATOR_SEED,
    handle_messages_concurrently=True,
)

# session_id → address of the dev agent waiting for this result
_pending: dict[str, str] = {}


@orchestrator.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[orchestrator] started  address={ctx.agent.address}")


@orchestrator.on_message(model=TopicQuery)
async def on_query(ctx: Context, sender: str, msg: TopicQuery):
    ctx.logger.info(f"[orchestrator] query: '{msg.topic}'  session={msg.session_id}")
    emit_demo_event(
        "query_received", {"topic": msg.topic, "session_id": msg.session_id}
    )

    # ── Warm-cache check ──────────────────────────────────────────────────────
    cached = get_warm(msg.topic)
    if cached:
        ctx.logger.info(f"[orchestrator] WARM HIT — serving '{msg.topic}' instantly")
        emit_demo_event(
            "warm_hit",
            {
                "topic": msg.topic,
                "session_id": msg.session_id,
                "warmed_at": cached.get("cached_at"),
            },
        )
        await ctx.send(
            sender,
            ResearchResult(
                topic=msg.topic,
                session_id=msg.session_id,
                summary=cached.get("summary", ""),
                content_type=cached.get("content_type", "prose"),
                key_facts=json.dumps(cached.get("key_facts", [])),
                related_concepts=json.dumps(cached.get("related_concepts", [])),
                mcp_tools=json.dumps(cached.get("mcp_tools", [])),
                warm=True,
                timestamp=cached.get("cached_at", time.time()),
            ),
        )
        return

    # ── Dispatch primary worker ───────────────────────────────────────────────
    _pending[msg.session_id] = sender
    ctx.logger.info(f"[orchestrator] dispatching primary worker for '{msg.topic}'")
    emit_demo_event(
        "cold_dispatch", {"topic": msg.topic, "session_id": msg.session_id}
    )
    await ctx.send(
        config.PRIMARY_WORKER_ADDRESS,
        ResearchRequest(topic=msg.topic, session_id=msg.session_id, is_speculative=False),
    )

    # ── Speculative expansion (runs while primary worker is browsing) ─────────
    candidates = await get_speculative_candidates(msg.topic, config.SPECULATION_BUDGET)
    warm_set = set(all_topics())
    ctx.logger.info(f"[orchestrator] speculative candidates: {candidates}")
    emit_demo_event(
        "speculation_planned", {"parent": msg.topic, "candidates": candidates}
    )

    for candidate in candidates:
        key = candidate.lower().strip()
        if key in warm_set:
            ctx.logger.info(f"[orchestrator] '{candidate}' already warm — skipping")
            emit_demo_event(
                "candidate_skipped", {"topic": candidate, "reason": "already_warm"}
            )
            continue
        ctx.logger.info(f"[orchestrator] dispatching speculative bet for '{candidate}'")
        emit_demo_event("spec_dispatch", {"topic": candidate, "parent": msg.topic})
        await ctx.send(
            config.PRIMARY_WORKER_ADDRESS,
            ResearchRequest(
                topic=candidate,
                session_id=f"spec-{candidate}",
                is_speculative=True,
            ),
        )


@orchestrator.on_message(model=ResearchResult)
async def on_result(ctx: Context, sender: str, msg: ResearchResult):
    # Speculative results just warm the cache — no dev agent is waiting
    if msg.session_id.startswith("spec-"):
        ctx.logger.info(
            f"[orchestrator] speculative result for '{msg.topic}' registered as warm"
        )
        return

    requester = _pending.pop(msg.session_id, None)
    if requester:
        ctx.logger.info(f"[orchestrator] forwarding '{msg.topic}' → dev agent")
        await ctx.send(requester, msg)
    else:
        ctx.logger.warning(
            f"[orchestrator] no pending requester for session '{msg.session_id}'"
        )