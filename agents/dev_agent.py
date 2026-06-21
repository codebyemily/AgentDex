"""The fake "developer's agent" that drives the demo.

It asks two questions in sequence:
  1. "atoms"     — cold. Nothing is warm yet, so the primary worker browses it
                   live AND the orchestrator speculatively pre-warms likely
                   follow-ups (electrons, protons, ...).
  2. "electrons" — by the time we ask, a speculative worker has already warmed
                   it. The orchestrator serves it instantly: a WARM HIT.

The panel's headline proof is the gap between when 'electrons' went warm
(spec_warm event) and when we actually asked for it (dev_query event).
"""

import asyncio
import json

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.messages import TopicQuery, ResearchResult
from shared.demo_events import emit_demo_event

load_dotenv()

dev_agent = Agent(name="dev_agent", seed=config.DEV_AGENT_SEED)

# Two-query demo: "atoms" is cold, "electrons" should be a warm hit.
_DEMO_TOPICS = ["atoms", "electrons"]
_query_index = 0

# How long the "developer" appears to think between questions. This is also the
# window the speculative worker has to finish warming "electrons".
_THINK_SECS = 8


async def _ask(ctx: Context, topic: str, session_id: str):
    ctx.logger.info(f"[dev_agent] ── querying: '{topic}'")
    emit_demo_event("dev_query", {"topic": topic, "session_id": session_id})
    await ctx.send(config.ORCHESTRATOR_ADDRESS, TopicQuery(topic=topic, session_id=session_id))


@dev_agent.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[dev_agent] started  address={ctx.agent.address}")
    await asyncio.sleep(2)  # give other agents time to register
    await _ask(ctx, _DEMO_TOPICS[0], "demo-1")


@dev_agent.on_message(model=ResearchResult)
async def on_result(ctx: Context, sender: str, msg: ResearchResult):
    global _query_index

    hit_label = "*** WARM HIT ***" if msg.warm else "cold run"
    facts = json.loads(msg.key_facts) if msg.key_facts else []

    ctx.logger.info(f"[dev_agent] [{hit_label}] topic='{msg.topic}'")
    ctx.logger.info(f"[dev_agent]   content_type : {msg.content_type}")
    ctx.logger.info(f"[dev_agent]   summary      : {msg.summary}")
    ctx.logger.info(f"[dev_agent]   top facts    : {facts[:3]}")

    emit_demo_event(
        "dev_result",
        {
            "topic": msg.topic,
            "session_id": msg.session_id,
            "warm": msg.warm,
            "summary": msg.summary,
        },
    )

    _query_index += 1
    if _query_index < len(_DEMO_TOPICS):
        next_topic = _DEMO_TOPICS[_query_index]
        ctx.logger.info(f"[dev_agent] thinking... ({_THINK_SECS}s)")
        await asyncio.sleep(_THINK_SECS)
        await _ask(ctx, next_topic, f"demo-{_query_index + 1}")
