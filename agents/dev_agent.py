import asyncio
import json

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.messages import TopicQuery, ResearchResult

load_dotenv()

dev_agent = Agent(name="dev_agent", seed=config.DEV_AGENT_SEED)

# Two-query demo: "atoms" is cold, "electrons" should be a warm hit
_DEMO_TOPICS = ["atoms", "electrons"]
_query_index = 0


@dev_agent.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[dev_agent] started  address={ctx.agent.address}")
    await asyncio.sleep(2)  # give other agents time to register
    topic = _DEMO_TOPICS[0]
    ctx.logger.info(f"[dev_agent] ── querying: '{topic}'")
    await ctx.send(
        config.ORCHESTRATOR_ADDRESS,
        TopicQuery(topic=topic, session_id="demo-1"),
    )


@dev_agent.on_message(model=ResearchResult)
async def on_result(ctx: Context, sender: str, msg: ResearchResult):
    global _query_index

    hit_label = "*** WARM HIT ***" if msg.warm else "cold run"
    facts = json.loads(msg.key_facts) if msg.key_facts else []

    ctx.logger.info(f"[dev_agent] [{hit_label}] topic='{msg.topic}'")
    ctx.logger.info(f"[dev_agent]   content_type : {msg.content_type}")
    ctx.logger.info(f"[dev_agent]   summary      : {msg.summary}")
    ctx.logger.info(f"[dev_agent]   top facts    : {facts[:3]}")

    _query_index += 1
    if _query_index < len(_DEMO_TOPICS):
        next_topic = _DEMO_TOPICS[_query_index]
        ctx.logger.info(f"[dev_agent] thinking... (5 s)")
        await asyncio.sleep(5)
        ctx.logger.info(f"[dev_agent] ── querying: '{next_topic}'")
        await ctx.send(
            config.ORCHESTRATOR_ADDRESS,
            TopicQuery(topic=next_topic, session_id=f"demo-{_query_index + 1}"),
        )
