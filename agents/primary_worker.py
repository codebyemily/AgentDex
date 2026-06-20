import json
import time

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.cache import set_warm
from shared.messages import ResearchRequest, ResearchResult
from shared.pipeline import research_topic

load_dotenv()

primary_worker = Agent(name="primary_worker", seed=config.PRIMARY_WORKER_SEED)


@primary_worker.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[primary_worker] started  address={ctx.agent.address}")


@primary_worker.on_message(model=ResearchRequest)
async def on_request(ctx: Context, sender: str, msg: ResearchRequest):
    ctx.logger.info(f"[primary_worker] researching '{msg.topic}' via Browserbase...")

    result = await research_topic(msg.topic)
    set_warm(msg.topic, result)

    ctx.logger.info(f"[primary_worker] done — '{msg.topic}' ingested and warm")
    await ctx.send(
        sender,
        ResearchResult(
            topic=msg.topic,
            session_id=msg.session_id,
            summary=result.get("summary", ""),
            content_type=result.get("content_type", "prose"),
            key_facts=json.dumps(result.get("key_facts", [])),
            related_concepts=json.dumps(result.get("related_concepts", [])),
            mcp_tools=json.dumps(result.get("mcp_tools", [])),
            warm=False,
            timestamp=time.time(),
        ),
    )
