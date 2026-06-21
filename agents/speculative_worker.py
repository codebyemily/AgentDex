import asyncio
import json
import os
import time

from dotenv import load_dotenv
from uagents import Agent, Context

import sentry_sdk
import shared.config as config
from shared.cache import set_warm
from shared.messages import ResearchRequest, ResearchResult
from shared.pipeline import research_topic

load_dotenv()

if os.getenv("SENTRY_DSN"):
    try:
        sentry_sdk.init(dsn=os.environ["SENTRY_DSN"])
    except Exception:
        pass

# handle_messages_concurrently lets this agent process multiple
# speculative requests at the same time without queuing them
speculative_worker = Agent(
    name="speculative_worker",
    seed=config.SPECULATIVE_WORKER_SEED,
    handle_messages_concurrently=True,
)


@speculative_worker.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[speculative_worker] started  address={ctx.agent.address}")


@speculative_worker.on_message(model=ResearchRequest)
async def on_request(ctx: Context, sender: str, msg: ResearchRequest):
    ctx.logger.info(f"[speculative_worker] betting on '{msg.topic}'...")

    try:
        result = await asyncio.wait_for(
            research_topic(msg.topic),
            timeout=config.SPECULATIVE_TIMEOUT_SECS,
        )
        await set_warm(msg.topic, result)
        ctx.logger.info(
            f"[speculative_worker] '{msg.topic}' is now WARM — ready for instant serving"
        )

        # Notify orchestrator so it can log the warm registration
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
    except asyncio.TimeoutError:
        ctx.logger.warning(
            f"[speculative_worker] TIMEOUT on '{msg.topic}' — wasted bet, discarded"
        )
        if os.getenv("SENTRY_DSN"):
            sentry_sdk.capture_message(
                f"Speculative worker timeout: {msg.topic}", level="warning"
            )
