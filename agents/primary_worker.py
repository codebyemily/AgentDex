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
    sentry_sdk.init(dsn=os.environ["SENTRY_DSN"])

# handle_messages_concurrently lets this worker process the live query and the
# speculative bets at the same time without queuing them behind each other
primary_worker = Agent(
    name="primary_worker",
    seed=config.PRIMARY_WORKER_SEED,
    handle_messages_concurrently=True,
)


@primary_worker.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[primary_worker] started  address={ctx.agent.address}")


@primary_worker.on_message(model=ResearchRequest)
async def on_request(ctx: Context, sender: str, msg: ResearchRequest):
    if msg.is_speculative:
        await _handle_speculative(ctx, sender, msg)
    else:
        await _handle_live(ctx, sender, msg)


async def _handle_live(ctx: Context, sender: str, msg: ResearchRequest):
    """The live query a dev agent is waiting on — research, warm, and reply."""
    ctx.logger.info(f"[primary_worker] researching '{msg.topic}' via Browserbase...")

    result = await research_topic(msg.topic)
    set_warm(msg.topic, result)

    ctx.logger.info(f"[primary_worker] done — '{msg.topic}' ingested and warm")
    await ctx.send(sender, _to_result(msg, result))


async def _handle_speculative(ctx: Context, sender: str, msg: ResearchRequest):
    """A speculative bet — time-boxed so a slow page can't tie up the worker."""
    ctx.logger.info(f"[primary_worker] betting on '{msg.topic}'...")

    try:
        result = await asyncio.wait_for(
            research_topic(msg.topic),
            timeout=config.SPECULATIVE_TIMEOUT_SECS,
        )
        set_warm(msg.topic, result)
        ctx.logger.info(
            f"[primary_worker] '{msg.topic}' is now WARM — ready for instant serving"
        )

        # Notify orchestrator so it can log the warm registration
        await ctx.send(sender, _to_result(msg, result))
    except asyncio.TimeoutError:
        ctx.logger.warning(
            f"[primary_worker] TIMEOUT on '{msg.topic}' — wasted bet, discarded"
        )
        if os.getenv("SENTRY_DSN"):
            sentry_sdk.capture_message(
                f"Speculative worker timeout: {msg.topic}", level="warning"
            )


def _to_result(msg: ResearchRequest, result: dict) -> ResearchResult:
    return ResearchResult(
        topic=msg.topic,
        session_id=msg.session_id,
        summary=result.get("summary", ""),
        content_type=result.get("content_type", "prose"),
        key_facts=json.dumps(result.get("key_facts", [])),
        related_concepts=json.dumps(result.get("related_concepts", [])),
        mcp_tools=json.dumps(result.get("mcp_tools", [])),
        warm=False,
        timestamp=time.time(),
    )
