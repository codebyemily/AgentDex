"""Developer agent — sends a batch of topics to AgentDex and receives back
structured research + the MCP tool definitions AgentDex generated for each.

Flow:
  1. Sends TopicBatch with all topics at once.
  2. Orchestrator fans them out in parallel to primary_worker(s).
  3. Each worker crawls Wikipedia, classifies with Claude (generating mcp_tools),
     and stores the result in Redis.
  4. When all are done, orchestrator sends a BatchResult back here.
  5. dev_agent logs the MCP tools now callable via mcp_server.py.
"""

import asyncio
import json

from dotenv import load_dotenv
from uagents import Agent, Context

import shared.config as config
from shared.messages import TopicBatch, BatchResult
from shared.demo_events import emit_demo_event

load_dotenv()

dev_agent = Agent(name="dev_agent", seed=config.DEV_AGENT_SEED)

_BATCH_SESSION = "demo-batch-1"
_DEMO_TOPICS = ["atoms", "electrons", "protons", "quantum mechanics", "periodic table"]


@dev_agent.on_event("startup")
async def on_start(ctx: Context):
    ctx.logger.info(f"[dev_agent] started  address={ctx.agent.address}")
    await asyncio.sleep(2)  # give other agents time to register

    ctx.logger.info(f"[dev_agent] ── sending batch of {len(_DEMO_TOPICS)} topics to AgentDex ──")
    ctx.logger.info(f"[dev_agent]   topics: {_DEMO_TOPICS}")
    ctx.logger.info(f"[dev_agent]   AgentDex will research all in parallel and generate MCP tools for each")
    emit_demo_event("batch_query", {"topics": _DEMO_TOPICS, "session_id": _BATCH_SESSION})

    await ctx.send(
        config.ORCHESTRATOR_ADDRESS,
        TopicBatch(topics=json.dumps(_DEMO_TOPICS), session_id=_BATCH_SESSION),
    )


@dev_agent.on_message(model=BatchResult)
async def on_batch_result(ctx: Context, sender: str, msg: BatchResult):
    results = json.loads(msg.results)

    ctx.logger.info(f"[dev_agent] BatchResult received from {sender}  session={msg.session_id}")
    ctx.logger.info(f"[dev_agent] {len(results)} topics researched:")

    for r in results:
        hit_label = "WARM HIT" if r.get("warm") else "cold run"
        facts = r.get("key_facts", [])
        related = r.get("related_concepts", [])
        tools = r.get("mcp_tools", [])

        ctx.logger.info(f"[dev_agent]   ── [{hit_label}] {r['topic']} ──")
        ctx.logger.info(f"[dev_agent]     content_type     : {r.get('content_type')}")
        ctx.logger.info(f"[dev_agent]     summary          : {r.get('summary', '')[:100]}...")
        ctx.logger.info(f"[dev_agent]     key_facts ({len(facts)})    : {facts[:3]}")
        ctx.logger.info(f"[dev_agent]     related_concepts : {related}")
        ctx.logger.info(f"[dev_agent]     mcp_tools ({len(tools)})    : {[t.get('name') for t in tools]}")

    emit_demo_event("batch_result", {
        "session_id": msg.session_id,
        "topics": [r["topic"] for r in results],
        "warm_count": sum(1 for r in results if r.get("warm")),
    })

    # ── MCP tools now accessible via mcp_server.py ───────────────────────────
    ctx.logger.info(f"[dev_agent] ── MCP tools now available via mcp_server.py ──")
    ctx.logger.info(f"[dev_agent]   (connect with: python mcp_server.py)")
    for r in results:
        tools = r.get("mcp_tools", [])
        if tools:
            for t in tools:
                ctx.logger.info(
                    f"[dev_agent]   {r['topic']:20s} → {t.get('name', '?'):30s}  {t.get('description', '')[:60]}"
                )
        else:
            ctx.logger.info(f"[dev_agent]   {r['topic']:20s} → (no tools generated)")

    ctx.logger.info(f"[dev_agent] ── done ──")
