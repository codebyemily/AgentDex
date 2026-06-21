"""MCP server exposing AgentDex research pipeline as tools.

Run with: python mcp_server.py
Or configure in Claude Desktop as a stdio MCP server.
"""

import json

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

mcp = FastMCP("AgentDex")


@mcp.tool()
async def research_topic(topic: str) -> str:
    """Crawl Wikipedia for a topic, classify with Claude, and store in the vector cache.

    Returns a JSON object with keys: topic, summary, content_type, key_facts,
    related_concepts, mcp_tools.
    """
    from shared.pipeline import research_topic as _pipeline_research
    from shared.redis_client import set_warm

    result = await _pipeline_research(topic)
    await set_warm(topic, result)
    return json.dumps(result)


@mcp.tool()
async def get_cached_topic(topic: str) -> str:
    """Return the cached research result for a topic as JSON, or empty object if not cached."""
    from shared.redis_client import get_warm

    cached = await get_warm(topic)
    return json.dumps(cached or {})


@mcp.tool()
async def search_similar_topics(query: str, k: int = 5) -> str:
    """Find topics in the vector cache most semantically similar to the query.

    Returns a JSON array of objects with keys: topic, distance.
    Distance is in [0, 2] for cosine metric; values below ~0.15 indicate a strong match.
    """
    from shared.redis_client import search_nearest_with_scores

    results = await search_nearest_with_scores(query, k=k)
    return json.dumps([{"topic": t, "distance": round(d, 4)} for t, d in results])


@mcp.tool()
async def list_warm_topics() -> str:
    """Return a JSON array of all topic strings currently stored in the vector cache."""
    from shared.redis_client import all_topics

    topics = await all_topics()
    return json.dumps(sorted(topics))


if __name__ == "__main__":
    mcp.run()
