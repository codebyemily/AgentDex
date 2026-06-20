import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import anthropic
from browserbase import Browserbase
from playwright.sync_api import sync_playwright

_executor = ThreadPoolExecutor(max_workers=8)


def _crawl_sync(topic: str) -> str:
    """Runs in a thread — Browserbase + Playwright are synchronous."""
    bb = Browserbase(api_key=os.environ["BROWSERBASE_API_KEY"])
    session = bb.sessions.create(project_id=os.environ["BROWSERBASE_PROJECT_ID"])
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(session.connect_url)
            page = browser.contexts[0].pages[0]

            slug = quote(topic.replace(" ", "_"), safe="")
            page.goto(
                f"https://en.wikipedia.org/wiki/{slug}",
                wait_until="domcontentloaded",
                timeout=15_000,
            )

            # Grab the article body; fall back to full page text
            try:
                text = page.locator("#mw-content-text").inner_text(timeout=5_000)
            except Exception:
                text = page.inner_text("body") or ""

            page.close()
            browser.close()

        return text[:4_000]
    except Exception as exc:
        return f"[crawl error: {exc}]"


async def crawl_topic(topic: str) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _crawl_sync, topic)


async def classify_and_structure(topic: str, raw: str) -> dict:
    client = anthropic.AsyncAnthropic()
    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1_024,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Topic: {topic}\n\n"
                    f"Raw content (Wikipedia excerpt):\n{raw}\n\n"
                    "Return a JSON object with exactly these keys:\n"
                    '- content_type: "tabular" if the data fits a table, "prose" if narrative\n'
                    "- summary: 2-3 sentence plain-English summary\n"
                    "- key_facts: array of 5-7 concise facts\n"
                    "- related_concepts: array of 3-5 topics a researcher would likely ask about next\n"
                    "- mcp_tools: array of 1-3 objects, each with keys name, description, input_schema\n\n"
                    "Return only valid JSON — no markdown fences, no extra text."
                ),
            }
        ],
    )
    try:
        return json.loads(resp.content[0].text)
    except Exception:
        return {
            "content_type": "prose",
            "summary": raw[:300],
            "key_facts": [],
            "related_concepts": [],
            "mcp_tools": [],
        }


async def get_speculative_candidates(topic: str, budget: int = 3) -> list[str]:
    """Ask Claude which topics a researcher would most likely ask about next."""
    client = anthropic.AsyncAnthropic()
    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        messages=[
            {
                "role": "user",
                "content": (
                    f'A researcher just asked about "{topic}". '
                    f"List the {budget} most likely follow-up topics they will ask about next.\n\n"
                    "Rules:\n"
                    "- Conceptually adjacent, not just thematically similar\n"
                    "- Each topic must have its own Wikipedia page\n"
                    "- No meta/historical topics (e.g. 'history of X', 'X in popular culture')\n\n"
                    f'Example for "atoms": ["electrons", "protons", "atomic nucleus"]\n\n'
                    "Return only a valid JSON array of strings — no markdown, no extra text."
                ),
            }
        ],
    )
    try:
        candidates = json.loads(resp.content[0].text)
        return [c for c in candidates if isinstance(c, str)][:budget]
    except Exception:
        return []


async def research_topic(topic: str) -> dict:
    raw = await crawl_topic(topic)
    structured = await classify_and_structure(topic, raw)
    return {"topic": topic, **structured}
