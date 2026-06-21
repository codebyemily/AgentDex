import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import anthropic
from browserbase import Browserbase
from playwright.sync_api import sync_playwright

_executor = ThreadPoolExecutor(max_workers=8)


def _crawl_sync(topic: str) -> str:
    """Runs in a thread — Browserbase + Playwright are synchronous."""
    t0 = time.time()
    print(f"[pipeline:crawl] '{topic}' — creating Browserbase session...")
    bb = Browserbase(api_key=os.environ["BROWSERBASE_API_KEY"])
    session = bb.sessions.create(project_id=os.environ["BROWSERBASE_PROJECT_ID"])
    print(f"[pipeline:crawl] '{topic}' — session created (id={session.id})")
    try:
        with sync_playwright() as pw:
            print(f"[pipeline:crawl] '{topic}' — connecting Playwright over CDP...")
            browser = pw.chromium.connect_over_cdp(session.connect_url)
            page = browser.contexts[0].pages[0]

            slug = quote(topic.replace(" ", "_"), safe="")
            url = f"https://en.wikipedia.org/wiki/{slug}"
            print(f"[pipeline:crawl] '{topic}' — navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=15_000)
            print(f"[pipeline:crawl] '{topic}' — page loaded, extracting #mw-content-text...")

            try:
                text = page.locator("#mw-content-text").inner_text(timeout=5_000)
            except Exception:
                print(f"[pipeline:crawl] '{topic}' — #mw-content-text not found, falling back to body")
                text = page.inner_text("body") or ""

            page.close()
            browser.close()

        truncated = text[:4_000]
        print(
            f"[pipeline:crawl] '{topic}' — done  "
            f"raw={len(text)} chars  truncated={len(truncated)} chars  "
            f"elapsed={time.time()-t0:.2f}s"
        )
        return truncated
    except Exception as exc:
        print(f"[pipeline:crawl] '{topic}' — ERROR: {exc}")
        return f"[crawl error: {exc}]"


async def crawl_topic(topic: str) -> str:
    print(f"[pipeline:crawl] '{topic}' — dispatching to thread pool")
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _crawl_sync, topic)


async def classify_and_structure(topic: str, raw: str) -> dict:
    print(f"[pipeline:classify] '{topic}' — calling Claude (input={len(raw)} chars)...")
    t0 = time.time()
    client = anthropic.AsyncAnthropic()
    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2_048,
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
        result = json.loads(resp.content[0].text)
        print(
            f"[pipeline:classify] '{topic}' — done  "
            f"content_type={result.get('content_type')}  "
            f"key_facts={len(result.get('key_facts', []))}  "
            f"related_concepts={result.get('related_concepts', [])}  "
            f"mcp_tools={len(result.get('mcp_tools', []))}  "
            f"elapsed={time.time()-t0:.2f}s"
        )
        return result
    except Exception as exc:
        print(f"[pipeline:classify] '{topic}' — JSON parse error: {exc}  falling back to raw excerpt")
        return {
            "content_type": "prose",
            "summary": raw[:300],
            "key_facts": [],
            "related_concepts": [],
            "mcp_tools": [],
        }


async def get_speculative_candidates(topic: str, budget: int = 3) -> list[str]:
    """Ask Claude which topics a researcher would most likely ask about next."""
    print(f"[pipeline:speculation] '{topic}' — cold-start: asking Claude for {budget} follow-up candidates...")
    t0 = time.time()
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
        result = [c for c in candidates if isinstance(c, str)][:budget]
        print(f"[pipeline:speculation] '{topic}' — candidates: {result}  elapsed={time.time()-t0:.2f}s")
        return result
    except Exception as exc:
        print(f"[pipeline:speculation] '{topic}' — parse error: {exc}  returning []")
        return []


async def filter_speculative_candidates(
    topic: str,
    raw_candidates: list[str],
    budget: int = 3,
) -> list[str]:
    """Filter a precomputed vector-similarity list down to genuine next-question candidates.

    Redis finds topics that are *similar*; this call asks Claude which of those are
    actually *likely to be the next question*, which is the distinction the PRD calls
    the core IP.  Only returns strings that appear in raw_candidates (safety check).
    """
    if not raw_candidates:
        return []

    normalized_topic = topic.lower().strip()
    valid_raw: dict[str, str] = {
        c.lower().strip(): c
        for c in raw_candidates
        if c.lower().strip() != normalized_topic
    }
    if not valid_raw:
        return []

    print(
        f"[pipeline:speculation] '{topic}' — filtering {len(valid_raw)} KNN candidates via Claude: {list(valid_raw.keys())}"
    )
    t0 = time.time()
    client = anthropic.AsyncAnthropic()
    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        messages=[
            {
                "role": "user",
                "content": (
                    f'A researcher just asked about "{topic}". '
                    "Here are semantically similar topics found via vector search:\n"
                    f"{json.dumps(list(valid_raw.keys()))}\n\n"
                    f"From this list, select at most {budget} topics that a researcher "
                    "would most likely ask about NEXT — meaning they are a genuine "
                    "follow-up question, not merely thematically similar.\n\n"
                    "Rules:\n"
                    "- Only return topics from the provided list\n"
                    "- Exclude topics that are similar but not a natural next step\n"
                    "- Each returned topic must have its own Wikipedia page\n\n"
                    'Example for "atoms": "electrons" is a good bet; '
                    '"history of atomic theory" is similar but a bad bet.\n\n'
                    "Return only a valid JSON array of strings — no markdown, no extra text."
                ),
            }
        ],
    )
    try:
        selected = json.loads(resp.content[0].text)
        filtered = [
            valid_raw[c.lower().strip()]
            for c in selected
            if isinstance(c, str) and c.lower().strip() in valid_raw
        ]
        result = filtered[:budget]
        print(f"[pipeline:speculation] '{topic}' — Claude selected: {result}  elapsed={time.time()-t0:.2f}s")
        return result
    except Exception as exc:
        fallback = list(valid_raw.values())[:budget]
        print(f"[pipeline:speculation] '{topic}' — parse error: {exc}  falling back to top-{budget}: {fallback}")
        return fallback


async def research_topic(topic: str) -> dict:
    print(f"[pipeline] '{topic}' — starting full research (crawl → classify)")
    t0 = time.time()
    raw = await crawl_topic(topic)
    structured = await classify_and_structure(topic, raw)
    result = {"topic": topic, **structured}
    print(f"[pipeline] '{topic}' — research complete  elapsed={time.time()-t0:.2f}s")
    return result
