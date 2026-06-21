#!/usr/bin/env python3
"""
Validate Person A: Redis vector index + Claude relevance filter.

Run from the project root:
    python scripts/test_person_a.py

Prerequisites:
  - Redis running at $REDIS_URL (default: redis://localhost:6379)
  - ANTHROPIC_API_KEY set in .env or environment
  - pip install -r requirements.txt   (first run downloads ~80 MB model)
"""

import asyncio
import sys
from pathlib import Path

# Make `shared` and `agents` importable when running from scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

# ── Seed data ─────────────────────────────────────────────────────────────────

_SEED: dict[str, dict] = {
    "atoms": {
        "summary": "Atoms are the basic units of matter and the defining structure of elements.",
        "content_type": "prose",
        "key_facts": ["Made of protons, neutrons, and electrons", "Mostly empty space"],
        "related_concepts": ["electrons", "protons", "atomic nucleus"],
        "mcp_tools": [],
    },
    "electrons": {
        "summary": "Electrons are negatively charged subatomic particles that orbit an atomic nucleus.",
        "content_type": "prose",
        "key_facts": ["Carry negative charge", "Mass ~1/1836 of a proton"],
        "related_concepts": ["atoms", "protons", "quantum mechanics"],
        "mcp_tools": [],
    },
    "protons": {
        "summary": "Protons are positively charged subatomic particles found in the atomic nucleus.",
        "content_type": "prose",
        "key_facts": ["Carry positive charge", "Atomic number equals proton count"],
        "related_concepts": ["atoms", "neutrons", "nuclear physics"],
        "mcp_tools": [],
    },
    "chemistry": {
        "summary": "Chemistry is the scientific study of the properties and behavior of matter.",
        "content_type": "prose",
        "key_facts": ["Studies matter and its transformations", "Organic and inorganic branches"],
        "related_concepts": ["atoms", "molecules", "chemical reactions"],
        "mcp_tools": [],
    },
    "periodic table": {
        "summary": "The periodic table organizes chemical elements by atomic number and properties.",
        "content_type": "tabular",
        "key_facts": ["118 confirmed elements", "Organized by atomic number"],
        "related_concepts": ["chemistry", "atoms", "elements"],
        "mcp_tools": [],
    },
}

QUERY_TOPIC = "atoms"
BUDGET = 3


def _sep(title: str) -> None:
    print(f"\n{'─' * 60}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'─' * 60}", flush=True)


def _step(msg: str) -> None:
    print(f"  → {msg}", flush=True)


async def main() -> None:
    import os
    _step(f"REDIS_URL = {os.environ.get('REDIS_URL', '(not set — will use redis://localhost:6379)')}")

    from shared.redis_client import set_warm, get_warm, search_nearest, all_topics
    from shared.pipeline import filter_speculative_candidates

    # ── 1. Upsert seed topics ──────────────────────────────────────────────────
    _sep("1 · Upserting seed topics")
    print("  (First run downloads ~80 MB sentence-transformer model — be patient)", flush=True)
    for topic, data in _SEED.items():
        _step(f"calling set_warm('{topic}')...")
        await set_warm(topic, data)
        print(f"  ✓  '{topic}' warm", flush=True)

    # ── 2. all_topics() ────────────────────────────────────────────────────────
    _sep("2 · all_topics()")
    _step("calling all_topics()...")
    warm = set(await all_topics())
    expected = {t.lower() for t in _SEED}
    print(f"  returned : {sorted(warm)}", flush=True)
    assert expected == warm, f"FAIL: expected {sorted(expected)}, got {sorted(warm)}"
    print(f"  ✓  matches seed set", flush=True)

    # ── 3. get_warm() — exact key lookup + field deserialization ───────────────
    _sep(f"3 · get_warm('{QUERY_TOPIC}')")
    _step(f"calling get_warm('{QUERY_TOPIC}')...")
    hit = await get_warm(QUERY_TOPIC)
    assert hit is not None, f"FAIL: get_warm('{QUERY_TOPIC}') returned None"
    assert hit["summary"], "FAIL: summary is empty"
    assert isinstance(hit["key_facts"], list), \
        f"FAIL: key_facts should be list, got {type(hit['key_facts'])}"
    assert isinstance(hit["related_concepts"], list), \
        f"FAIL: related_concepts should be list, got {type(hit['related_concepts'])}"
    print(f"  summary      : {hit['summary'][:70]}…", flush=True)
    print(f"  content_type : {hit['content_type']}", flush=True)
    print(f"  key_facts    : {hit['key_facts']}", flush=True)
    print(f"  ✓  warm hit, fields deserialized correctly", flush=True)

    # ── 4. search_nearest() ────────────────────────────────────────────────────
    _sep(f"4 · search_nearest('{QUERY_TOPIC}', k=10)")
    _step(f"calling search_nearest('{QUERY_TOPIC}', k=10)...")
    raw = await search_nearest(QUERY_TOPIC, k=10)
    print(f"  raw candidates : {raw}", flush=True)
    assert len(raw) > 0, "FAIL: search_nearest returned no results (is Redis populated?)"
    assert QUERY_TOPIC.lower() not in [c.lower() for c in raw], \
        f"FAIL: query topic '{QUERY_TOPIC}' appeared in its own nearest-neighbors"
    print(f"  ✓  {len(raw)} result(s), query topic correctly excluded", flush=True)

    # ── 5. filter_speculative_candidates() ────────────────────────────────────
    _sep(f"5 · filter_speculative_candidates('{QUERY_TOPIC}', raw, budget={BUDGET})")
    _step(f"calling filter_speculative_candidates (Claude API)...")
    kept = await filter_speculative_candidates(QUERY_TOPIC, raw, budget=BUDGET)
    rejected = [c for c in raw if c not in kept]
    print(f"  Raw      ({len(raw):2d}): {raw}", flush=True)
    print(f"  Kept     ({len(kept):2d}): {kept}", flush=True)
    print(f"  Rejected ({len(rejected):2d}): {rejected}", flush=True)
    assert len(kept) <= BUDGET, \
        f"FAIL: Claude returned {len(kept)} candidates, budget is {BUDGET}"
    stray = [k for k in kept if k not in raw]
    assert not stray, f"FAIL: Claude hallucinated topics not in raw list: {stray}"
    print(f"  ✓  budget={BUDGET} respected, all kept topics are from raw list", flush=True)

    # ── 6. Cold-start path (structural check — not re-runnable once seeded) ────
    _sep("6 · Cold-start path (structural)")
    print("  When the index is EMPTY, _search_nearest_sync catches the Redis error", flush=True)
    print("  and returns [].  The orchestrator's 'if raw_candidates' branch then", flush=True)
    print("  falls back to get_speculative_candidates() (Claude-only generation).", flush=True)

    # ── 7. main.py import compatibility ────────────────────────────────────────
    _sep("7 · main.py import compatibility")
    _step("importing all agents...")
    import shared.config as config        # noqa: F401
    from shared.cache import get_warm as gw, set_warm as sw, all_topics as at  # noqa: F401
    from agents.orchestrator_agent import orchestrator   # noqa: F401
    from agents.primary_worker import primary_worker     # noqa: F401
    from agents.speculative_worker import speculative_worker  # noqa: F401
    from agents.dev_agent import dev_agent               # noqa: F401
    print("  ✓  all agent and shared imports resolve without error", flush=True)

    print(f"\n{'=' * 60}", flush=True)
    print("  All checks passed — Person A implementation is valid.", flush=True)
    print(f"{'=' * 60}\n", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
