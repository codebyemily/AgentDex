"""Scripted demo replay → the live-demo fallback.

Emits a realistic, correctly-timed event sequence into the same Redis stream the
real pipeline uses, so the panel renders the entire story — speculative workers
warming follow-ups, then a WARM HIT served before the question was asked —
without depending on Browserbase being fast (or up) on stage.

Use it to:
  • develop/style the panel without running the full Bureau
  • rehearse the panel narration
  • fall back to a known-good run if the live demo gets flaky

Run (with the panel open at http://localhost:8000):
    python -m panel.demo_replay
"""

import time

from dotenv import load_dotenv

from shared.demo_events import emit_demo_event, reset

load_dotenv()

# (delay_before_seconds, event_type, data)
SCRIPT = [
    (0.0, "dev_query", {"topic": "atoms", "session_id": "demo-1"}),
    (0.4, "cold_dispatch", {"topic": "atoms", "session_id": "demo-1"}),
    (0.3, "cold_started", {"topic": "atoms", "session_id": "demo-1"}),
    (0.6, "speculation_planned",
     {"parent": "atoms", "candidates": ["electrons", "protons", "atomic nucleus"]}),
    # Person A's relevance filter drops an off-topic candidate before any bet.
    (0.3, "candidate_rejected",
     {"topic": "ancient greek philosophy", "reason": "similarity 0.31 < 0.70 threshold"}),
    (0.3, "spec_dispatch", {"topic": "electrons", "parent": "atoms"}),
    (0.2, "spec_dispatch", {"topic": "protons", "parent": "atoms"}),
    (0.2, "spec_dispatch", {"topic": "atomic nucleus", "parent": "atoms"}),
    (0.2, "spec_started", {"topic": "electrons"}),
    (0.2, "spec_started", {"topic": "protons"}),
    (0.2, "spec_started", {"topic": "atomic nucleus"}),
    (3.5, "spec_warm", {"topic": "electrons"}),     # ← ready well before we ask
    (1.5, "cold_done", {"topic": "atoms", "session_id": "demo-1"}),
    (0.3, "dev_result", {"topic": "atoms", "session_id": "demo-1", "warm": False}),
    (1.2, "spec_warm", {"topic": "protons"}),
    (2.0, "spec_timeout", {"topic": "atomic nucleus"}),
    # The developer "thinks", then asks the follow-up that's already warm.
    (3.0, "dev_query", {"topic": "electrons", "session_id": "demo-2"}),
    (0.4, "warm_hit", {"topic": "electrons", "session_id": "demo-2"}),
    (0.2, "dev_result", {"topic": "electrons", "session_id": "demo-2", "warm": True}),
]


def main():
    reset()
    print("[demo_replay] streaming scripted run — open the panel to watch")
    for delay, etype, data in SCRIPT:
        time.sleep(delay)
        emit_demo_event(etype, data)
    print("[demo_replay] done")


if __name__ == "__main__":
    main()
