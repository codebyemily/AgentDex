"""Demo event bus — the panel's data source.

Every meaningful step in the speculative pipeline emits one event here. The
agent process (the uAgents Bureau) writes; the FastAPI panel reads. They talk
over a Redis stream so they can be separate processes.

Design goals for a live demo:
  - Never crash the agents because Redis is down. If Redis is unavailable we
    degrade to a no-op (events still print to stdout) instead of taking the
    whole Bureau with us.
  - One JSON blob per stream entry, so the reader doesn't have to reassemble
    typed fields.
"""

import json
import os
import time

DEMO_EVENT_STREAM = "agentdex:demo_events"

# ── Redis (optional) ─────────────────────────────────────────────────────────
_redis = None
_redis_tried = False


def _client():
    """Lazily connect. Returns None (and stays None) if Redis is unreachable."""
    global _redis, _redis_tried
    if _redis_tried:
        return _redis
    _redis_tried = True
    url = os.getenv("REDIS_URL")
    if not url:
        print("[demo_events] REDIS_URL not set — panel events disabled")
        return None
    try:
        import redis

        client = redis.from_url(url, decode_responses=True)
        client.ping()
        _redis = client
        print(f"[demo_events] connected to Redis stream '{DEMO_EVENT_STREAM}'")
    except Exception as exc:  # pragma: no cover - infra failure path
        print(f"[demo_events] Redis unavailable ({exc}) — panel events disabled")
        _redis = None
    return _redis


# ── Sentry breadcrumb (optional) ─────────────────────────────────────────────
try:
    import sentry_sdk
except Exception:  # pragma: no cover
    sentry_sdk = None


def emit_demo_event(event_type: str, data: dict | None = None) -> dict:
    """Publish one event. Safe to call even if Redis/Sentry are down."""
    payload = {"type": event_type, "timestamp": time.time(), **(data or {})}

    # Always visible in the agent logs, Redis or not.
    print(f"[demo_event] {event_type}  {data or {}}")

    client = _client()
    if client is not None:
        try:
            client.xadd(
                DEMO_EVENT_STREAM,
                {"json": json.dumps(payload)},
                maxlen=500,
                approximate=True,
            )
        except Exception as exc:  # pragma: no cover
            print(f"[demo_events] xadd failed: {exc}")

    if sentry_sdk is not None:
        sentry_sdk.add_breadcrumb(
            category="agentdex.demo",
            message=event_type,
            data=payload,
            level="info",
        )

    return payload


def read_events(last_id: str = "0", block_ms: int = 15_000):
    """Blocking read for the panel. Yields (entry_id, payload) tuples.

    Returns an empty list if Redis isn't available so the caller can back off.
    """
    client = _client()
    if client is None:
        return []
    import redis  # already imported+cached by _client(); just bring it into scope

    try:
        resp = client.xread({DEMO_EVENT_STREAM: last_id}, block=block_ms, count=100)
    except redis.exceptions.TimeoutError:
        # XREAD BLOCK with no new events: redis-py ties the socket read timeout
        # to the BLOCK value, so an idle window surfaces as a socket timeout.
        # That's the normal "nothing new" case — back off and let the caller poll again.
        return []
    out = []
    for _stream, entries in resp or []:
        for entry_id, fields in entries:
            try:
                out.append((entry_id, json.loads(fields["json"])))
            except Exception:
                continue
    return out


def history(count: int = 500):
    """All events currently in the stream, oldest first (for panel cold-start)."""
    client = _client()
    if client is None:
        return []
    out = []
    for entry_id, fields in client.xrange(DEMO_EVENT_STREAM, count=count):
        try:
            out.append((entry_id, json.loads(fields["json"])))
        except Exception:
            continue
    return out


def reset():
    """Clear the stream — call before a fresh demo run."""
    client = _client()
    if client is not None:
        try:
            client.delete(DEMO_EVENT_STREAM)
            print("[demo_events] stream cleared")
        except Exception as exc:  # pragma: no cover
            print(f"[demo_events] reset failed: {exc}")
