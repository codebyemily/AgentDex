"""Single Sentry init point, shared by every agent in the Bureau.

Call init_sentry() once at process startup (main.py). It's idempotent, so the
defensive init in primary_worker.py won't double-configure.
If SENTRY_DSN is unset we stay silent — local dev shouldn't require Sentry.
"""

import os

_inited = False


def init_sentry() -> bool:
    global _inited
    if _inited:
        return True
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        print("[observability] SENTRY_DSN not set — Sentry disabled")
        return False
    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=1.0,
            # Tag every event so primary vs speculative failures are filterable.
            environment=os.getenv("AGENTDEX_ENV", "demo"),
        )
        _inited = True
        print("[observability] Sentry initialized")
        return True
    except Exception as exc:  # pragma: no cover
        print(f"[observability] Sentry init failed: {exc}")
        return False


def capture_pipeline_error(exc: Exception, *, path: str, topic: str) -> None:
    """Report a swallowed pipeline failure to Sentry without crashing the agent.

    The cold path swallows its most likely failures (Browserbase crawl errors,
    Claude classification parse errors) and returns a fallback so the demo keeps
    going. Call this at those swallow points so the error is still visible:

        try:
            ...crawl...
        except Exception as exc:
            capture_pipeline_error(exc, path="crawl", topic=topic)
            return f"[crawl error: {exc}]"

    `path` ("crawl" | "classify" | "mcp_gen" | ...) and `topic` become Sentry
    tags so failures are filterable per stage. Safe to call when Sentry is off.
    """
    try:
        import sentry_sdk

        with sentry_sdk.push_scope() as scope:
            scope.set_tag("pipeline_path", path)
            scope.set_tag("topic", topic)
            sentry_sdk.capture_exception(exc)
    except Exception:  # never let observability break the pipeline
        pass
