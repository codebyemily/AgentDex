import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import numpy as np
from redisvl.index import SearchIndex
from redisvl.query import VectorQuery
from redisvl.schema import IndexSchema

_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=4)
_index: Optional[SearchIndex] = None
_model = None  # sentence-transformers model, lazy-loaded on first embed call

VECTOR_DIM: int = 384
INDEX_NAME: str = "agentdex_topics"
INDEX_PREFIX: str = "agentdex:topic"
WARM_TOPICS_KEY: str = "agentdex:warm_topics"

_SCHEMA: dict = {
    "index": {
        "name": INDEX_NAME,
        "prefix": INDEX_PREFIX,
        "storage_type": "hash",
    },
    "fields": [
        {"name": "topic", "type": "tag"},
        {"name": "summary", "type": "text"},
        {"name": "content_type", "type": "tag"},
        {"name": "key_facts", "type": "text"},
        {"name": "related_concepts", "type": "text"},
        {"name": "mcp_tools", "type": "text"},
        {"name": "cached_at", "type": "numeric"},
        {
            "name": "embedding",
            "type": "vector",
            "attrs": {
                "dims": VECTOR_DIM,
                "distance_metric": "cosine",
                "algorithm": "flat",
                "datatype": "float32",
            },
        },
    ],
}


def _topic_id(topic: str) -> str:
    return topic.lower().strip().replace(" ", "_")


# ── Embedding ─────────────────────────────────────────────────────────────────

def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def _embed_sync(text: str) -> list[float]:
    t0 = time.time()
    print(f"[redis:embed] '{text}' — encoding with all-MiniLM-L6-v2...")
    vec = _get_model().encode(text, normalize_embeddings=True).tolist()
    print(f"[redis:embed] '{text}' — done  dims={len(vec)}  elapsed={time.time()-t0:.3f}s")
    return vec


async def embed(text: str) -> list[float]:
    """Return a 384-dim L2-normalized vector for text using a local sentence-transformer model."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _embed_sync, text)


# ── Index lifecycle ───────────────────────────────────────────────────────────

def _get_index() -> SearchIndex:
    global _index
    if _index is None:
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        schema = IndexSchema.from_dict(_SCHEMA)

        if redis_url.startswith("rediss://"):
            # redis-py 5.x builds its SSL context internally; on Python 3.13 /
            # OpenSSL 3.x this fails with WRONG_VERSION_NUMBER.  Subclassing
            # SSLConnection and overriding _wrap_socket_with_ssl is the only way
            # to inject a custom SSLContext without patching redis-py internals.
            import ssl as _ssl
            import redis as _redis_lib
            from redis.connection import SSLConnection as _BaseSSL, ConnectionPool as _Pool
            from urllib.parse import urlparse as _urlparse

            _parsed = _urlparse(redis_url)
            _host = _parsed.hostname
            _port = _parsed.port or 6380

            class _LenientSSLConn(_BaseSSL):
                def _wrap_socket_with_ssl(self, sock):
                    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
                    ctx.check_hostname = False
                    ctx.verify_mode = _ssl.CERT_NONE
                    ctx.minimum_version = _ssl.TLSVersion.TLSv1_2
                    return ctx.wrap_socket(sock, server_hostname=_host)

            _pool = _Pool(
                connection_class=_LenientSSLConn,
                host=_host,
                port=_port,
                password=_parsed.password,
                username=_parsed.username or "default",
                db=0,
                socket_timeout=15,
                socket_connect_timeout=10,
            )
            _redis_client = _redis_lib.Redis(connection_pool=_pool)
            idx = SearchIndex(schema, redis_client=_redis_client)
        else:
            idx = SearchIndex(schema, redis_url=redis_url)

        try:
            idx.create(overwrite=False)
        except Exception:
            pass  # index already exists
        _index = idx
    return _index


# ── Write path ────────────────────────────────────────────────────────────────

def _upsert_topic_sync(topic: str, vector: list[float], data: dict) -> None:
    key = f"{INDEX_PREFIX}:{_topic_id(topic)}"
    print(
        f"[redis:upsert] '{topic}' — writing to key={key}  "
        f"key_facts={len(data.get('key_facts', []))}  "
        f"related_concepts={data.get('related_concepts', [])}  "
        f"mcp_tools={len(data.get('mcp_tools', []))}"
    )
    doc = {
        "id": _topic_id(topic),
        "topic": topic.lower().strip(),
        "embedding": np.array(vector, dtype=np.float32).tobytes(),
        "summary": data.get("summary", ""),
        "content_type": data.get("content_type", "prose"),
        "key_facts": json.dumps(data.get("key_facts", [])),
        "related_concepts": json.dumps(data.get("related_concepts", [])),
        "mcp_tools": json.dumps(data.get("mcp_tools", [])),
        "cached_at": data.get("cached_at", time.time()),
    }
    idx = _get_index()
    idx.load([doc], id_field="id")
    idx.client.sadd(WARM_TOPICS_KEY, topic.lower().strip())
    print(f"[redis:upsert] '{topic}' — done, registered in warm set '{WARM_TOPICS_KEY}'")


async def upsert_topic(topic: str, vector: list[float], data: dict) -> None:
    """Store a topic's structured data and embedding vector in the Redis index."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, _upsert_topic_sync, topic, vector, data)


async def set_warm(topic: str, data: dict) -> None:
    """Embed topic, persist result in vector index, and register it as warm."""
    print(f"[redis:set_warm] '{topic}' — embedding + upserting...")
    payload = {**data, "cached_at": time.time()}
    vector = await embed(topic)
    await upsert_topic(topic, vector, payload)
    print(f"[redis:set_warm] '{topic}' — now warm")


# ── Read path ─────────────────────────────────────────────────────────────────

def _get_warm_sync(topic: str) -> Optional[dict]:
    key = f"{INDEX_PREFIX}:{_topic_id(topic)}"
    print(f"[redis:get_warm] '{topic}' — looking up key={key}")
    raw: dict = _get_index().client.hgetall(key)
    if not raw:
        print(f"[redis:get_warm] '{topic}' — MISS")
        return None

    def _d(v: object) -> str:
        return v.decode() if isinstance(v, bytes) else str(v)

    decoded = {
        _d(k): _d(v)
        for k, v in raw.items()
        if k not in (b"embedding", "embedding")
    }
    result = {
        "summary": decoded.get("summary", ""),
        "content_type": decoded.get("content_type", "prose"),
        "key_facts": json.loads(decoded.get("key_facts", "[]")),
        "related_concepts": json.loads(decoded.get("related_concepts", "[]")),
        "mcp_tools": json.loads(decoded.get("mcp_tools", "[]")),
        "cached_at": float(decoded.get("cached_at", 0.0)),
    }
    print(
        f"[redis:get_warm] '{topic}' — HIT  "
        f"content_type={result['content_type']}  "
        f"key_facts={len(result['key_facts'])}  "
        f"cached_at={result['cached_at']:.0f}"
    )
    return result


async def get_warm(topic: str) -> Optional[dict]:
    """Return the cached result for topic, or None if it has not been ingested yet."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _get_warm_sync, topic)


# ── Nearest-neighbor search ───────────────────────────────────────────────────

def _search_nearest_sync(vector: list[float], k: int) -> list[tuple[str, float]]:
    print(f"[redis:knn] querying index '{INDEX_NAME}' for top-{k} nearest neighbors...")
    query = VectorQuery(
        vector=vector,
        vector_field_name="embedding",
        return_fields=["topic"],
        num_results=k,
        return_score=True,
    )
    try:
        results = _get_index().query(query)
    except Exception as exc:
        print(f"[redis:knn] query failed: {exc}")
        return []
    out: list[tuple[str, float]] = []
    for r in results:
        t = r.get("topic", "")
        if isinstance(t, bytes):
            t = t.decode()
        dist = float(r.get("vector_distance", 1.0))
        if t:
            out.append((t, dist))
    print(f"[redis:knn] results ({len(out)}): {[(t, round(d, 4)) for t, d in out]}")
    return out


async def search_nearest_with_scores(query_topic: str, k: int = 10) -> list[tuple[str, float]]:
    """Embed query_topic and return (topic, cosine_distance) pairs, closest first.

    Distance is in [0, 2] for cosine metric; values below ~0.15 indicate a strong match.
    """
    print(f"[redis:knn] '{query_topic}' — embedding query for KNN search (k={k})...")
    vector = await embed(query_topic)
    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(_executor, _search_nearest_sync, vector, k)
    normalized = query_topic.lower().strip()
    filtered = [(t, d) for t, d in results if t.lower().strip() != normalized]
    print(f"[redis:knn] '{query_topic}' — {len(filtered)} results after excluding self")
    return filtered


async def search_nearest(query_topic: str, k: int = 10) -> list[str]:
    """Embed query_topic and return the k most similar ingested topics, closest first."""
    scored = await search_nearest_with_scores(query_topic, k)
    return [t for t, _ in scored]


# ── Topic registry ────────────────────────────────────────────────────────────

def _all_topics_sync() -> list[str]:
    members = _get_index().client.smembers(WARM_TOPICS_KEY)
    topics = [m.decode() if isinstance(m, bytes) else m for m in members]
    print(f"[redis:all_topics] {len(topics)} warm topics: {sorted(topics)}")
    return topics


async def all_topics() -> list[str]:
    """Return all ingested topic strings (lowercased) from the warm registry SET."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _all_topics_sync)
