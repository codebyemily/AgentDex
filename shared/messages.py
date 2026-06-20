from uagents import Model


class TopicQuery(Model):
    topic: str
    session_id: str


class ResearchRequest(Model):
    topic: str
    session_id: str
    is_speculative: bool


class ResearchResult(Model):
    topic: str
    session_id: str
    summary: str
    content_type: str      # "tabular" or "prose"
    key_facts: str         # JSON-encoded list[str]
    related_concepts: str  # JSON-encoded list[str]
    mcp_tools: str         # JSON-encoded list[dict]
    warm: bool
    timestamp: float
