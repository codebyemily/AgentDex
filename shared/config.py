import os

ORCHESTRATOR_SEED = "orchestrator_agentdex_v1"
PRIMARY_WORKER_SEED = "primary_worker_agentdex_v1"
SPECULATIVE_WORKER_SEED = "speculative_worker_agentdex_v1"
DEV_AGENT_SEED = "dev_agent_agentdex_v1"

SPECULATION_BUDGET = int(os.getenv("SPECULATION_BUDGET", "3"))
SPECULATIVE_TIMEOUT_SECS = int(os.getenv("SPECULATIVE_TIMEOUT_SECS", "30"))
# Cosine distance threshold for a semantic cache hit (0 = identical, 2 = opposite).
# Topics with distance below this are considered close enough to serve from cache.
SEMANTIC_HIT_THRESHOLD: float = float(os.getenv("SEMANTIC_HIT_THRESHOLD", "0.15"))

# Populated by main.py before the Bureau starts
ORCHESTRATOR_ADDRESS: str = ""
PRIMARY_WORKER_ADDRESS: str = ""
SPECULATIVE_WORKER_ADDRESS: str = ""
DEV_AGENT_ADDRESS: str = ""
