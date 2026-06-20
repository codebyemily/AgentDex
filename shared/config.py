import os

ORCHESTRATOR_SEED = "orchestrator_agentdex_v1"
PRIMARY_WORKER_SEED = "primary_worker_agentdex_v1"
DEV_AGENT_SEED = "dev_agent_agentdex_v1"

SPECULATION_BUDGET = int(os.getenv("SPECULATION_BUDGET", "3"))
SPECULATIVE_TIMEOUT_SECS = int(os.getenv("SPECULATIVE_TIMEOUT_SECS", "30"))

# Populated by main.py before the Bureau starts
ORCHESTRATOR_ADDRESS: str = ""
PRIMARY_WORKER_ADDRESS: str = ""
DEV_AGENT_ADDRESS: str = ""
