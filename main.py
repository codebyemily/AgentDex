from dotenv import load_dotenv
load_dotenv()

import shared.config as config
from agents.orchestrator_agent import orchestrator
from agents.primary_worker import primary_worker
from agents.speculative_worker import speculative_worker
from agents.dev_agent import dev_agent

from uagents import Bureau

# Wire up addresses before the Bureau starts so agents can send to each other
config.ORCHESTRATOR_ADDRESS = orchestrator.address
config.PRIMARY_WORKER_ADDRESS = primary_worker.address
config.SPECULATIVE_WORKER_ADDRESS = speculative_worker.address
config.DEV_AGENT_ADDRESS = dev_agent.address

print("─" * 60)
print(f"  orchestrator       {config.ORCHESTRATOR_ADDRESS}")
print(f"  primary_worker     {config.PRIMARY_WORKER_ADDRESS}")
print(f"  speculative_worker {config.SPECULATIVE_WORKER_ADDRESS}")
print(f"  dev_agent          {config.DEV_AGENT_ADDRESS}")
print("─" * 60)

bureau = Bureau()
bureau.add(orchestrator)
bureau.add(primary_worker)
bureau.add(speculative_worker)
bureau.add(dev_agent)
bureau.run()
