"""Locust load test suite for CacheForge.

Workload Scenarios:
  - read_heavy:  95% GET /items/{id}, 5% PUT /items/{id}
  - mixed:       70% GET /items/{id}, 30% PUT /items/{id}
  - write_heavy: 50% GET /items/{id}, 50% PUT /items/{id}

Key distributions:
  - zipfian: simulates hot keys (Pareto principle)
  - uniform: worst-case flat distribution across all items
"""
from __future__ import annotations

import os
import random
from locust import HttpUser, between, task

from scenarios.distributions import UniformGenerator, ZipfianGenerator

WORKLOAD = os.getenv("WORKLOAD", "read_heavy").lower()
DISTRIBUTION = os.getenv("DISTRIBUTION", "zipfian").lower()
DATASET_SIZE = int(os.getenv("DATASET_SIZE", "1000"))

# Initialize generator
if DISTRIBUTION == "uniform":
    key_gen = UniformGenerator(n=DATASET_SIZE)
else:
    key_gen = ZipfianGenerator(n=DATASET_SIZE, s=1.1)


class CacheForgeUser(HttpUser):
    wait_time = between(0.001, 0.005)

    @task
    def execute_workload(self) -> None:
        """Route request according to configured workload ratio."""
        roll = random.random()

        if WORKLOAD == "write_heavy":
            is_read = roll < 0.50
        elif WORKLOAD == "mixed":
            is_read = roll < 0.70
        else:  # read_heavy
            is_read = roll < 0.95

        item_id = key_gen.next_key()

        if is_read:
            self.client.get(f"/items/{item_id}", name="/items/[id]")
        else:
            payload = {
                "name": f"Item-{item_id}-updated",
                "description": "Updated via loadtest",
                "value": {"price": round(random.uniform(5.0, 99.0), 2)},
            }
            self.client.put(f"/items/{item_id}", json=payload, name="/items/[id]")

