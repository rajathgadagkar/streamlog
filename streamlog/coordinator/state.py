"""
streamlog.coordinator.state
===========================
Thread-safe cluster state managed by the Coordinator:
  - Registered broker table & liveness tracking
  - Topic -> partition -> (primary, replica) metadata
  - Round-robin partition routing counters
  - Consumer group membership, partition assignments, and committed offsets
"""

from __future__ import annotations

import logging
import threading
import time

from streamlog import config

log = logging.getLogger("streamlog.coordinator")


class CoordinatorState:
    """In-memory metadata store protected by a single mutex."""

    def __init__(self) -> None:
        self.lock = threading.Lock()

        # brokers: {broker_id -> {host, port, last_hb}}
        self.brokers: dict[str, dict] = {}

        # topics: {topic -> {num_partitions, partitions: {pid -> {primary, replica}}}}
        self.topics: dict[str, dict] = {}

        # producer routing counters for round-robin: {topic -> int}
        self._rr_counters: dict[str, int] = {}

        # consumer groups:
        # {group_id -> {topic, members: {cid -> last_hb}, assignment: {cid -> [pids]},
        #               offsets: {pid -> offset}}}
        self.groups: dict[str, dict] = {}

    # ── Broker helpers ────────────────────────────────────────────────────────

    def live_brokers(self) -> list[str]:
        """Return IDs of brokers whose heartbeat has not timed out."""
        now = time.time()
        return [
            bid
            for bid, info in self.brokers.items()
            if now - info["last_hb"] < config.BROKER_TIMEOUT
        ]

    def broker_addr(self, broker_id: str) -> tuple[str, int] | None:
        """Return ``(host, port)`` for *broker_id*, or ``None`` if unknown."""
        info = self.brokers.get(broker_id)
        if info is None:
            return None
        return info["host"], info["port"]

    # ── Topic helpers ─────────────────────────────────────────────────────────

    def next_partition_rr(self, topic: str) -> int:
        """Return the next partition index via round-robin."""
        n = self.topics[topic]["num_partitions"]
        idx = self._rr_counters.get(topic, 0) % n
        self._rr_counters[topic] = idx + 1
        return idx

    # ── Consumer group helpers ────────────────────────────────────────────────

    def rebalance(self, group_id: str) -> None:
        """
        Assign partitions to active consumers in *group_id*.
        Caller MUST hold ``self.lock``.
        """
        grp = self.groups[group_id]
        topic = grp["topic"]
        if topic not in self.topics:
            return
        num_parts = self.topics[topic]["num_partitions"]
        members = sorted(grp["members"].keys())
        assignment: dict[str, list[int]] = {m: [] for m in members}

        if members:
            for pid in range(num_parts):
                owner = members[pid % len(members)]
                assignment[owner].append(pid)

        grp["assignment"] = assignment
        log.info("Rebalance group=%s  assignment=%s", group_id, assignment)

    def promote_replicas(self, dead_broker_id: str) -> None:
        """
        Promote replicas to primaries for all partitions owned by *dead_broker_id*.
        Caller MUST hold ``self.lock``.
        """
        for topic, tinfo in self.topics.items():
            for pid, pinfo in tinfo["partitions"].items():
                if pinfo["primary"] == dead_broker_id:
                    new_primary = pinfo["replica"]
                    pinfo["primary"] = new_primary
                    log.info(
                        "Promoted replica -> primary  topic=%s  part=%s  broker=%s",
                        topic,
                        pid,
                        new_primary,
                    )
