"""
streamlog.client.consumer
=========================
Thread-safe Consumer API with consumer-group partition sharing, fan-out
subscriptions, offset commits, and background liveness heartbeats.
"""

from __future__ import annotations

import logging
import socket
import threading
import time

from streamlog import config
from streamlog.common.models import (
    COMMIT_OFFSET,
    CONSUMER_HEARTBEAT,
    FETCH,
    GET_ASSIGNMENT,
    GET_OFFSET,
    STATUS_OK,
    SUBSCRIBE,
    UNSUBSCRIBE,
)
from streamlog.common.protocol import recv_msg, send_msg

log = logging.getLogger("streamlog.consumer")


class Consumer:
    """
    Thread-safe consumer client with background heartbeat management.

    :param coord_host:  Coordinator hostname
    :param coord_port:  Coordinator port
    :param consumer_id: Unique ID for this consumer instance
    """

    def __init__(
        self,
        coord_host: str = config.COORDINATOR_HOST,
        coord_port: int = config.COORDINATOR_PORT,
        consumer_id: str = "consumer-1",
    ) -> None:
        self._coord_host = coord_host
        self._coord_port = coord_port
        self.consumer_id = consumer_id

        self._group_id: str | None = None
        self._topic: str | None = None

        # Current partition assignment: [{partition, host, port, broker_id}, ...]
        self._assignment: list[dict] = []
        self._assignment_lock = threading.Lock()

        # Local offset tracking (uncommitted): {partition -> next_offset_to_fetch}
        self._fetch_offsets: dict[int, int] = {}

        self._running = False
        self._hb_thread: threading.Thread | None = None

    # ── Context manager support ───────────────────────────────────────────────

    def __enter__(self) -> Consumer:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ── Public API ────────────────────────────────────────────────────────────

    def subscribe(self, topic: str, group_id: str) -> None:
        """
        Subscribe to *topic* as a member of *group_id*.
        Triggers a rebalance on the coordinator side.
        """
        self._group_id = group_id
        self._topic = topic

        resp = self._coord_request(
            {
                "type": SUBSCRIBE,
                "topic": topic,
                "group_id": group_id,
                "consumer_id": self.consumer_id,
            }
        )
        if resp.get("status") != STATUS_OK:
            raise RuntimeError(f"subscribe failed: {resp.get('reason')}")

        self._refresh_assignment()
        self._start_heartbeat()
        log.info(
            "Subscribed  consumer=%s  group=%s  topic=%s",
            self.consumer_id,
            group_id,
            topic,
        )

    def poll(self, timeout_ms: int = 1000) -> list[dict]:
        """
        Fetch available messages from all assigned partitions.

        Returns a flat list of record dicts:
          ``{offset, key, message, ts, partition, topic}``
        """
        if self._group_id is None:
            raise RuntimeError("Call subscribe() before poll()")

        self._refresh_assignment()

        results: list[dict] = []
        with self._assignment_lock:
            assignment = list(self._assignment)

        for part_info in assignment:
            partition = part_info["partition"]
            broker_host = part_info["host"]
            broker_port = part_info["port"]

            if broker_host is None or broker_port is None:
                continue

            start_offset = self._fetch_offsets.get(
                partition, self._get_committed_offset(partition)
            )
            self._fetch_offsets.setdefault(partition, start_offset)

            try:
                resp = self._broker_request(
                    broker_host,
                    int(broker_port),
                    {
                        "type": FETCH,
                        "topic": self._topic,
                        "partition": partition,
                        "offset": self._fetch_offsets[partition],
                        "max_records": config.CONSUMER_MAX_POLL_RECORDS,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("Fetch failed  part=%d: %s", partition, exc)
                continue

            if resp.get("status") != STATUS_OK:
                continue

            for record in resp.get("records", []):
                record["partition"] = partition
                record["topic"] = self._topic
                results.append(record)
                self._fetch_offsets[partition] = record["offset"] + 1

        return results

    def commit(self, partition: int, offset: int) -> None:
        """
        Commit *offset* for *partition* to the coordinator.
        The next poll on this partition will resume from *offset*.
        """
        if self._group_id is None:
            raise RuntimeError("Call subscribe() before commit()")
        resp = self._coord_request(
            {
                "type": COMMIT_OFFSET,
                "group_id": self._group_id,
                "consumer_id": self.consumer_id,
                "partition": partition,
                "offset": offset,
            }
        )
        if resp.get("status") != STATUS_OK:
            log.warning(
                "Commit failed  part=%d offset=%d: %s",
                partition,
                offset,
                resp.get("reason"),
            )
        else:
            self._fetch_offsets[partition] = offset

    def close(self) -> None:
        """Gracefully unsubscribe from the group and stop heartbeats."""
        self._running = False
        if self._group_id:
            try:
                self._coord_request(
                    {
                        "type": UNSUBSCRIBE,
                        "group_id": self._group_id,
                        "consumer_id": self.consumer_id,
                    }
                )
            except Exception:  # noqa: BLE001
                pass

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _refresh_assignment(self) -> None:
        try:
            resp = self._coord_request(
                {
                    "type": GET_ASSIGNMENT,
                    "group_id": self._group_id,
                    "consumer_id": self.consumer_id,
                }
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to refresh assignment: %s", exc)
            return

        if resp.get("status") != STATUS_OK:
            return

        partitions = resp.get("partitions", [])
        with self._assignment_lock:
            self._assignment = partitions

    def _get_committed_offset(self, partition: int) -> int:
        try:
            resp = self._coord_request(
                {
                    "type": GET_OFFSET,
                    "group_id": self._group_id,
                    "partition": partition,
                }
            )
            if resp.get("status") == STATUS_OK:
                return int(resp.get("offset", 0))
        except Exception:  # noqa: BLE001
            pass
        return 0

    def _start_heartbeat(self) -> None:
        if self._running:
            return
        self._running = True
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True
        )
        self._hb_thread.start()

    def _heartbeat_loop(self) -> None:
        while self._running:
            time.sleep(config.CONSUMER_HEARTBEAT_INTERVAL)
            try:
                self._coord_request(
                    {
                        "type": CONSUMER_HEARTBEAT,
                        "group_id": self._group_id,
                        "consumer_id": self.consumer_id,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("Consumer heartbeat failed: %s", exc)

    def _coord_request(self, msg: dict) -> dict:
        with socket.create_connection(
            (self._coord_host, self._coord_port), timeout=10
        ) as s:
            send_msg(s, msg)
            return recv_msg(s)

    def _broker_request(self, host: str, port: int, msg: dict) -> dict:
        with socket.create_connection((host, port), timeout=10) as s:
            send_msg(s, msg)
            return recv_msg(s)
