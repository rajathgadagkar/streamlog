"""
streamlog.client.producer
=========================
Thread-safe Producer API with coordinator-driven partition routing,
key-based hashing, round-robin distribution, and exponential backoff retries.
"""

from __future__ import annotations

import logging
import socket
import time

from streamlog import config
from streamlog.common.models import (
    CREATE_TOPIC,
    GET_PARTITION_INFO,
    PRODUCE,
    STATUS_OK,
)
from streamlog.common.protocol import recv_msg, send_msg

log = logging.getLogger("streamlog.producer")


class Producer:
    """
    Thread-safe producer client for a StreamLog cluster.

    :param coord_host: Coordinator hostname
    :param coord_port: Coordinator port
    """

    def __init__(
        self,
        coord_host: str = config.COORDINATOR_HOST,
        coord_port: int = config.COORDINATOR_PORT,
    ) -> None:
        self._coord_host = coord_host
        self._coord_port = coord_port

    # ── Public API ────────────────────────────────────────────────────────────

    def create_topic(self, topic: str, num_partitions: int = 3) -> None:
        """
        Ask the coordinator to create *topic*.
        Idempotent — safe to call even if the topic already exists.
        """
        resp = self._coord_request(
            {
                "type": CREATE_TOPIC,
                "topic": topic,
                "num_partitions": num_partitions,
            }
        )
        if resp.get("status") != STATUS_OK:
            raise RuntimeError(f"create_topic failed: {resp.get('reason')}")
        log.info("Topic ready: %s (%d partitions)", topic, num_partitions)

    def send(self, topic: str, message: str, key: str | None = None) -> int:
        """
        Publish *message* to *topic*.

        :param topic:   Target topic name
        :param message: Message payload
        :param key:     Optional routing key — identical keys always map to the same partition
        :returns:       Committed offset on the primary broker
        :raises RuntimeError: After all retries are exhausted
        """
        last_exc: Exception | None = None
        delay = config.PRODUCER_RETRY_BASE_S

        for attempt in range(1, config.PRODUCER_MAX_RETRIES + 1):
            try:
                return self._do_send(topic, message, key)
            except (ConnectionError, OSError, RuntimeError) as exc:
                last_exc = exc
                log.warning(
                    "send attempt %d/%d failed: %s — retrying in %.2f s",
                    attempt,
                    config.PRODUCER_MAX_RETRIES,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay *= 2

        raise RuntimeError(
            f"Failed to send message after {config.PRODUCER_MAX_RETRIES} retries"
        ) from last_exc

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _do_send(self, topic: str, message: str, key: str | None) -> int:
        routing = self._coord_request(
            {
                "type": GET_PARTITION_INFO,
                "topic": topic,
                "key": key,
            }
        )
        if routing.get("status") != STATUS_OK:
            raise RuntimeError(f"Routing failed: {routing.get('reason')}")

        partition = routing["partition"]
        broker_host = routing["host"]
        broker_port = routing["port"]
        replica_host = routing.get("replica_host")
        replica_port = routing.get("replica_port")

        msg = {
            "type": PRODUCE,
            "topic": topic,
            "partition": partition,
            "message": message,
            "key": key,
            "replica_host": replica_host,
            "replica_port": replica_port,
        }
        resp = self._broker_request(broker_host, broker_port, msg)
        if resp.get("status") != STATUS_OK:
            raise RuntimeError(f"Produce failed: {resp.get('reason')}")

        offset = resp["offset"]
        log.debug("Sent  topic=%s  part=%d  offset=%d", topic, partition, offset)
        return offset

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
