"""
streamlog.broker.server
=======================
TCP Broker server that stores messages in PartitionLog instances, serves
produce/fetch requests, performs async primary-to-replica replication, and
sends periodic heartbeats to the Coordinator.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from pathlib import Path

from streamlog import config
from streamlog.broker.storage import PartitionLog
from streamlog.common.models import (
    BROKER_HEARTBEAT,
    FETCH,
    PRODUCE,
    REGISTER_BROKER,
    REPLICATE,
    error,
    ok,
)
from streamlog.common.protocol import recv_msg, send_msg


def _make_logger(broker_id: str) -> logging.Logger:
    logger = logging.getLogger(f"streamlog.broker.{broker_id}")
    old_factory = logging.getLogRecordFactory()

    def factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.broker_id = broker_id
        return record

    logging.setLogRecordFactory(factory)
    return logger


class Broker:
    """Storage broker node in a StreamLog cluster."""

    def __init__(
        self,
        broker_id: str,
        host: str,
        port: int,
        coord_host: str = config.COORDINATOR_HOST,
        coord_port: int = config.COORDINATOR_PORT,
        data_dir: str = config.DATA_DIR,
    ) -> None:
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.coord_host = coord_host
        self.coord_port = coord_port
        self.data_root = Path(data_dir) / f"broker_{broker_id}"
        self.log = _make_logger(broker_id)

        # (topic, partition) -> PartitionLog
        self._logs: dict[tuple[str, int], PartitionLog] = {}
        self._logs_lock = threading.Lock()

    def _get_log(self, topic: str, partition: int) -> PartitionLog:
        key = (topic, partition)
        with self._logs_lock:
            if key not in self._logs:
                path = self.data_root / f"{topic}_{partition}.log"
                self._logs[key] = PartitionLog(path)
        return self._logs[key]

    # ── Request dispatch ──────────────────────────────────────────────────────

    def _handle_client(self, conn: socket.socket, _addr: tuple) -> None:
        try:
            while True:
                try:
                    msg = recv_msg(conn)
                except ConnectionError:
                    break
                resp = self._dispatch(msg)
                send_msg(conn, resp)
        finally:
            conn.close()

    def _dispatch(self, msg: dict) -> dict:
        mtype = msg.get("type", "")
        if mtype == PRODUCE:
            return self._h_produce(msg)
        if mtype == FETCH:
            return self._h_fetch(msg)
        if mtype == REPLICATE:
            return self._h_replicate(msg)
        return error(f"Unknown message type: {mtype}")

    # ── Handlers ──────────────────────────────────────────────────────────────

    def _h_produce(self, msg: dict) -> dict:
        topic = msg["topic"]
        partition = int(msg["partition"])
        message = msg["message"]
        key = msg.get("key")
        replica_host = msg.get("replica_host")
        replica_port = msg.get("replica_port")

        plog = self._get_log(topic, partition)
        offset = plog.append(message, key)

        self.log.info(
            "PRODUCE  topic=%s  part=%d  offset=%d", topic, partition, offset
        )

        # Async primary -> replica replication
        if replica_host and replica_port:
            threading.Thread(
                target=self._replicate,
                args=(
                    replica_host,
                    int(replica_port),
                    topic,
                    partition,
                    offset,
                    message,
                    key,
                ),
                daemon=True,
            ).start()

        return ok({"offset": offset})

    def _h_fetch(self, msg: dict) -> dict:
        topic = msg["topic"]
        partition = int(msg["partition"])
        offset = int(msg.get("offset", 0))
        max_recs = int(msg.get("max_records", config.CONSUMER_MAX_POLL_RECORDS))

        plog = self._get_log(topic, partition)
        records = plog.fetch(offset, max_recs)

        self.log.debug(
            "FETCH  topic=%s  part=%d  from=%d  returned=%d",
            topic,
            partition,
            offset,
            len(records),
        )
        return ok({"records": records})

    def _h_replicate(self, msg: dict) -> dict:
        """Accept a replication payload from the primary broker."""
        topic = msg["topic"]
        partition = int(msg["partition"])
        offset = int(msg["offset"])
        message = msg["message"]
        key = msg.get("key")

        plog = self._get_log(topic, partition)
        plog.append_replica(offset, message, key)
        return ok()

    # ── Replication helper ────────────────────────────────────────────────────

    def _replicate(
        self,
        host: str,
        port: int,
        topic: str,
        partition: int,
        offset: int,
        message: str,
        key: str | None,
    ) -> None:
        try:
            with socket.create_connection((host, port), timeout=5) as s:
                send_msg(
                    s,
                    {
                        "type": REPLICATE,
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                        "message": message,
                        "key": key,
                    },
                )
                recv_msg(s)
        except Exception as exc:  # noqa: BLE001
            self.log.warning(
                "Replication failed to %s:%s — %s", host, port, exc
            )

    # ── Registration & heartbeat ──────────────────────────────────────────────

    def _register_with_coordinator(self) -> None:
        while True:
            try:
                with socket.create_connection(
                    (self.coord_host, self.coord_port), timeout=5
                ) as s:
                    send_msg(
                        s,
                        {
                            "type": REGISTER_BROKER,
                            "broker_id": self.broker_id,
                            "host": self.host,
                            "port": self.port,
                        },
                    )
                    resp = recv_msg(s)
                    if resp.get("status") == "ok":
                        self.log.info("Registered with coordinator")
                        return
            except Exception as exc:  # noqa: BLE001
                self.log.warning(
                    "Registration failed (%s) — retrying in 2 s", exc
                )
                time.sleep(2)

    def _heartbeat_loop(self) -> None:
        while True:
            time.sleep(config.BROKER_HEARTBEAT_INTERVAL)
            try:
                with socket.create_connection(
                    (self.coord_host, self.coord_port), timeout=5
                ) as s:
                    send_msg(
                        s,
                        {"type": BROKER_HEARTBEAT, "broker_id": self.broker_id},
                    )
                    recv_msg(s)
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Heartbeat failed: %s", exc)

    # ── Server entry point ────────────────────────────────────────────────────

    def run(self) -> None:
        """Register with coordinator, start heartbeats, and serve TCP clients."""
        self._register_with_coordinator()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(64)
        self.log.info(
            "Broker %s listening on %s:%s", self.broker_id, self.host, self.port
        )

        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=self._handle_client, args=(conn, addr), daemon=True
            ).start()
