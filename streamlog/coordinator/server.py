"""
streamlog.coordinator.server
============================
TCP Coordinator server handling broker registration, topic metadata,
consumer group coordination, offset commits, and heartbeat failure detection.
"""

from __future__ import annotations

import logging
import socket
import threading
import time

from streamlog import config
from streamlog.common.models import (
    BROKER_HEARTBEAT,
    COMMIT_OFFSET,
    CONSUMER_HEARTBEAT,
    CREATE_TOPIC,
    GET_ASSIGNMENT,
    GET_OFFSET,
    GET_PARTITION_INFO,
    LIST_TOPICS,
    REGISTER_BROKER,
    SUBSCRIBE,
    UNSUBSCRIBE,
    error,
    ok,
)
from streamlog.common.protocol import recv_msg, send_msg
from streamlog.coordinator.state import CoordinatorState

log = logging.getLogger("streamlog.coordinator")


class Coordinator:
    """Central metadata and coordination server for a StreamLog cluster."""

    def __init__(
        self,
        host: str = config.COORDINATOR_HOST,
        port: int = config.COORDINATOR_PORT,
    ) -> None:
        self.host = host
        self.port = port
        self.state = CoordinatorState()

    # ── Client connection loop ────────────────────────────────────────────────

    def _handle_client(self, conn: socket.socket, addr: tuple) -> None:
        log.debug("New connection from %s", addr)
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
        handlers = {
            REGISTER_BROKER: self._h_register_broker,
            CREATE_TOPIC: self._h_create_topic,
            GET_PARTITION_INFO: self._h_get_partition_info,
            LIST_TOPICS: self._h_list_topics,
            SUBSCRIBE: self._h_subscribe,
            UNSUBSCRIBE: self._h_unsubscribe,
            GET_ASSIGNMENT: self._h_get_assignment,
            GET_OFFSET: self._h_get_offset,
            COMMIT_OFFSET: self._h_commit_offset,
            BROKER_HEARTBEAT: self._h_broker_hb,
            CONSUMER_HEARTBEAT: self._h_consumer_hb,
        }
        handler = handlers.get(mtype)
        if handler is None:
            return error(f"Unknown message type: {mtype}")
        try:
            return handler(msg)
        except Exception as exc:  # noqa: BLE001
            log.exception("Error handling %s: %s", mtype, exc)
            return error(str(exc))

    # ── Request handlers ──────────────────────────────────────────────────────

    def _h_register_broker(self, msg: dict) -> dict:
        bid = msg["broker_id"]
        host = msg["host"]
        port = msg["port"]
        with self.state.lock:
            self.state.brokers[bid] = {
                "host": host,
                "port": port,
                "last_hb": time.time(),
            }
        log.info("Broker registered  id=%s  %s:%s", bid, host, port)
        return ok()

    def _h_create_topic(self, msg: dict) -> dict:
        topic = msg["topic"]
        num_parts = int(msg.get("num_partitions", 1))

        with self.state.lock:
            if topic in self.state.topics:
                return ok({"message": "Topic already exists"})

            live = self.state.live_brokers()
            if not live:
                return error("No live brokers available")

            partitions: dict[int, dict] = {}
            for pid in range(num_parts):
                primary = live[pid % len(live)]
                replica = live[(pid + 1) % len(live)] if len(live) > 1 else primary
                partitions[pid] = {"primary": primary, "replica": replica}

            self.state.topics[topic] = {
                "num_partitions": num_parts,
                "partitions": partitions,
            }

        log.info("Topic created  topic=%s  partitions=%d", topic, num_parts)
        return ok()

    def _h_get_partition_info(self, msg: dict) -> dict:
        topic = msg["topic"]
        key = msg.get("key")  # None -> round-robin
        partition = msg.get("partition")  # explicit override

        with self.state.lock:
            if topic not in self.state.topics:
                return error(f"Unknown topic: {topic}")
            tinfo = self.state.topics[topic]
            num_parts = tinfo["num_partitions"]

            if partition is None:
                if key is not None:
                    partition = hash(key) % num_parts
                else:
                    partition = self.state.next_partition_rr(topic)

            pinfo = tinfo["partitions"][partition]
            primary = pinfo["primary"]
            replica_id = pinfo.get("replica")

            binfo = self.state.brokers.get(primary)
            if binfo is None:
                return error(f"Primary broker info not found for {primary}")

            replica_host = None
            replica_port = None
            if replica_id and replica_id != primary:
                rinfo = self.state.brokers.get(replica_id)
                if rinfo is not None:
                    replica_host = rinfo.get("host")
                    replica_port = rinfo.get("port")

            return ok(
                {
                    "partition": partition,
                    "broker_id": primary,
                    "host": binfo["host"],
                    "port": binfo["port"],
                    "replica_id": replica_id,
                    "replica_host": replica_host,
                    "replica_port": replica_port,
                }
            )

    def _h_list_topics(self, _msg: dict) -> dict:
        with self.state.lock:
            topics_info = {
                t: {
                    "num_partitions": v["num_partitions"],
                    "partitions": {
                        str(pid): pa for pid, pa in v["partitions"].items()
                    },
                }
                for t, v in self.state.topics.items()
            }
        return ok({"topics": topics_info})

    def _h_subscribe(self, msg: dict) -> dict:
        group_id = msg["group_id"]
        topic = msg["topic"]
        consumer_id = msg["consumer_id"]

        with self.state.lock:
            if topic not in self.state.topics:
                return error(f"Unknown topic: {topic}")

            if group_id not in self.state.groups:
                self.state.groups[group_id] = {
                    "topic": topic,
                    "members": {},
                    "assignment": {},
                    "offsets": {},
                }

            grp = self.state.groups[group_id]
            if grp["topic"] != topic:
                return error(
                    f"Group {group_id} already subscribed to {grp['topic']}"
                )

            grp["members"][consumer_id] = time.time()
            self.state.rebalance(group_id)

        log.info(
            "Consumer subscribed  group=%s  consumer=%s  topic=%s",
            group_id,
            consumer_id,
            topic,
        )
        return ok()

    def _h_unsubscribe(self, msg: dict) -> dict:
        group_id = msg["group_id"]
        consumer_id = msg["consumer_id"]

        with self.state.lock:
            grp = self.state.groups.get(group_id)
            if grp and consumer_id in grp["members"]:
                del grp["members"][consumer_id]
                self.state.rebalance(group_id)

        return ok()

    def _h_get_assignment(self, msg: dict) -> dict:
        group_id = msg["group_id"]
        consumer_id = msg["consumer_id"]

        with self.state.lock:
            grp = self.state.groups.get(group_id)
            if grp is None:
                return error(f"Unknown group: {group_id}")
            partitions = grp["assignment"].get(consumer_id, [])
            topic = grp["topic"]
            part_info = []
            for pid in partitions:
                tinfo = self.state.topics.get(topic, {})
                pinfo = tinfo.get("partitions", {}).get(pid, {})
                primary = pinfo.get("primary")
                binfo = self.state.brokers.get(primary, {})
                part_info.append(
                    {
                        "partition": pid,
                        "broker_id": primary,
                        "host": binfo.get("host"),
                        "port": binfo.get("port"),
                    }
                )

        return ok({"topic": topic, "partitions": part_info})

    def _h_get_offset(self, msg: dict) -> dict:
        group_id = msg["group_id"]
        partition = int(msg["partition"])

        with self.state.lock:
            grp = self.state.groups.get(group_id)
            if grp is None:
                return ok({"offset": 0})
            offset = grp["offsets"].get(partition, 0)

        return ok({"offset": offset})

    def _h_commit_offset(self, msg: dict) -> dict:
        group_id = msg["group_id"]
        partition = int(msg["partition"])
        offset = int(msg["offset"])

        with self.state.lock:
            grp = self.state.groups.get(group_id)
            if grp is None:
                return error(f"Unknown group: {group_id}")
            grp["offsets"][partition] = offset

        return ok()

    def _h_broker_hb(self, msg: dict) -> dict:
        bid = msg["broker_id"]
        with self.state.lock:
            if bid in self.state.brokers:
                self.state.brokers[bid]["last_hb"] = time.time()
        return ok()

    def _h_consumer_hb(self, msg: dict) -> dict:
        group_id = msg["group_id"]
        consumer_id = msg["consumer_id"]
        with self.state.lock:
            grp = self.state.groups.get(group_id)
            if grp and consumer_id in grp["members"]:
                grp["members"][consumer_id] = time.time()
        return ok()

    # ── Heartbeat monitor ─────────────────────────────────────────────────────

    def _heartbeat_monitor(self) -> None:
        """Background thread that evicts dead brokers and consumers."""
        interval = min(
            config.BROKER_HEARTBEAT_INTERVAL, config.CONSUMER_HEARTBEAT_INTERVAL
        )
        while True:
            time.sleep(interval)
            now = time.time()
            self._check_brokers(now)
            self._check_consumers(now)

    def _check_brokers(self, now: float) -> None:
        with self.state.lock:
            dead = [
                bid
                for bid, info in self.state.brokers.items()
                if now - info["last_hb"] >= config.BROKER_TIMEOUT
            ]
            for dead_id in dead:
                log.warning("Broker DEAD  id=%s — promoting replicas", dead_id)
                del self.state.brokers[dead_id]
                self.state.promote_replicas(dead_id)

    def _check_consumers(self, now: float) -> None:
        with self.state.lock:
            for group_id, grp in self.state.groups.items():
                dead = [
                    cid
                    for cid, last_hb in grp["members"].items()
                    if now - last_hb >= config.CONSUMER_TIMEOUT
                ]
                for cid in dead:
                    log.warning(
                        "Consumer EVICTED  group=%s  consumer=%s", group_id, cid
                    )
                    del grp["members"][cid]
                if dead:
                    self.state.rebalance(group_id)

    # ── Server entry point ────────────────────────────────────────────────────

    def run(self) -> None:
        """Start heartbeat monitor and listen for incoming TCP connections."""
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(64)
        log.info("Coordinator listening on %s:%s", self.host, self.port)

        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=self._handle_client, args=(conn, addr), daemon=True
            ).start()
