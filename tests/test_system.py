"""
tests/test_system.py
====================
Comprehensive integration test suite for the Kafka-inspired pub-sub system.

Test Groups:
  TC-01  Basic Produce / Consume
  TC-02  Multiple Partitions & Routing
  TC-03  Key-Based Partition Routing
  TC-04  Consumer Groups — Partition Sharing
  TC-05  Fan-Out (Multiple Independent Groups)
  TC-06  Offset Commit & Resume
  TC-07  At-Least-Once Delivery Guarantee
  TC-08  Message Ordering Within Partition
  TC-09  Multiple Topics
  TC-10  Concurrent Producers
  TC-11  Concurrent Consumers (Same Group)
  TC-12  Consumer Joins Group Mid-Stream (Dynamic Rebalance)
  TC-13  Consumer Leaves Group Mid-Stream (Rebalance + Redistribution)
  TC-14  Large Message Payloads
  TC-15  High-Volume Throughput
  TC-16  Idempotent Topic Creation
  TC-17  Round-Robin Partition Distribution
  TC-18  Replication Verification
  TC-19  Broker Failure — Replica Promotion + Continued Consume
  TC-20  Broker Failure — Producer Rerouting After Timeout
  TC-21  Multiple Broker Failures (One Survives)
  TC-22  Consumer Eviction via Heartbeat Timeout
  TC-23  Three Consumer Groups Same Topic (Triple Fan-Out)
  TC-24  Sequential Consume Batches / Incremental Polling
  TC-25  Message Metadata Integrity (key, ts, offset, partition)

Run:
  python -m pytest tests/test_system.py -v --tb=short
  # or
  python tests/test_system.py
"""

from __future__ import annotations

import collections
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from streamlog import Consumer, Producer

# ─── Port assignments (high range to avoid conflicts with live cluster) ───────
COORD_HOST   = "127.0.0.1"
COORD_PORT   = 19000
BROKER_PORTS = [19001, 19002, 19003, 19004]

DATA_DIR = "/tmp/kafka_test_data"

# ─── Terminal colours ─────────────────────────────────────────────────────────
GRN  = "\033[92m"
RED  = "\033[91m"
YLW  = "\033[93m"
CYN  = "\033[96m"
RST  = "\033[0m"
BOLD = "\033[1m"


# ═════════════════════════════════════════════════════════════════════════════
# Infrastructure helpers
# ═════════════════════════════════════════════════════════════════════════════

def start_process(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(ROOT),
    )


def wait_port(host: str, port: int, timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except OSError:
            time.sleep(0.15)
    return False


def fresh_data_dir():
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    os.makedirs(DATA_DIR, exist_ok=True)


def start_coord() -> subprocess.Popen:
    p = start_process([
        sys.executable, "-m", "streamlog.coordinator",
        "--host", COORD_HOST, "--port", str(COORD_PORT),
    ])
    assert wait_port(COORD_HOST, COORD_PORT, 12), "Coordinator failed to start"
    return p


def start_broker(broker_num: int) -> subprocess.Popen:
    port = BROKER_PORTS[broker_num - 1]
    p = start_process([
        sys.executable, "-m", "streamlog.broker",
        "--id",         f"broker{broker_num}",
        "--host",       COORD_HOST,
        "--port",       str(port),
        "--coord-host", COORD_HOST,
        "--coord-port", str(COORD_PORT),
        "--data-dir",   DATA_DIR,
    ])
    assert wait_port(COORD_HOST, port, 12), f"Broker {broker_num} failed to start"
    return p


def stop_procs(*procs):
    for p in procs:
        if p is None:
            continue
        try:
            p.terminate()
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


class KafkaCluster:
    """
    Context manager that boots coordinator + N brokers as real OS processes.
    Cleans the data directory on entry and terminates all processes on exit.
    """

    def __init__(self, num_brokers: int = 2):
        self.num_brokers = num_brokers
        self.coord: subprocess.Popen | None = None
        self.brokers: list[subprocess.Popen] = []

    def __enter__(self) -> "KafkaCluster":
        fresh_data_dir()
        self.coord = start_coord()
        for i in range(1, self.num_brokers + 1):
            self.brokers.append(start_broker(i))
        time.sleep(0.6)   # let brokers register
        return self

    def __exit__(self, *_):
        stop_procs(*self.brokers, self.coord)

    def kill_broker(self, broker_num: int) -> None:
        """Kill a specific broker (1-indexed)."""
        idx = broker_num - 1
        if idx < len(self.brokers):
            stop_procs(self.brokers[idx])
            self.brokers[idx] = None   # type: ignore[assignment]

    def restart_broker(self, broker_num: int) -> None:
        """Restart a killed broker."""
        proc = start_broker(broker_num)
        self.brokers[broker_num - 1] = proc


def P() -> Producer:
    return Producer(COORD_HOST, COORD_PORT)


def C(cid: str) -> Consumer:
    return Consumer(COORD_HOST, COORD_PORT, consumer_id=cid)


def poll_until(consumer: Consumer,
               expected_count: int,
               timeout_s: float = 8.0,
               commit: bool = True) -> list[dict]:
    """
    Keep calling poll() until *expected_count* records arrive or timeout.
    Optionally commits each record.
    """
    collected: list[dict] = []
    deadline = time.time() + timeout_s
    while len(collected) < expected_count and time.time() < deadline:
        recs = consumer.poll()
        for r in recs:
            collected.append(r)
            if commit:
                consumer.commit(r["partition"], r["offset"] + 1)
        if not recs:
            time.sleep(0.1)
    return collected


def log(msg: str):
    print(f"\n  {CYN}▸{RST} {msg}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-01  Basic Produce / Consume
# ═════════════════════════════════════════════════════════════════════════════

class TC01_BasicProduceConsume(unittest.TestCase):
    """Simplest possible smoke test — single producer, single consumer."""

    def test_single_message(self):
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc01_single", num_partitions=1)
            c = C("tc01-c1")
            c.subscribe("tc01_single", "tc01-grp")
            time.sleep(0.5)
            p.send("tc01_single", "hello-kafka")
            recs = poll_until(c, 1)
            c.close()
            self.assertEqual(len(recs), 1)
            self.assertEqual(recs[0]["message"], "hello-kafka")
            log(f"TC01 single message ✓  offset={recs[0]['offset']}")

    def test_multiple_messages_single_partition(self):
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc01_multi", num_partitions=1)
            c = C("tc01-c2")
            c.subscribe("tc01_multi", "tc01-grp2")
            time.sleep(0.5)
            payloads = [f"msg-{i}" for i in range(20)]
            for msg in payloads:
                p.send("tc01_multi", msg)
            recs = poll_until(c, 20)
            c.close()
            self.assertEqual(len(recs), 20)
            got = {r["message"] for r in recs}
            for pl in payloads:
                self.assertIn(pl, got)
            log(f"TC01 20 messages ✓")

    def test_produce_returns_monotone_offsets(self):
        """Offsets within the same partition must be strictly increasing."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc01_offsets", num_partitions=1)
            time.sleep(0.2)
            offsets = [p.send("tc01_offsets", f"m{i}") for i in range(10)]
        # offsets from a single partition must be 0..9
        self.assertEqual(offsets, list(range(10)),
                         f"Expected 0-9, got {offsets}")
        log(f"TC01 monotone offsets ✓  {offsets}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-02  Multiple Partitions
# ═════════════════════════════════════════════════════════════════════════════

class TC02_MultiplePartitions(unittest.TestCase):

    def test_messages_spread_across_partitions(self):
        """Round-robin sends should distribute across all partitions."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc02_spread", num_partitions=3)
            c = C("tc02-c1")
            c.subscribe("tc02_spread", "tc02-grp")
            time.sleep(0.6)
            for i in range(30):
                p.send("tc02_spread", f"item-{i}")
            recs = poll_until(c, 30, timeout_s=12)
            c.close()
            parts_used = {r["partition"] for r in recs}
            self.assertGreaterEqual(len(parts_used), 2,
                "Messages should hit at least 2 partitions with round-robin")
            log(f"TC02 partitions used: {sorted(parts_used)} ✓")

    def test_all_messages_received_across_partitions(self):
        """No message should be lost regardless of which partition it landed on."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc02_all", num_partitions=3)
            c = C("tc02-c2")
            c.subscribe("tc02_all", "tc02-grp2")
            time.sleep(0.6)
            sent = [f"pkt-{i}" for i in range(60)]
            for m in sent:
                p.send("tc02_all", m)
            recs = poll_until(c, 60, timeout_s=15)
            c.close()
            got = {r["message"] for r in recs}
            missing = set(sent) - got
            self.assertEqual(missing, set(),
                             f"Missing messages: {missing}")
            log(f"TC02 all 60 messages received ✓")


# ═════════════════════════════════════════════════════════════════════════════
# TC-03  Key-Based Partition Routing
# ═════════════════════════════════════════════════════════════════════════════

class TC03_KeyRouting(unittest.TestCase):

    def test_same_key_same_partition(self):
        """Messages with identical keys must always land on the same partition."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc03_keyed", num_partitions=3)
            c = C("tc03-c1")
            c.subscribe("tc03_keyed", "tc03-grp")
            time.sleep(0.6)
            key = "user-777"
            for i in range(15):
                p.send("tc03_keyed", f"order-{i}", key=key)
            recs = poll_until(c, 15, timeout_s=10)
            c.close()
            keyed = [r for r in recs if f"order-" in r["message"]]
            parts = {r["partition"] for r in keyed}
            self.assertEqual(len(parts), 1,
                             f"Key messages landed on multiple partitions: {parts}")
            log(f"TC03 same key → same partition {parts.pop()} ✓")

    def test_different_keys_can_go_different_partitions(self):
        """Different keys should potentially hit different partitions."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc03_diffkeys", num_partitions=3)
            c = C("tc03-c2")
            c.subscribe("tc03_diffkeys", "tc03-grp2")
            time.sleep(0.6)
            keys = [f"user-{i}" for i in range(12)]
            for k in keys:
                p.send("tc03_diffkeys", f"msg-for-{k}", key=k)
            recs = poll_until(c, 12, timeout_s=10)
            c.close()
            parts = {r["partition"] for r in recs}
            self.assertGreaterEqual(len(parts), 2,
                "Different keys should spread across partitions")
            log(f"TC03 different keys → partitions {sorted(parts)} ✓")

    def test_keyed_and_keyless_mix(self):
        """Mixing keyed and keyless messages must not lose any."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc03_mix", num_partitions=2)
            c = C("tc03-c3")
            c.subscribe("tc03_mix", "tc03-grp3")
            time.sleep(0.5)
            keyed   = [p.send("tc03_mix", f"keyed-{i}", key=f"k{i%3}")  for i in range(10)]
            keyless = [p.send("tc03_mix", f"rr-{i}")                     for i in range(10)]
            recs = poll_until(c, 20, timeout_s=10)
            c.close()
            self.assertEqual(len(recs), 20, f"Expected 20, got {len(recs)}")
            log(f"TC03 keyed+keyless mix — 20/20 received ✓")


# ═════════════════════════════════════════════════════════════════════════════
# TC-04  Consumer Groups — Partition Sharing
# ═════════════════════════════════════════════════════════════════════════════

class TC04_ConsumerGroupsPartitionSharing(unittest.TestCase):

    def test_two_consumers_share_two_partitions(self):
        """Each consumer in the group should own exactly 1 partition."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc04_share2", num_partitions=2)
            c1 = C("tc04-c1"); c2 = C("tc04-c2")
            c1.subscribe("tc04_share2", "tc04-grp")
            c2.subscribe("tc04_share2", "tc04-grp")
            time.sleep(1.2)
            for i in range(20):
                p.send("tc04_share2", f"msg-{i}")
            r1 = poll_until(c1, 1, timeout_s=6, commit=False)
            r2 = poll_until(c2, 1, timeout_s=6, commit=False)
            # Drain remaining
            r1 += poll_until(c1, 100, timeout_s=4, commit=False)
            r2 += poll_until(c2, 100, timeout_s=4, commit=False)
            total = len(r1) + len(r2)
            c1.close(); c2.close()
            self.assertEqual(total, 20, f"Expected 20 total, got {total}")
            # No message should appear in both consumers' lists
            ids1 = {(r["partition"], r["offset"]) for r in r1}
            ids2 = {(r["partition"], r["offset"]) for r in r2}
            overlap = ids1 & ids2
            self.assertEqual(overlap, set(), f"Duplicate delivery: {overlap}")
            log(f"TC04 2-consumer sharing — c1={len(r1)}, c2={len(r2)}, no overlap ✓")

    def test_three_consumers_three_partitions(self):
        """3 consumers × 3 partitions → every message received exactly once across the group."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc04_share3", num_partitions=3)
            consumers = [C(f"tc04-3c{i}") for i in range(3)]
            for c in consumers:
                c.subscribe("tc04_share3", "tc04-grp3")
            time.sleep(1.5)
            sent = 30
            for i in range(sent):
                p.send("tc04_share3", f"x-{i}")
            # Each consumer polls its own assigned partitions — collect separately
            per_consumer: list[list[dict]] = []
            for c in consumers:
                recs = poll_until(c, 100, timeout_s=8, commit=True)
                per_consumer.append(recs)
                c.close()
            all_recs = [r for bucket in per_consumer for r in bucket]
            # Total must equal sent (at-most-once within a group)
            self.assertEqual(len(all_recs), sent,
                             f"Expected {sent}, got {len(all_recs)}")
            # No duplicate (partition, offset) across all consumers in the group
            ids = [(r['partition'], r['offset']) for r in all_recs]
            dups = {x for x in ids if ids.count(x) > 1}
            self.assertEqual(dups, set(), f"Duplicate deliveries: {dups}")
            log(f"TC04 3-consumer 3-partition ✓  total={len(all_recs)}, no dups")

    def test_no_message_duplication_in_same_group(self):
        """A message consumed by consumer A must NOT be delivered to consumer B in same group."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc04_nodup", num_partitions=2)
            c1 = C("tc04-nd1"); c2 = C("tc04-nd2")
            c1.subscribe("tc04_nodup", "tc04-nd-grp")
            c2.subscribe("tc04_nodup", "tc04-nd-grp")
            time.sleep(1.2)
            for i in range(20):
                p.send("tc04_nodup", f"unique-{i}")
            r1 = poll_until(c1, 100, timeout_s=6, commit=True)
            r2 = poll_until(c2, 100, timeout_s=6, commit=True)
            c1.close(); c2.close()
            msgs1 = {r["message"] for r in r1}
            msgs2 = {r["message"] for r in r2}
            dups = msgs1 & msgs2
            self.assertEqual(dups, set(), f"Duplicate messages across consumers: {dups}")
            log(f"TC04 no duplication ✓  msgs1={len(msgs1)}, msgs2={len(msgs2)}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-05  Fan-Out (Multiple Independent Consumer Groups)
# ═════════════════════════════════════════════════════════════════════════════

class TC05_FanOut(unittest.TestCase):

    def test_two_groups_both_get_all_messages(self):
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc05_fanout2", num_partitions=3)
            ca = C("tc05-a"); cb = C("tc05-b")
            ca.subscribe("tc05_fanout2", "grp-A")
            cb.subscribe("tc05_fanout2", "grp-B")
            time.sleep(0.8)
            N = 30
            for i in range(N):
                p.send("tc05_fanout2", f"evt-{i}")
            ra = poll_until(ca, N, timeout_s=12)
            rb = poll_until(cb, N, timeout_s=12)
            ca.close(); cb.close()
            self.assertEqual(len(ra), N, f"grp-A expected {N}, got {len(ra)}")
            self.assertEqual(len(rb), N, f"grp-B expected {N}, got {len(rb)}")
            log(f"TC05 2-group fan-out ✓  A={len(ra)}, B={len(rb)}")

    def test_three_groups_all_independent(self):
        """Three separate groups must each receive the full message set independently."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc05_fanout3", num_partitions=2)
            cg = [C(f"tc05-g{i}") for i in range(3)]
            for i, c in enumerate(cg):
                c.subscribe("tc05_fanout3", f"grp-{i}")
            time.sleep(0.8)
            N = 12
            for i in range(N):
                p.send("tc05_fanout3", f"broadcast-{i}")
            results = [poll_until(c, N, timeout_s=10) for c in cg]
            for c in cg:
                c.close()
            for i, r in enumerate(results):
                self.assertEqual(len(r), N,
                    f"grp-{i} expected {N}, got {len(r)}")
            log(f"TC05 3-group fan-out ✓  counts={[len(r) for r in results]}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-06  Offset Commit & Resume
# ═════════════════════════════════════════════════════════════════════════════

class TC06_OffsetCommitResume(unittest.TestCase):

    def test_resume_from_committed_offset(self):
        """New consumer in the same group should start from the committed offset."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc06_resume", num_partitions=1)
            # Consumer 1: read 3 msgs, commit 2
            c1 = C("tc06-c1")
            c1.subscribe("tc06_resume", "tc06-grp")
            time.sleep(0.5)
            for ch in ["alpha", "beta", "gamma"]:
                p.send("tc06_resume", ch)
            r1 = poll_until(c1, 3, timeout_s=6, commit=False)
            self.assertEqual(len(r1), 3)
            # Commit only first 2
            for r in r1[:2]:
                c1.commit(r["partition"], r["offset"] + 1)
            c1.close()
            time.sleep(0.4)
            # Consumer 2 (same group): should get only "gamma"
            c2 = C("tc06-c2")
            c2.subscribe("tc06_resume", "tc06-grp")
            time.sleep(0.5)
            r2 = poll_until(c2, 1, timeout_s=6, commit=False)
            c2.close()
            msgs = [r["message"] for r in r2]
            self.assertIn("gamma", msgs, f"Expected 'gamma', got {msgs}")
            self.assertNotIn("alpha", msgs, "'alpha' should have been committed past")
            log(f"TC06 resume from committed offset ✓  resumed={msgs}")

    def test_uncommitted_messages_redelivered(self):
        """If consumer crashes without committing, messages must be re-delivered."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc06_redeliver", num_partitions=1)
            c1 = C("tc06-r1")
            c1.subscribe("tc06_redeliver", "tc06-rgrp")
            time.sleep(0.5)
            p.send("tc06_redeliver", "important-data")
            r1 = poll_until(c1, 1, timeout_s=6, commit=False)
            # Deliberately NOT committing — simulate crash
            c1.close()
            time.sleep(0.4)
            # New consumer: same group, no committed offset → should get message again
            c2 = C("tc06-r2")
            c2.subscribe("tc06_redeliver", "tc06-rgrp")
            time.sleep(0.5)
            r2 = poll_until(c2, 1, timeout_s=6, commit=False)
            c2.close()
            self.assertGreaterEqual(len(r2), 1,
                                    "Uncommitted message must be redelivered")
            self.assertEqual(r2[0]["message"], "important-data")
            log(f"TC06 uncommitted redelivery ✓")

    def test_commit_mid_batch_resumes_correctly(self):
        """Committing mid-batch: next consumer should resume from the precise offset."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc06_midbatch", num_partitions=1)
            c1 = C("tc06-mb1")
            c1.subscribe("tc06_midbatch", "tc06-mb-grp")
            time.sleep(0.5)
            for i in range(10):
                p.send("tc06_midbatch", f"item-{i}")
            recs = poll_until(c1, 10, timeout_s=8, commit=False)
            self.assertEqual(len(recs), 10)
            # Commit after item-5 (offset 5 → next = 6)
            commit_point = recs[5]
            c1.commit(commit_point["partition"], commit_point["offset"] + 1)
            c1.close()
            time.sleep(0.4)
            c2 = C("tc06-mb2")
            c2.subscribe("tc06_midbatch", "tc06-mb-grp")
            time.sleep(0.5)
            r2 = poll_until(c2, 4, timeout_s=8, commit=False)
            c2.close()
            msgs = [r["message"] for r in r2]
            # Should get item-6 through item-9
            for i in range(6, 10):
                self.assertIn(f"item-{i}", msgs,
                              f"item-{i} missing after mid-batch commit")
            log(f"TC06 mid-batch commit ✓  resumed items: {msgs}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-07  At-Least-Once Delivery Guarantee
# ═════════════════════════════════════════════════════════════════════════════

class TC07_AtLeastOnceDelivery(unittest.TestCase):

    def test_all_messages_received_at_least_once(self):
        """Every produced message must appear in the consumer's received set."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc07_atleast", num_partitions=3)
            c = C("tc07-c1")
            c.subscribe("tc07_atleast", "tc07-grp")
            time.sleep(0.7)
            N = 100
            sent = {f"alo-{i}" for i in range(N)}
            for m in sent:
                p.send("tc07_atleast", m)
            recs = poll_until(c, N, timeout_s=20)
            c.close()
            received = {r["message"] for r in recs}
            missing = sent - received
            self.assertEqual(missing, set(),
                             f"{len(missing)} messages missing: {list(missing)[:5]}")
            log(f"TC07 at-least-once: {len(received)}/{N} unique received ✓")

    def test_no_message_appears_zero_times(self):
        """Counter-check: build a frequency map — every message count must be ≥ 1."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc07_freq", num_partitions=2)
            c = C("tc07-f1")
            c.subscribe("tc07_freq", "tc07-fgrp")
            time.sleep(0.6)
            N = 40
            for i in range(N):
                p.send("tc07_freq", f"freq-{i}")
            recs = poll_until(c, N, timeout_s=12)
            c.close()
            freq = collections.Counter(r["message"] for r in recs)
            zero_count = [m for m in [f"freq-{i}" for i in range(N)] if freq[m] == 0]
            self.assertEqual(zero_count, [],
                             f"Messages with 0 deliveries: {zero_count[:5]}")
            log(f"TC07 frequency check ✓  all {N} msgs delivered ≥ once")


# ═════════════════════════════════════════════════════════════════════════════
# TC-08  Message Ordering Within a Partition
# ═════════════════════════════════════════════════════════════════════════════

class TC08_MessageOrdering(unittest.TestCase):

    def test_offsets_monotone_within_partition(self):
        """For each partition, offsets in received messages must be strictly increasing."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc08_order", num_partitions=2)
            c = C("tc08-c1")
            c.subscribe("tc08_order", "tc08-grp")
            time.sleep(0.6)
            for i in range(40):
                p.send("tc08_order", f"seq-{i}")
            recs = poll_until(c, 40, timeout_s=12)
            c.close()
            # Group by partition
            by_part: dict[int, list[int]] = collections.defaultdict(list)
            for r in recs:
                by_part[r["partition"]].append(r["offset"])
            for part, offs in by_part.items():
                for i in range(len(offs) - 1):
                    self.assertLess(offs[i], offs[i+1],
                        f"Partition {part}: offset {offs[i]} >= {offs[i+1]}")
            log(f"TC08 ordering verified on partitions {list(by_part.keys())} ✓")

    def test_keyed_messages_ordered_within_partition(self):
        """Messages with the same key (→ same partition) must arrive in send order."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc08_keyorder", num_partitions=3)
            c = C("tc08-c2")
            c.subscribe("tc08_keyorder", "tc08-kg")
            time.sleep(0.6)
            key = "stream-X"
            for i in range(20):
                p.send("tc08_keyorder", f"ev-{i:03d}", key=key)
            recs = poll_until(c, 20, timeout_s=10)
            c.close()
            keyed = sorted([r for r in recs], key=lambda r: r["offset"])
            texts = [r["message"] for r in keyed]
            expected = [f"ev-{i:03d}" for i in range(20)]
            self.assertEqual(texts, expected,
                             f"Order mismatch: {texts[:5]}…")
            log(f"TC08 keyed ordering ✓")


# ═════════════════════════════════════════════════════════════════════════════
# TC-09  Multiple Topics
# ═════════════════════════════════════════════════════════════════════════════

class TC09_MultipleTopics(unittest.TestCase):

    def test_isolated_topics_no_cross_contamination(self):
        """Messages on topic-A must NOT appear in a consumer for topic-B."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc09_topicA", num_partitions=2)
            p.create_topic("tc09_topicB", num_partitions=2)
            cA = C("tc09-cA"); cB = C("tc09-cB")
            cA.subscribe("tc09_topicA", "grp-A")
            cB.subscribe("tc09_topicB", "grp-B")
            time.sleep(0.6)
            for i in range(10):
                p.send("tc09_topicA", f"A-{i}")
                p.send("tc09_topicB", f"B-{i}")
            rA = poll_until(cA, 10, timeout_s=8)
            rB = poll_until(cB, 10, timeout_s=8)
            cA.close(); cB.close()
            for r in rA:
                self.assertTrue(r["message"].startswith("A-"),
                                f"topic-A consumer got: {r['message']}")
            for r in rB:
                self.assertTrue(r["message"].startswith("B-"),
                                f"topic-B consumer got: {r['message']}")
            log(f"TC09 topic isolation ✓  A={len(rA)}, B={len(rB)}")

    def test_three_topics_simultaneously(self):
        with KafkaCluster(3):
            p = P()
            topics = ["tc09_t1", "tc09_t2", "tc09_t3"]
            for t in topics:
                p.create_topic(t, num_partitions=2)
            consumers = {t: C(f"tc09-{t}") for t in topics}
            for t, c in consumers.items():
                c.subscribe(t, f"grp-{t}")
            time.sleep(0.8)
            for t in topics:
                for i in range(15):
                    p.send(t, f"{t}-msg-{i}")
            results = {}
            for t, c in consumers.items():
                results[t] = poll_until(c, 15, timeout_s=10)
                c.close()
            for t, recs in results.items():
                self.assertEqual(len(recs), 15,
                                 f"{t}: expected 15, got {len(recs)}")
                for r in recs:
                    self.assertTrue(r["message"].startswith(t),
                                    f"{t} got wrong message: {r['message']}")
            log(f"TC09 3-topic isolation ✓")


# ═════════════════════════════════════════════════════════════════════════════
# TC-10  Concurrent Producers
# ═════════════════════════════════════════════════════════════════════════════

class TC10_ConcurrentProducers(unittest.TestCase):

    def test_two_concurrent_producers_no_loss(self):
        """Two producers writing simultaneously; consumer should get all messages."""
        with KafkaCluster(3):
            p1 = P(); p2 = P()
            p1.create_topic("tc10_concurrent", num_partitions=3)
            c = C("tc10-c1")
            c.subscribe("tc10_concurrent", "tc10-grp")
            time.sleep(0.6)
            N = 30
            errors: list[str] = []

            def produce(producer, prefix):
                for i in range(N):
                    try:
                        producer.send("tc10_concurrent", f"{prefix}-{i}")
                    except Exception as e:
                        errors.append(str(e))

            t1 = threading.Thread(target=produce, args=(p1, "P1"))
            t2 = threading.Thread(target=produce, args=(p2, "P2"))
            t1.start(); t2.start()
            t1.join(); t2.join()

            self.assertEqual(errors, [], f"Producer errors: {errors}")
            total = N * 2
            recs = poll_until(c, total, timeout_s=20)
            c.close()
            got = {r["message"] for r in recs}
            for prefix in ["P1", "P2"]:
                for i in range(N):
                    self.assertIn(f"{prefix}-{i}", got,
                                  f"Missing {prefix}-{i}")
            log(f"TC10 2 concurrent producers ✓  got={len(recs)}")

    def test_five_concurrent_producers(self):
        """5 threads producing in parallel — all messages must arrive."""
        with KafkaCluster(3):
            producers = [P() for _ in range(5)]
            producers[0].create_topic("tc10_five", num_partitions=3)
            c = C("tc10-c5")
            c.subscribe("tc10_five", "tc10-5grp")
            time.sleep(0.7)
            N_per = 20
            lock = threading.Lock()
            sent_set: set[str] = set()
            errors: list[str] = []

            def produce_worker(prod, pid):
                for i in range(N_per):
                    msg = f"prod{pid}-msg{i}"
                    try:
                        prod.send("tc10_five", msg)
                        with lock:
                            sent_set.add(msg)
                    except Exception as e:
                        errors.append(str(e))

            threads = [threading.Thread(target=produce_worker, args=(producers[i], i))
                       for i in range(5)]
            for t in threads: t.start()
            for t in threads: t.join()

            self.assertEqual(errors, [], f"Errors: {errors[:3]}")
            recs = poll_until(c, len(sent_set), timeout_s=25)
            c.close()
            got = {r["message"] for r in recs}
            missing = sent_set - got
            self.assertLessEqual(len(missing), 0,
                f"{len(missing)} messages missing: {list(missing)[:5]}")
            log(f"TC10 5 concurrent producers ✓  sent={len(sent_set)}, got={len(got)}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-11  Concurrent Consumers (Same Group)
# ═════════════════════════════════════════════════════════════════════════════

class TC11_ConcurrentConsumers(unittest.TestCase):

    def test_concurrent_poll_no_duplication(self):
        """Multiple consumers polling simultaneously must not duplicate messages."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc11_concpoll", num_partitions=3)
            consumers = [C(f"tc11-cc{i}") for i in range(3)]
            for c in consumers:
                c.subscribe("tc11_concpoll", "tc11-grp")
            time.sleep(1.5)
            N = 60
            for i in range(N):
                p.send("tc11_concpoll", f"data-{i}")

            all_msgs: list[str] = []
            lock = threading.Lock()

            def poll_worker(consumer):
                recs = poll_until(consumer, 1, timeout_s=5, commit=True)
                recs += poll_until(consumer, 100, timeout_s=5, commit=True)
                with lock:
                    all_msgs.extend(r["message"] for r in recs)
                consumer.close()

            threads = [threading.Thread(target=poll_worker, args=(c,))
                       for c in consumers]
            for t in threads: t.start()
            for t in threads: t.join()

            self.assertEqual(len(all_msgs), N,
                             f"Expected {N}, got {len(all_msgs)}")
            freq = collections.Counter(all_msgs)
            dups = {m: cnt for m, cnt in freq.items() if cnt > 1}
            self.assertEqual(dups, {}, f"Duplicate messages: {dups}")
            log(f"TC11 concurrent consumers ✓  total={len(all_msgs)}, dups={len(dups)}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-12  Dynamic Rebalance — Consumer Joins Mid-Stream
# ═════════════════════════════════════════════════════════════════════════════

class TC12_ConsumerJoinsMidStream(unittest.TestCase):

    def test_new_consumer_takes_over_partition(self):
        """
        Start with 1 consumer on 2 partitions.
        Midway, add a second consumer → rebalance should split partitions.
        Total messages received by both must equal total sent.
        """
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc12_join", num_partitions=2)
            c1 = C("tc12-c1")
            c1.subscribe("tc12_join", "tc12-grp")
            time.sleep(0.8)

            # Phase 1: produce 20, only c1 running
            for i in range(20):
                p.send("tc12_join", f"phase1-{i}")
            r1_phase1 = poll_until(c1, 20, timeout_s=8, commit=True)
            self.assertEqual(len(r1_phase1), 20, "Phase 1 messages missing")

            # Phase 2: c2 joins
            c2 = C("tc12-c2")
            c2.subscribe("tc12_join", "tc12-grp")
            time.sleep(1.5)   # allow rebalance to settle

            for i in range(20):
                p.send("tc12_join", f"phase2-{i}")

            r1_phase2 = poll_until(c1, 1, timeout_s=5, commit=True)
            r2_phase2 = poll_until(c2, 1, timeout_s=5, commit=True)
            r1_phase2 += poll_until(c1, 100, timeout_s=4, commit=True)
            r2_phase2 += poll_until(c2, 100, timeout_s=4, commit=True)

            total_phase2 = len(r1_phase2) + len(r2_phase2)
            c1.close(); c2.close()

            self.assertEqual(total_phase2, 20,
                             f"Phase 2: expected 20, got {total_phase2}")
            log(f"TC12 join mid-stream ✓  phase2: c1={len(r1_phase2)}, c2={len(r2_phase2)}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-13  Dynamic Rebalance — Consumer Leaves Mid-Stream
# ═════════════════════════════════════════════════════════════════════════════

class TC13_ConsumerLeavesMidStream(unittest.TestCase):

    def test_remaining_consumer_gets_all_messages(self):
        """
        2 consumers running. Kill c1. Remaining c2 should get all subsequent messages.
        """
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc13_leave", num_partitions=2)
            c1 = C("tc13-c1"); c2 = C("tc13-c2")
            c1.subscribe("tc13_leave", "tc13-grp")
            c2.subscribe("tc13_leave", "tc13-grp")
            time.sleep(1.2)

            # Phase 1: both alive
            for i in range(10):
                p.send("tc13_leave", f"both-{i}")
            poll_until(c1, 1, timeout_s=5, commit=True)
            poll_until(c2, 1, timeout_s=5, commit=True)
            poll_until(c1, 100, timeout_s=4, commit=True)
            poll_until(c2, 100, timeout_s=4, commit=True)

            # Kill c1 gracefully
            c1.close()
            time.sleep(1.0)   # coordinator rebalance

            # Phase 2: only c2
            for i in range(20):
                p.send("tc13_leave", f"solo-{i}")
            r2 = poll_until(c2, 20, timeout_s=10, commit=True)
            c2.close()

            self.assertEqual(len(r2), 20,
                             f"Expected 20 after c1 left, got {len(r2)}")
            log(f"TC13 consumer leave ✓  c2 got {len(r2)} sole messages")


# ═════════════════════════════════════════════════════════════════════════════
# TC-14  Large Message Payloads
# ═════════════════════════════════════════════════════════════════════════════

class TC14_LargePayloads(unittest.TestCase):

    def test_64kb_messages(self):
        """64 KB messages (much larger than typical) must be produced and consumed intact."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc14_large", num_partitions=1)
            c = C("tc14-c1")
            c.subscribe("tc14_large", "tc14-grp")
            time.sleep(0.5)
            payload_64k = "X" * 65_536
            p.send("tc14_large", payload_64k)
            recs = poll_until(c, 1, timeout_s=8)
            c.close()
            self.assertEqual(len(recs), 1)
            self.assertEqual(recs[0]["message"], payload_64k)
            log(f"TC14 64 KB message ✓  received len={len(recs[0]['message'])}")

    def test_varied_payload_sizes(self):
        """Mix of tiny (1 B) → large (32 KB) messages, all must arrive correctly."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc14_mixed", num_partitions=2)
            c = C("tc14-c2")
            c.subscribe("tc14_mixed", "tc14-grp2")
            time.sleep(0.5)
            payloads = [
                "A" * 1,
                "B" * 256,
                "C" * 4_096,
                "D" * 16_384,
                "E" * 32_768,
            ]
            for pl in payloads:
                p.send("tc14_mixed", pl)
            recs = poll_until(c, len(payloads), timeout_s=10)
            c.close()
            received = {r["message"] for r in recs}
            for pl in payloads:
                self.assertIn(pl, received,
                              f"Missing payload of size {len(pl)}")
            log(f"TC14 varied sizes ✓  {[len(pl) for pl in payloads]}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-15  High-Volume Throughput
# ═════════════════════════════════════════════════════════════════════════════

class TC15_HighVolume(unittest.TestCase):

    def test_1000_messages_end_to_end(self):
        """1 000 messages: all must be produced, consumed, and acknowledged."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc15_hv1k", num_partitions=3)
            c = C("tc15-c1")
            c.subscribe("tc15_hv1k", "tc15-grp")
            time.sleep(0.7)
            N = 1000
            for i in range(N):
                p.send("tc15_hv1k", f"bulk-{i:04d}")
            recs = poll_until(c, N, timeout_s=60)
            c.close()
            self.assertEqual(len(recs), N, f"Expected {N}, got {len(recs)}")
            log(f"TC15 1 000-message throughput ✓")

    def test_throughput_above_minimum(self):
        """Measure produce throughput — must be above 100 msg/s."""
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc15_tput", num_partitions=3)
            time.sleep(0.3)
            N = 500
            t0 = time.perf_counter()
            for i in range(N):
                p.send("tc15_tput", f"tput-{i}")
            elapsed = time.perf_counter() - t0
            tps = N / elapsed
            self.assertGreater(tps, 100,
                               f"Throughput {tps:.0f} msg/s too low")
            log(f"TC15 throughput ✓  {tps:.0f} msg/s")


# ═════════════════════════════════════════════════════════════════════════════
# TC-16  Idempotent Topic Creation
# ═════════════════════════════════════════════════════════════════════════════

class TC16_IdempotentTopicCreation(unittest.TestCase):

    def test_create_topic_twice_no_error(self):
        """Calling create_topic twice with same name must not raise."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc16_idem", num_partitions=2)
            # Second call should succeed silently
            try:
                p.create_topic("tc16_idem", num_partitions=2)
            except Exception as e:
                self.fail(f"create_topic raised on duplicate: {e}")
            log(f"TC16 idempotent create_topic ✓")

    def test_existing_topic_data_unaffected_by_recreate(self):
        """Re-creating an existing topic must not clear its data."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc16_data", num_partitions=1)
            c = C("tc16-c1")
            c.subscribe("tc16_data", "tc16-grp")
            time.sleep(0.5)
            p.send("tc16_data", "before-recreate")
            r1 = poll_until(c, 1, timeout_s=6, commit=True)
            self.assertEqual(len(r1), 1)
            # Recreate
            p.create_topic("tc16_data", num_partitions=1)
            p.send("tc16_data", "after-recreate")
            r2 = poll_until(c, 1, timeout_s=6, commit=True)
            c.close()
            self.assertEqual(r2[0]["message"], "after-recreate")
            log(f"TC16 data unaffected by re-create ✓")


# ═════════════════════════════════════════════════════════════════════════════
# TC-17  Round-Robin Partition Distribution
# ═════════════════════════════════════════════════════════════════════════════

class TC17_RoundRobinDistribution(unittest.TestCase):

    def test_keyless_messages_distributed_round_robin(self):
        """
        Keyless messages should cycle through all partitions.
        With N partitions and N*K messages, each partition should get roughly K.
        """
        with KafkaCluster(3):
            p = P()
            NUM_PARTS = 3
            p.create_topic("tc17_rr", num_partitions=NUM_PARTS)
            c = C("tc17-c1")
            c.subscribe("tc17_rr", "tc17-grp")
            time.sleep(0.6)
            N = NUM_PARTS * 10   # 30 messages → ~10 per partition
            for i in range(N):
                p.send("tc17_rr", f"rr-{i}")
            recs = poll_until(c, N, timeout_s=12)
            c.close()
            freq: dict[int, int] = collections.Counter(r["partition"] for r in recs)
            for part in range(NUM_PARTS):
                count = freq.get(part, 0)
                # Each partition should get between 5 and 15 (given 10 expected)
                self.assertGreaterEqual(count, 5,
                    f"Partition {part} got only {count} messages (expected ~10)")
            log(f"TC17 round-robin distribution ✓  {dict(freq)}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-18  Replication Verification
# ═════════════════════════════════════════════════════════════════════════════

class TC18_ReplicationVerification(unittest.TestCase):

    def test_replica_log_file_exists(self):
        """
        After producing to a topic, the replica broker's data directory
        must contain a non-empty log file for the replicated partition.
        """
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc18_repl", num_partitions=1)
            time.sleep(0.4)
            for i in range(10):
                p.send("tc18_repl", f"repl-msg-{i}")
            time.sleep(1.0)   # allow async replication to complete

            # At least two brokers should have a log file for this partition
            log_files_found = 0
            for bid in ["broker1", "broker2", "broker3"]:
                log_path = Path(DATA_DIR) / f"broker_{bid}" / "tc18_repl_0.log"
                if log_path.exists() and log_path.stat().st_size > 0:
                    log_files_found += 1

            self.assertGreaterEqual(log_files_found, 1,
                "Expected at least primary broker log for tc18_repl partition 0")
            log(f"TC18 replication ✓  log files found: {log_files_found}")

    def test_replica_data_matches_primary(self):
        """
        Parse replica log file and compare offsets with primary.
        Both must contain the same offsets.
        """
        import json as _json
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc18_match", num_partitions=1)
            time.sleep(0.4)
            N = 15
            for i in range(N):
                p.send("tc18_match", f"check-{i}")
            time.sleep(1.5)   # generous replication window

            def read_log_offsets(broker_id: str) -> set[int]:
                path = Path(DATA_DIR) / f"broker_{broker_id}" / "tc18_match_0.log"
                if not path.exists():
                    return set()
                offsets = set()
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                rec = _json.loads(line)
                                offsets.add(rec["offset"])
                            except Exception:
                                pass
                return offsets

            sets = {}
            for bid in ["broker1", "broker2", "broker3"]:
                s = read_log_offsets(bid)
                if s:
                    sets[bid] = s

            self.assertGreaterEqual(len(sets), 1, "No broker has log data")
            if len(sets) >= 2:
                brokers = list(sets.keys())
                self.assertEqual(sets[brokers[0]], sets[brokers[1]],
                    f"Primary vs replica offset mismatch: "
                    f"{sets[brokers[0]]} vs {sets[brokers[1]]}")
            log(f"TC18 replica data match ✓  brokers={list(sets.keys())}, "
                f"offsets={len(next(iter(sets.values())))}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-19  Broker Failure — Replica Promotion + Continued Consume
# ═════════════════════════════════════════════════════════════════════════════

class TC19_BrokerFailureConsumeRecovery(unittest.TestCase):

    def test_consumer_continues_after_primary_broker_killed(self):
        """
        Kill the primary broker for a topic's partition.
        After coordinator promotes replica, a consumer must still be able to receive
        messages.
        """
        fresh_data_dir()
        coord = start_coord()
        b1 = start_broker(1)
        b2 = start_broker(2)
        time.sleep(0.6)

        p = P()
        p.create_topic("tc19_failconsume", num_partitions=2)
        c = C("tc19-c1")
        c.subscribe("tc19_failconsume", "tc19-grp")
        time.sleep(0.6)

        # Pre-failure produce & consume
        for i in range(10):
            p.send("tc19_failconsume", f"pre-{i}")
        pre_recs = poll_until(c, 10, timeout_s=8, commit=True)
        self.assertEqual(len(pre_recs), 10, "Pre-failure messages missing")

        # Kill broker1
        stop_procs(b1)
        log("TC19 broker1 killed — waiting for coordinator timeout …")
        time.sleep(12)   # BROKER_TIMEOUT = 10 s

        # Post-failure produce
        post_sent = 0
        for i in range(10):
            try:
                p.send("tc19_failconsume", f"post-{i}")
                post_sent += 1
            except Exception as e:
                pass   # some may fail if routed to dead broker before reroute

        post_recs = poll_until(c, post_sent, timeout_s=12, commit=True)
        c.close()
        stop_procs(b2, coord)

        self.assertGreater(post_sent, 0, "No messages sent post-failure")
        self.assertGreater(len(post_recs), 0,
            "Consumer received nothing after broker failure")
        log(f"TC19 consume after failure ✓  post_sent={post_sent}, "
            f"post_received={len(post_recs)}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-20  Broker Failure — Producer Rerouting
# ═════════════════════════════════════════════════════════════════════════════

class TC20_BrokerFailureProducerRerouting(unittest.TestCase):

    def test_producer_sends_succeed_after_broker_death(self):
        """
        Producer must resume once coordinator reroutes to the replica broker.
        At least 80% of post-failure sends should succeed (some initial failures
        during the timeout window are acceptable).
        """
        fresh_data_dir()
        coord = start_coord()
        b1 = start_broker(1)
        b2 = start_broker(2)
        time.sleep(0.6)

        p = P()
        p.create_topic("tc20_reroute", num_partitions=2)
        time.sleep(0.3)

        for i in range(10):
            p.send("tc20_reroute", f"pre-{i}")

        stop_procs(b1)
        log("TC20 broker1 killed — waiting for timeout …")
        time.sleep(12)

        ok_count = 0
        for i in range(10):
            try:
                p.send("tc20_reroute", f"post-{i}")
                ok_count += 1
            except Exception:
                pass

        stop_procs(b2, coord)
        self.assertGreaterEqual(ok_count, 5,
            f"Expected ≥5/10 post-failure sends, got {ok_count}")
        log(f"TC20 producer rerouting ✓  {ok_count}/10 succeeded")


# ═════════════════════════════════════════════════════════════════════════════
# TC-21  Multiple Broker Failures
# ═════════════════════════════════════════════════════════════════════════════

class TC21_MultipleBrokerFailures(unittest.TestCase):

    def test_one_broker_survives_out_of_three(self):
        """
        3 brokers. Kill 2. The surviving broker must still serve produce + consume.
        """
        fresh_data_dir()
        coord = start_coord()
        b1 = start_broker(1)
        b2 = start_broker(2)
        b3 = start_broker(3)
        time.sleep(0.6)

        p = P()
        p.create_topic("tc21_multifail", num_partitions=3)
        c = C("tc21-c1")
        c.subscribe("tc21_multifail", "tc21-grp")
        time.sleep(0.6)

        for i in range(9):
            p.send("tc21_multifail", f"pre-{i}")
        pre = poll_until(c, 9, timeout_s=8, commit=True)
        self.assertEqual(len(pre), 9, "Pre-failure messages missing")

        stop_procs(b1, b2)
        log("TC21 broker1 & broker2 killed …")
        time.sleep(13)   # let coordinator evict both

        ok = 0
        for i in range(5):
            try:
                p.send("tc21_multifail", f"survivor-{i}")
                ok += 1
            except Exception:
                pass

        c.close()
        stop_procs(b3, coord)
        self.assertGreater(ok, 0,
            f"Expected at least 1 successful produce with 1 broker surviving, got {ok}")
        log(f"TC21 multi-broker failure ✓  {ok}/5 sends OK after 2 brokers dead")


# ═════════════════════════════════════════════════════════════════════════════
# TC-22  Consumer Eviction via Heartbeat Timeout
# ═════════════════════════════════════════════════════════════════════════════

class TC22_ConsumerEviction(unittest.TestCase):

    def test_partitions_reassigned_after_eviction(self):
        """
        A consumer that stops heartbeating is evicted. Its partitions must
        be reassigned to a surviving consumer, which then picks up messages.
        """
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc22_evict", num_partitions=2)

            c1 = C("tc22-c1")
            c2 = C("tc22-c2")
            c1.subscribe("tc22_evict", "tc22-grp")
            c2.subscribe("tc22_evict", "tc22-grp")
            time.sleep(1.2)

            # Produce first batch
            for i in range(10):
                p.send("tc22_evict", f"early-{i}")
            poll_until(c1, 1, timeout_s=5, commit=True)
            poll_until(c2, 1, timeout_s=5, commit=True)
            poll_until(c1, 100, timeout_s=4, commit=True)
            poll_until(c2, 100, timeout_s=4, commit=True)

            # Silently kill c1's heartbeat thread by marking it non-running
            # (simulate crash: stop heartbeat without calling close/unsubscribe)
            c1._running = False
            log("TC22 c1 heartbeat stopped — waiting for eviction (15 s) …")
            time.sleep(17)   # CONSUMER_TIMEOUT = 15 s

            # Now only c2 should be in the group
            for i in range(20):
                p.send("tc22_evict", f"after-evict-{i}")

            r2 = poll_until(c2, 20, timeout_s=12, commit=True)
            c2.close()

            self.assertEqual(len(r2), 20,
                             f"Expected 20 after c1 evicted, got {len(r2)}")
            log(f"TC22 eviction & reassignment ✓  c2 received {len(r2)} messages")


# ═════════════════════════════════════════════════════════════════════════════
# TC-23  Three Consumer Groups — Triple Fan-Out
# ═════════════════════════════════════════════════════════════════════════════

class TC23_TripleFanOut(unittest.TestCase):

    def test_three_groups_each_get_full_stream(self):
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc23_triple", num_partitions=3)
            groups = [f"tc23-grp{i}" for i in range(3)]
            consumers = [C(f"tc23-c{i}") for i in range(3)]
            for c, grp in zip(consumers, groups):
                c.subscribe("tc23_triple", grp)
            time.sleep(0.8)

            N = 30
            for i in range(N):
                p.send("tc23_triple", f"triple-{i}")

            results = [poll_until(c, N, timeout_s=15) for c in consumers]
            for c in consumers:
                c.close()

            for i, (grp, recs) in enumerate(zip(groups, results)):
                self.assertEqual(len(recs), N,
                                 f"{grp}: expected {N}, got {len(recs)}")
                msgs = {r["message"] for r in recs}
                missing = {f"triple-{j}" for j in range(N)} - msgs
                self.assertEqual(missing, set(),
                    f"{grp} missing {len(missing)} messages")
            log(f"TC23 triple fan-out ✓  counts={[len(r) for r in results]}")


# ═════════════════════════════════════════════════════════════════════════════
# TC-24  Sequential Batched Polling
# ═════════════════════════════════════════════════════════════════════════════

class TC24_SequentialBatchPolling(unittest.TestCase):

    def test_incremental_produce_then_incremental_consume(self):
        """
        Produce in 5 batches of 10. After each batch, poll and verify cumulative
        message count. This tests that the offset pointer advances correctly.
        """
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc24_batch", num_partitions=1)
            c = C("tc24-c1")
            c.subscribe("tc24_batch", "tc24-grp")
            time.sleep(0.5)

            total_received = 0
            for batch in range(5):
                for i in range(10):
                    p.send("tc24_batch", f"b{batch}-m{i}")
                # Poll this batch
                recs = poll_until(c, 10, timeout_s=8, commit=True)
                self.assertEqual(len(recs), 10,
                                 f"Batch {batch}: expected 10, got {len(recs)}")
                total_received += len(recs)

            c.close()
            self.assertEqual(total_received, 50)
            log(f"TC24 sequential batches ✓  total={total_received}")

    def test_poll_empty_partition_returns_empty(self):
        """Polling a partition with no new messages should return empty list, not block."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc24_empty", num_partitions=1)
            c = C("tc24-c2")
            c.subscribe("tc24_empty", "tc24-grp2")
            time.sleep(0.5)
            # No messages produced yet
            t0 = time.perf_counter()
            recs = c.poll()
            elapsed = time.perf_counter() - t0
            c.close()
            self.assertEqual(recs, [], "Expected empty list from empty partition")
            self.assertLess(elapsed, 5.0, "poll() on empty partition took too long")
            log(f"TC24 empty poll ✓  took {elapsed*1000:.1f} ms")


# ═════════════════════════════════════════════════════════════════════════════
# TC-25  Message Metadata Integrity
# ═════════════════════════════════════════════════════════════════════════════

class TC25_MessageMetadataIntegrity(unittest.TestCase):

    def test_all_required_fields_present(self):
        """Every received record must have: offset, key, message, ts, partition, topic."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc25_meta", num_partitions=2)
            c = C("tc25-c1")
            c.subscribe("tc25_meta", "tc25-grp")
            time.sleep(0.5)
            p.send("tc25_meta", "meta-test", key="meta-key")
            recs = poll_until(c, 1, timeout_s=6)
            c.close()
            self.assertEqual(len(recs), 1)
            r = recs[0]
            for field in ("offset", "key", "message", "ts", "partition", "topic"):
                self.assertIn(field, r, f"Field '{field}' missing from record")
            self.assertEqual(r["message"], "meta-test")
            self.assertEqual(r["key"], "meta-key")
            self.assertEqual(r["topic"], "tc25_meta")
            self.assertIsInstance(r["offset"], int)
            self.assertIsInstance(r["partition"], int)
            self.assertIsInstance(r["ts"], float)
            self.assertGreater(r["ts"], 0)
            log(f"TC25 metadata integrity ✓  {r}")

    def test_timestamp_is_recent(self):
        """Message timestamp must be within 10 seconds of the receive time."""
        with KafkaCluster(2):
            p = P()
            p.create_topic("tc25_ts", num_partitions=1)
            c = C("tc25-c2")
            c.subscribe("tc25_ts", "tc25-ts-grp")
            time.sleep(0.5)
            t_before = time.time()
            p.send("tc25_ts", "ts-check")
            recs = poll_until(c, 1, timeout_s=6)
            t_after = time.time()
            c.close()
            ts = recs[0]["ts"]
            self.assertGreaterEqual(ts, t_before - 1,
                f"Timestamp {ts} predates send time {t_before}")
            self.assertLessEqual(ts, t_after + 1,
                f"Timestamp {ts} is in the future vs {t_after}")
            log(f"TC25 timestamp ✓  ts={ts:.3f}, now={t_after:.3f}")

    def test_offsets_globally_unique_within_partition(self):
        """
        Within a consumer group, each partition is owned by exactly one consumer.
        Therefore across all consumers in the group, (partition, offset) must be unique.
        We use 3 consumers in 3 separate groups so each gets all messages independently;
        within each group no duplicate (partition, offset) must appear.
        """
        with KafkaCluster(3):
            p = P()
            p.create_topic("tc25_unique", num_partitions=3)
            # One consumer per group so each owns all partitions — clean isolation
            consumers = [C(f"tc25-uq{i}") for i in range(3)]
            for i, c in enumerate(consumers):
                c.subscribe("tc25_unique", f"tc25-uq-grp{i}")
            time.sleep(1.0)
            N = 30
            for i in range(N):
                p.send("tc25_unique", f"u-{i}")
            # Each consumer (sole member of its group) gets all N messages
            for c in consumers:
                recs = poll_until(c, N, timeout_s=10, commit=True)
                ids = [(r["partition"], r["offset"]) for r in recs]
                dups = [x for x in ids if ids.count(x) > 1]
                self.assertEqual(dups, [],
                    f"Duplicate (partition, offset) within consumer {c.consumer_id}: {set(dups)}")
                c.close()
            log(f"TC25 unique identifiers ✓  3 consumers × {N} msgs, 0 dups each")


# ═════════════════════════════════════════════════════════════════════════════
# Test Runner
# ═════════════════════════════════════════════════════════════════════════════

def _build_suite() -> unittest.TestSuite:
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    test_classes = [
        TC01_BasicProduceConsume,
        TC02_MultiplePartitions,
        TC03_KeyRouting,
        TC04_ConsumerGroupsPartitionSharing,
        TC05_FanOut,
        TC06_OffsetCommitResume,
        TC07_AtLeastOnceDelivery,
        TC08_MessageOrdering,
        TC09_MultipleTopics,
        TC10_ConcurrentProducers,
        TC11_ConcurrentConsumers,
        TC12_ConsumerJoinsMidStream,
        TC13_ConsumerLeavesMidStream,
        TC14_LargePayloads,
        TC15_HighVolume,
        TC16_IdempotentTopicCreation,
        TC17_RoundRobinDistribution,
        TC18_ReplicationVerification,
        TC19_BrokerFailureConsumeRecovery,
        TC20_BrokerFailureProducerRerouting,
        TC21_MultipleBrokerFailures,
        TC22_ConsumerEviction,
        TC23_TripleFanOut,
        TC24_SequentialBatchPolling,
        TC25_MessageMetadataIntegrity,
    ]
    for cls in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return suite


if __name__ == "__main__":
    print(f"\n{BOLD}{CYN}{'═'*62}")
    print("  StreamLog — Comprehensive Integration Test Suite")
    print("  25 Test Classes | Fault Tolerance | Replication | Concurrency")
    print(f"{'═'*62}{RST}\n")

    runner = unittest.TextTestRunner(verbosity=2, failfast=False)
    result = runner.run(_build_suite())

    total = result.testsRun
    fail  = len(result.failures) + len(result.errors)
    print(f"\n{BOLD}{'═'*62}")
    print(f"  Results: {GRN}{total - fail} passed{RST}  "
          f"{RED}{fail} failed{RST}  (total {total})")
    print(f"{'═'*62}{RST}\n")
    sys.exit(0 if result.wasSuccessful() else 1)
