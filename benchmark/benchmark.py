"""
benchmark/benchmark.py
=======================
Benchmark script for the Kafka-inspired pub-sub system.

Measures:
  1. Throughput   — messages/second for bulk produce
  2. Latency      — end-to-end round-trip (produce → consume) per message
  3. Scalability  — throughput at different message sizes
  4. Recovery     — (manual) documents how to test broker failover

Usage:
  # Ensure cluster is running: bash scripts/start_cluster.sh
  python benchmark/benchmark.py
"""

from __future__ import annotations

import argparse
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streamlog import Consumer, Producer

COORD_HOST = "127.0.0.1"
COORD_PORT = 9000

# Separate ports for self-managed broker scaling benchmark
SCALE_COORD_PORT = 20000
SCALE_DATA_DIR = "/tmp/kafka_bench_data"

RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"


def section(title: str):
    print(f"\n{BOLD}{CYAN}{'─'*55}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*55}{RESET}")


# ─── Self-managed cluster helpers (for broker scaling benchmark) ─────────────

def _start_process(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(Path(__file__).resolve().parent.parent),
    )


def _wait_port(host: str, port: int, timeout: float = 12.0) -> bool:
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except OSError:
            time.sleep(0.15)
    return False


def _fresh_data_dir():
    shutil.rmtree(SCALE_DATA_DIR, ignore_errors=True)
    Path(SCALE_DATA_DIR).mkdir(parents=True, exist_ok=True)


def _start_coord(port: int) -> subprocess.Popen:
    p = _start_process([
        sys.executable, "-m", "streamlog.coordinator",
        "--host", COORD_HOST, "--port", str(port),
    ])
    if not _wait_port(COORD_HOST, port, 12):
        raise RuntimeError("Coordinator failed to start for scaling benchmark")
    return p


def _start_broker(broker_num: int, coord_port: int) -> subprocess.Popen:
    port = coord_port + broker_num
    p = _start_process([
        sys.executable, "-m", "streamlog.broker",
        "--id", f"broker{broker_num}",
        "--host", COORD_HOST,
        "--port", str(port),
        "--coord-host", COORD_HOST,
        "--coord-port", str(coord_port),
        "--data-dir", SCALE_DATA_DIR,
    ])
    if not _wait_port(COORD_HOST, port, 12):
        raise RuntimeError(f"Broker {broker_num} failed to start")
    return p


def _stop_procs(*procs):
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


# ─── Benchmark 1: Throughput ─────────────────────────────────────────────────

def bench_throughput(num_messages: int = 5000, num_partitions: int = 3):
    section(f"1. Throughput  ({num_messages} messages, {num_partitions} partitions)")
    topic = "bench_throughput"
    p = Producer(COORD_HOST, COORD_PORT)
    p.create_topic(topic, num_partitions=num_partitions)
    time.sleep(0.3)

    msg = "x" * 256  # 256-byte payload

    start = time.perf_counter()
    for i in range(num_messages):
        p.send(topic, f"{msg}_{i}")
    elapsed = time.perf_counter() - start

    tps = num_messages / elapsed
    print(f"  Messages sent : {num_messages}")
    print(f"  Elapsed       : {elapsed:.3f} s")
    print(f"  {GREEN}Throughput    : {tps:,.0f} msg/s{RESET}")
    return tps


# ─── Benchmark 2: E2E Latency ─────────────────────────────────────────────────

def bench_latency(num_samples: int = 2000):
    section(f"2. End-to-End Latency  ({num_samples} samples)")
    topic = "bench_latency"
    p = Producer(COORD_HOST, COORD_PORT)
    p.create_topic(topic, num_partitions=1)
    time.sleep(0.3)

    c = Consumer(COORD_HOST, COORD_PORT, consumer_id="bench-latency-consumer")
    c.subscribe(topic, "bench-latency-group")
    time.sleep(0.5)

    latencies_ms: list[float] = []

    for i in range(num_samples):
        t0 = time.perf_counter()
        p.send(topic, f"latency-probe-{i}")

        # Poll until we get the message
        deadline = time.perf_counter() + 5.0
        received = False
        while time.perf_counter() < deadline and not received:
            records = c.poll()
            for rec in records:
                if f"latency-probe-{i}" in rec["message"]:
                    t1 = time.perf_counter()
                    latencies_ms.append((t1 - t0) * 1000)
                    c.commit(rec["partition"], rec["offset"] + 1)
                    received = True
                    break
            if not received:
                time.sleep(0.01)

        if not received:
            print(f"  WARNING: probe {i} timed out")

    c.close()

    if latencies_ms:
        print(f"  Samples       : {len(latencies_ms)}")
        print(f"  {GREEN}Min latency   : {min(latencies_ms):.2f} ms{RESET}")
        print(f"  {GREEN}Max latency   : {max(latencies_ms):.2f} ms{RESET}")
        print(f"  {GREEN}Mean latency  : {statistics.mean(latencies_ms):.2f} ms{RESET}")
        print(f"  {GREEN}p95 latency   : {statistics.quantiles(latencies_ms, n=20)[-1]:.2f} ms{RESET}")
    return latencies_ms


# ─── Benchmark 3: Scalability (payload sizes) ─────────────────────────────────

def bench_scalability():
    section("3. Scalability — Throughput vs Message Size")
    sizes   = [64, 256, 1024, 4096, 16384, 65536]
    results = []

    for size in sizes:
        topic = f"bench_scale_{size}"
        p = Producer(COORD_HOST, COORD_PORT)
        p.create_topic(topic, num_partitions=3)
        time.sleep(0.2)

        msg   = "x" * size
        count = 150
        start = time.perf_counter()
        for i in range(count):
            p.send(topic, f"{msg}_{i}")
        elapsed = time.perf_counter() - start
        tps = count / elapsed
        results.append((size, tps))
        print(f"  Size={size:5d}B  → {GREEN}{tps:,.0f} msg/s{RESET}  ({elapsed:.2f} s for {count} msgs)")

    return results


# ─── Benchmark 4: Broker Count Scalability (self-managed) ───────────────────

def bench_broker_scaling(broker_counts: list[int] | None = None,
                         num_messages: int = 500):
    section("4. Scalability — Throughput vs Broker Count")
    if broker_counts is None:
        broker_counts = [1, 2, 3, 4, 5]

    results = []
    for count in broker_counts:
        _fresh_data_dir()
        coord = _start_coord(SCALE_COORD_PORT)
        brokers = [_start_broker(i, SCALE_COORD_PORT) for i in range(1, count + 1)]
        time.sleep(0.6)

        p = Producer(COORD_HOST, SCALE_COORD_PORT)
        topic = f"bench_brokers_{count}"
        p.create_topic(topic, num_partitions=max(1, count))
        time.sleep(0.3)

        msg = "x" * 256
        start = time.perf_counter()
        for i in range(num_messages):
            p.send(topic, f"{msg}_{i}")
        elapsed = time.perf_counter() - start
        tps = num_messages / elapsed
        results.append((count, tps))
        print(f"  Brokers={count}  {GREEN}{tps:,.0f} msg/s{RESET}  ({elapsed:.2f} s)")

        _stop_procs(*brokers, coord)
        time.sleep(0.4)

    return results


# ─── Benchmark 5: Partition Scalability ───────────────────────────────────────

def bench_partition_scaling():
    section("5. Scalability — Throughput vs Partitions")
    partitions_to_test = [1, 3, 6, 9]
    results = []

    for parts in partitions_to_test:
        topic = f"bench_parts_{parts}"
        p = Producer(COORD_HOST, COORD_PORT)
        p.create_topic(topic, num_partitions=parts)
        time.sleep(0.3)

        msg = "x" * 256
        count = 1000
        start = time.perf_counter()
        for i in range(count):
            p.send(topic, f"{msg}_{i}")
        elapsed = time.perf_counter() - start
        tps = count / elapsed
        results.append((parts, tps))
        print(f"  Partitions={parts:2d}  → {GREEN}{tps:,.0f} msg/s{RESET}  ({elapsed:.2f} s for {count} msgs)")

    return results


# ─── Benchmark 6: Failure Recovery Info ──────────────────────────────────────

def info_failure_recovery():
    section("5. Failure Recovery (Manual Test Instructions)")
    print(f"""
  {BOLD}How to test broker failure recovery:{RESET}

  1. Start cluster:  bash scripts/start_cluster.sh
  2. Run demo:       python demo/ecommerce_demo.py  (in background)
  3. Kill broker1:   kill $(lsof -ti:9001)
  4. Observe coordinator log — should print:
       "Broker DEAD id=broker1 — promoting replicas"
     and consumers in demo should continue receiving messages
     from the promoted replica broker(s).
  5. Recovery time is bounded by BROKER_TIMEOUT = {RESET}10 s (configurable in streamlog/config.py).
""")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="StreamLog Benchmark Suite")
    parser.add_argument(
        "--scale-brokers",
        action="store_true",
        help="Run broker-count scalability benchmark using a self-managed cluster",
    )
    args = parser.parse_args()

    print(f"""
{BOLD}{CYAN}╔══════════════════════════════════════════════════════════╗
║               StreamLog Benchmark Suite                  ║
╚══════════════════════════════════════════════════════════╝{RESET}
""")
    import socket as _sock
    try:
        s = _sock.create_connection((COORD_HOST, COORD_PORT), timeout=2)
        s.close()
    except OSError:
        print("ERROR: Coordinator not reachable. Run:  bash scripts/start_cluster.sh")
        sys.exit(1)

    tps = bench_throughput()
    latencies = bench_latency()
    bench_scalability()
    broker_scale = []
    if args.scale_brokers:
        broker_scale = bench_broker_scaling()
    part_scale = bench_partition_scaling()
    info_failure_recovery()

    section("Benchmark Complete")
    print(f"  Throughput  : {tps:,.0f} msg/s")
    if latencies:
        import statistics
        print(f"  Mean latency: {statistics.mean(latencies):.2f} ms")
    if broker_scale:
        summary = ", ".join([f"{n}b={tps:,.0f}" for n, tps in broker_scale])
        print(f"  Broker scale: {summary} msg/s")
    print()


if __name__ == "__main__":
    main()
