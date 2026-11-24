"""
demo/ecommerce_demo.py
======================
E-commerce scenario demonstration for the Kafka-inspired pub-sub system.

Scenario:
  - An "order service" publishes 20 orders to the `orders` topic (3 partitions)
  - Two independent consumer groups process orders in parallel:
      * payment-group  : 2 consumers sharing partitions (parallel payment processing)
      * inventory-group: 1 consumer receiving ALL messages (independent fan-out)

Prerequisites (run this first):
  Start the cluster:  bash scripts/start_cluster.sh

Usage:
  python demo/ecommerce_demo.py
"""

from __future__ import annotations

import random
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streamlog import Consumer, Producer

COORD_HOST = "127.0.0.1"
COORD_PORT = 9000
TOPIC = "orders"
NUM_ORDERS = 20
NUM_PARTS = 3

# ANSI colors for visual clarity
RESET = "\033[0m"
GREEN = "\033[92m"
BLUE = "\033[94m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"

PRODUCT_NAMES = [
    "Laptop", "Phone", "Tablet", "Headphones", "Camera",
    "Keyboard", "Mouse", "Monitor", "SSD", "USB Hub",
]
CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune", "Hyderabad"]


def print_banner():
    print(f"""
{BOLD}{CYAN}╔══════════════════════════════════════════════════════════╗
║           StreamLog — E-Commerce Event Demo              ║
╚══════════════════════════════════════════════════════════╝{RESET}
""")


def run_producer():
    """Publish NUM_ORDERS orders to the orders topic."""
    print(f"{BOLD}{GREEN}[ORDER SERVICE]{RESET} Starting …")
    p = Producer(COORD_HOST, COORD_PORT)
    p.create_topic(TOPIC, num_partitions=NUM_PARTS)
    time.sleep(0.5)  # wait for consumers to subscribe

    for i in range(1, NUM_ORDERS + 1):
        product  = random.choice(PRODUCT_NAMES)
        qty      = random.randint(1, 5)
        price    = round(random.uniform(99, 4999), 2)
        city     = random.choice(CITIES)
        user_id  = f"user-{random.randint(1000, 9999)}"
        order_id = f"ORD-{1000 + i}"

        payload = (
            f"order_id={order_id}  product={product}  "
            f"qty={qty}  price=₹{price}  city={city}  user_id={user_id}"
        )

        offset = p.send(TOPIC, payload, key=user_id)
        print(f"{GREEN}[ORDER SERVICE]{RESET} Published #{i:02d} → {order_id}  "
              f"part=? offset={offset}")
        time.sleep(0.1)

    print(f"{GREEN}[ORDER SERVICE]{RESET} All {NUM_ORDERS} orders published.\n")


def run_consumer(consumer_id: str, group_id: str, color: str,
                 received: list, stop_event: threading.Event):
    """Poll until stop_event is set (producer done + grace period)."""
    c = Consumer(COORD_HOST, COORD_PORT, consumer_id=consumer_id)
    c.subscribe(TOPIC, group_id)
    print(f"{color}[{consumer_id}]{RESET} Subscribed to '{TOPIC}' in group '{group_id}'")

    while not stop_event.is_set():
        records = c.poll(timeout_ms=500)
        for rec in records:
            received.append(rec)
            print(f"{color}[{consumer_id}]{RESET} ← part={rec['partition']} "
                  f"offset={rec['offset']}  {rec['message'][:60]}…")
            c.commit(rec["partition"], rec["offset"] + 1)
        time.sleep(0.2)

    c.close()


def wait_for_coordinator(host: str, port: int, retries: int = 15):
    import socket
    for _ in range(retries):
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            return True
        except OSError:
            print("Waiting for coordinator …")
            time.sleep(1)
    return False


def main():
    print_banner()

    if not wait_for_coordinator(COORD_HOST, COORD_PORT):
        print("ERROR: Could not connect to coordinator. Is the cluster running?")
        print("Run:  bash scripts/start_cluster.sh")
        sys.exit(1)

    stop_event = threading.Event()

    # Message buckets per consumer
    payment_c1_msgs: list  = []
    payment_c2_msgs: list  = []
    inventory_msgs:  list  = []

    threads = [
        threading.Thread(
            target=run_consumer,
            args=("payment-consumer-1", "payment-group", BLUE,
                  payment_c1_msgs, stop_event),
            daemon=True,
        ),
        threading.Thread(
            target=run_consumer,
            args=("payment-consumer-2", "payment-group", BLUE,
                  payment_c2_msgs, stop_event),
            daemon=True,
        ),
        threading.Thread(
            target=run_consumer,
            args=("inventory-consumer-1", "inventory-group", YELLOW,
                  inventory_msgs, stop_event),
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    # Let consumers subscribe before producer fires
    time.sleep(1.5)
    run_producer()

    # Grace period to drain in-flight messages
    time.sleep(3)
    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_payment = len(payment_c1_msgs) + len(payment_c2_msgs)
    print(f"""
{BOLD}{CYAN}═══════════════════════ SUMMARY ═══════════════════════{RESET}
{BOLD}payment-group  (2 consumers, partition-sharing){RESET}
  payment-consumer-1 received : {len(payment_c1_msgs):3d} messages
  payment-consumer-2 received : {len(payment_c2_msgs):3d} messages
  Combined (should ≈ {NUM_ORDERS})       : {total_payment:3d} messages

{BOLD}inventory-group  (1 consumer, fan-out){RESET}
  inventory-consumer-1 received: {len(inventory_msgs):3d} messages (should ≈ {NUM_ORDERS})

{BOLD}Fan-out check:{RESET} payment-group got {total_payment}, inventory-group got {len(inventory_msgs)}
{CYAN}═══════════════════════════════════════════════════════{RESET}
""")


if __name__ == "__main__":
    main()
