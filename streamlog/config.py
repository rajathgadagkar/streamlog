"""
streamlog.config
================
Centralized configuration defaults for StreamLog.

Every setting can be overridden via environment variables prefixed with
``STREAMLOG_`` (for example, ``STREAMLOG_COORDINATOR_PORT=9100``).
"""

from __future__ import annotations

import os

# ─── Coordinator ─────────────────────────────────────────────────────────────
COORDINATOR_HOST: str = os.getenv("STREAMLOG_COORDINATOR_HOST", "127.0.0.1")
COORDINATOR_PORT: int = int(os.getenv("STREAMLOG_COORDINATOR_PORT", "9000"))

# ─── Broker defaults ─────────────────────────────────────────────────────────
BROKER_BASE_PORT: int = int(os.getenv("STREAMLOG_BROKER_BASE_PORT", "9001"))
DATA_DIR: str = os.getenv("STREAMLOG_DATA_DIR", "data")
SEGMENT_SIZE_BYTES: int = int(
    os.getenv("STREAMLOG_SEGMENT_SIZE_BYTES", str(1024 * 1024))
)  # 1 MiB per log segment

# ─── Heartbeat & failure detection ───────────────────────────────────────────
BROKER_HEARTBEAT_INTERVAL: float = float(
    os.getenv("STREAMLOG_BROKER_HEARTBEAT_INTERVAL", "3")
)  # seconds between broker -> coordinator heartbeats
BROKER_TIMEOUT: float = float(
    os.getenv("STREAMLOG_BROKER_TIMEOUT", "10")
)  # seconds without heartbeat before replica promotion

CONSUMER_HEARTBEAT_INTERVAL: float = float(
    os.getenv("STREAMLOG_CONSUMER_HEARTBEAT_INTERVAL", "5")
)  # seconds between consumer -> coordinator heartbeats
CONSUMER_TIMEOUT: float = float(
    os.getenv("STREAMLOG_CONSUMER_TIMEOUT", "15")
)  # seconds without heartbeat before consumer eviction

# ─── Producer retry policy ───────────────────────────────────────────────────
PRODUCER_MAX_RETRIES: int = int(os.getenv("STREAMLOG_PRODUCER_MAX_RETRIES", "5"))
PRODUCER_RETRY_BASE_S: float = float(
    os.getenv("STREAMLOG_PRODUCER_RETRY_BASE_S", "0.2")
)  # initial back-off (doubles each retry)

# ─── Consumer poll ───────────────────────────────────────────────────────────
CONSUMER_MAX_POLL_RECORDS: int = int(
    os.getenv("STREAMLOG_CONSUMER_MAX_POLL_RECORDS", "100")
)
