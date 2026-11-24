"""
streamlog.common.models
=======================
Shared message-type constants and response factories used across all nodes.
"""

from __future__ import annotations

# ─── Coordinator request types ────────────────────────────────────────────────
REGISTER_BROKER = "REGISTER_BROKER"
CREATE_TOPIC = "CREATE_TOPIC"
GET_PARTITION_INFO = "GET_PARTITION_INFO"  # producer: which broker owns a partition?
LIST_TOPICS = "LIST_TOPICS"

SUBSCRIBE = "SUBSCRIBE"  # consumer joins a group
UNSUBSCRIBE = "UNSUBSCRIBE"
GET_ASSIGNMENT = "GET_ASSIGNMENT"  # consumer polls its current partition list
GET_OFFSET = "GET_OFFSET"  # consumer fetches last committed offset
COMMIT_OFFSET = "COMMIT_OFFSET"

BROKER_HEARTBEAT = "BROKER_HEARTBEAT"
CONSUMER_HEARTBEAT = "CONSUMER_HEARTBEAT"

# ─── Broker request types ────────────────────────────────────────────────────
PRODUCE = "PRODUCE"
FETCH = "FETCH"
REPLICATE = "REPLICATE"  # primary -> replica replication payload

# ─── Generic response fields ─────────────────────────────────────────────────
STATUS_OK = "ok"
STATUS_ERROR = "error"


def ok(payload: dict | None = None) -> dict:
    """Convenience factory for a success response."""
    resp: dict = {"status": STATUS_OK}
    if payload:
        resp.update(payload)
    return resp


def error(reason: str) -> dict:
    """Convenience factory for an error response."""
    return {"status": STATUS_ERROR, "reason": reason}
