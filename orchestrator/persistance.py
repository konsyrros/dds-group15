"""
orchestrator/persistence.py
============================
All Redis I/O for workflow records.
No business logic, no HTTP, no domain knowledge.
"""

from __future__ import annotations

import logging

import redis
from msgspec import msgpack

from .models import WorkflowRecord

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Key helpers
# ─────────────────────────────────────────────────────────────────

def _workflow_key(workflow_id: str) -> str:
    return f"orchestrator:workflow:{workflow_id}"


# ─────────────────────────────────────────────────────────────────
# CRUD
# ─────────────────────────────────────────────────────────────────

def save_workflow(db: redis.Redis, record: WorkflowRecord) -> None:
    """
    Persist a WorkflowRecord to Redis.
    Raises redis.exceptions.RedisError on failure — caller decides how to handle.
    """
    key   = _workflow_key(record.workflow_id)
    value = msgpack.encode(record)
    db.set(key, value)
    logger.debug("[persistence] saved workflow_id=%s state=%s", record.workflow_id, record.state)


def load_workflow(db: redis.Redis, workflow_id: str) -> WorkflowRecord | None:
    """
    Load a WorkflowRecord from Redis.
    Returns None if the key does not exist.
    Raises redis.exceptions.RedisError on I/O failure.
    """
    raw = db.get(_workflow_key(workflow_id))
    if raw is None:
        return None
    record = msgpack.decode(raw, type=WorkflowRecord)
    logger.debug("[persistence] loaded workflow_id=%s state=%s", record.workflow_id, record.state)
    return record


def list_active_workflows(db: redis.Redis) -> list[WorkflowRecord]:
    """
    Scan all orchestrator:workflow:* keys and return records that are
    not yet in a terminal state.  Used by the recovery thread.

    This is O(n) over all workflow keys — acceptable because:
      - The scan is done in the background, not on the hot path.
      - Completed workflows should be pruned (see prune_workflow).
    """
    from .models import TERMINAL_STATES  # local import avoids circular

    active = []
    try:
        for key in db.scan_iter("orchestrator:workflow:*"):
            raw = db.get(key)
            if not raw:
                continue
            record = msgpack.decode(raw, type=WorkflowRecord)
            if record.state not in TERMINAL_STATES:
                active.append(record)
    except redis.exceptions.RedisError as exc:
        logger.warning("[persistence] scan failed during list_active_workflows: %s", exc)
    return active


def prune_workflow(db: redis.Redis, workflow_id: str) -> None:
    """
    Delete a completed workflow record from Redis.
    Call this after the caller has acted on the terminal state
    (e.g. marked the order as paid) to keep Redis tidy.
    Optional — leaving records is harmless but wastes memory.
    """
    db.delete(_workflow_key(workflow_id))
    logger.debug("[persistence] pruned workflow_id=%s", workflow_id)