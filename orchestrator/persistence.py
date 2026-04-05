from __future__ import annotations

import logging

import redis
from msgspec import msgpack

from .models import WorkflowRecord

logger = logging.getLogger(__name__)


def _workflow_key(workflow_id: str) -> str:
    return f"orchestrator:workflow:{workflow_id}"


def save_workflow(db: redis.Redis, record: WorkflowRecord) -> None:
    """ Save workflow to Redis. """
    key = _workflow_key(record.workflow_id)
    value = msgpack.encode(record)
    db.set(key, value)
    logger.debug("[persistence] saved workflow_id=%s state=%s", record.workflow_id, record.state)


def load_workflow(db: redis.Redis, workflow_id: str) -> WorkflowRecord | None:
    """ Load workflow record from Redis. """
    raw = db.get(_workflow_key(workflow_id))
    if raw is None:
        return None
    record = msgpack.decode(raw, type=WorkflowRecord)
    logger.debug("[persistence] loaded workflow_id=%s state=%s", record.workflow_id, record.state)
    return record


def list_active_workflows(db: redis.Redis) -> list[WorkflowRecord]:
    """ Search workflows not in terminal sate, used by recovery procedure. """
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
    """ Delete a completed workflow record from Redis. """
    db.delete(_workflow_key(workflow_id))
    logger.debug("[persistence] pruned workflow_id=%s", workflow_id)
