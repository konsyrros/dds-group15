"""
orchestrator/lock.py
====================
Lightweight distributed lock backed by Redis.

Uses the standard SET NX EX pattern for acquisition and a Lua
script for safe release (only deletes the key if the token matches,
so a lock whose TTL expired before we released it cannot accidentally
release a lock held by a different caller).
"""

from __future__ import annotations

import logging
import uuid

import redis

logger = logging.getLogger(__name__)

# Lua script: delete key only if its value matches our token.
# Returns 1 if deleted, 0 if not (someone else holds the lock or it expired).
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


def _lock_key(workflow_id: str) -> str:
    return f"orchestrator:lock:{workflow_id}"


def acquire(db: redis.Redis, workflow_id: str, ttl_seconds: int) -> str | None:
    """
    Try to acquire the lock for *workflow_id*.

    Returns a non-empty token string on success (keep it to release later).
    Returns None if the lock is already held by another caller.

    The lock auto-expires after *ttl_seconds* so a crashed process
    cannot hold it forever.  Set ttl_seconds to comfortably exceed
    your longest expected workflow duration.
    """
    token = uuid.uuid4().hex
    acquired = db.set(_lock_key(workflow_id), token, nx=True, ex=ttl_seconds)
    if acquired:
        logger.debug("[lock] acquired  workflow_id=%s  token=%s  ttl=%ds",
                     workflow_id, token[:8], ttl_seconds)
        return token
    logger.debug("[lock] contention workflow_id=%s — already locked", workflow_id)
    return None


def release(db: redis.Redis, workflow_id: str, token: str) -> bool:
    """
    Release the lock for *workflow_id* only if we still own it.

    Returns True if the lock was released, False if it had already
    expired or been claimed by another caller (both safe outcomes).
    """
    result = db.eval(_RELEASE_SCRIPT, 1, _lock_key(workflow_id), token)
    released = bool(result)
    if released:
        logger.debug("[lock] released  workflow_id=%s  token=%s", workflow_id, token[:8])
    else:
        logger.warning("[lock] release no-op  workflow_id=%s  token=%s (expired or stolen)",
                       workflow_id, token[:8])
    return released