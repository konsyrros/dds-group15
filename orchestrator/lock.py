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
    """ Acquire lock for the given workflow. """
    token = uuid.uuid4().hex
    acquired = db.set(_lock_key(workflow_id), token, nx=True, ex=ttl_seconds)
    if acquired:
        logger.debug("[lock] acquired  workflow_id=%s  token=%s  ttl=%ds",
                     workflow_id, token[:8], ttl_seconds)
        return token
    logger.debug("[lock] contention workflow_id=%s — already locked", workflow_id)
    return None


def release(db: redis.Redis, workflow_id: str, token: str) -> bool:
    """ Release lock for the given workflow based on the token. """
    result = db.eval(_RELEASE_SCRIPT, 1, _lock_key(workflow_id), token)
    released = bool(result)
    if released:
        logger.debug("[lock] released  workflow_id=%s  token=%s", workflow_id, token[:8])
    else:
        logger.warning("[lock] release no-op  workflow_id=%s  token=%s (expired or stolen)",
                       workflow_id, token[:8])
    return released