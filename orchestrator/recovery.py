"""
orchestrator/recovery.py
=========================
Background recovery thread.

Periodically scans Redis for workflows that are not yet in a terminal
state and drives them to completion via WorkflowEngine.resume().

This handles the case where the orchestrator process crashed in the
middle of a workflow — on restart the thread picks up where it left off.
"""

from __future__ import annotations

import logging
import threading
import time

import redis

logger = logging.getLogger(__name__)


def start_recovery_thread(
    engine,                        # WorkflowEngine — avoid circular import with string hint
    db:               redis.Redis,
    interval_seconds: float = 5.0,
    daemon:           bool  = True,
) -> threading.Thread:
    """
    Start and return a background thread that periodically scans for
    non-terminal workflows and resumes them.

    Parameters
    ----------
    engine:
        A WorkflowEngine instance.  The thread calls engine.resume()
        for each non-terminal workflow it finds.

    db:
        Redis client used for scanning.  Can be the same client the
        engine uses.

    interval_seconds:
        How often to scan.  5 seconds is a reasonable default.

    daemon:
        If True (default) the thread does not prevent the process
        from exiting.

    Returns
    -------
    The started threading.Thread.  You do not need to keep a reference
    to it unless you want to join() on shutdown.
    """
    from .persistence import list_active_workflows  # local to avoid circular import

    def _loop():
        logger.info("[recovery] thread started  interval=%.1fs", interval_seconds)
        while True:
            try:
                _scan_and_resume(engine, db, list_active_workflows)
            except Exception as exc:
                logger.error("[recovery] unexpected error in scan loop: %s", exc)
            time.sleep(interval_seconds)

    t = threading.Thread(target=_loop, name="orchestrator-recovery", daemon=daemon)
    t.start()
    return t


def _scan_and_resume(engine, db, list_active_workflows_fn) -> None:
    """Scan for non-terminal workflows and resume each one."""
    try:
        active = list_active_workflows_fn(db)
    except redis.exceptions.RedisError as exc:
        logger.warning("[recovery] scan failed: %s", exc)
        return

    if not active:
        return

    logger.info("[recovery] found %d non-terminal workflow(s) — resuming", len(active))

    for record in active:
        try:
            logger.info("[recovery] resuming workflow_id=%s  state=%s",
                        record.workflow_id, record.state)
            engine.resume(record.workflow_id)
        except Exception as exc:
            logger.warning("[recovery] failed to resume workflow_id=%s: %s",
                           record.workflow_id, exc)