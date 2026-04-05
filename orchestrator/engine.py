"""
orchestrator/engine.py
=======================
WorkflowEngine — the only class callers need.

The engine is completely agnostic about what the steps do.
It only knows:
  - Execute steps in order.
  - If a step raises StepFailed → roll back completed steps in reverse.
  - In 2PC: first run all execute() (prepare/vote), then run all commit().
  - Persist state after every individual step so a crash always leaves
    a recoverable snapshot.
  - On resume after a crash: skip steps that are already done.
"""

from __future__ import annotations

import json
import logging
from typing import Sequence

import redis

from . import lock as lock_module
from .models import (
    STEP_COMMITTED,
    STEP_EXECUTED,
    STEP_PENDING,
    STEP_ROLLEDBACK,
    STATE_COMMITTING,
    STATE_COMPLETED,
    STATE_CREATED,
    STATE_FAILED,
    STATE_PREPARING,
    STATE_ROLLED_BACK,
    STATE_ROLLING_BACK,
    TERMINAL_STATES,
    Context,
    Step,
    StepFailed,
    WorkflowRecord,
)
from .persistence import load_workflow, save_workflow

logger = logging.getLogger(__name__)

MODE_SAGA = "SAGA"
MODE_2PC  = "2PC"

# How long the distributed lock is held while driving a workflow.
# If the process dies the lock auto-expires and the recovery thread
# can re-acquire it.
DEFAULT_LOCK_TTL_SECONDS = 120


class WorkflowEngine:
    """
    Generic distributed-transaction executor.

    Parameters
    ----------
    db:
        A connected redis.Redis client.  The engine uses it for
        workflow persistence and distributed locking.

    steps:
        Ordered list of Step objects defined by the caller.
        The order is significant: steps execute left-to-right and
        roll back right-to-left.

    mode:
        "SAGA" or "2PC" (case-insensitive).

    lock_ttl_seconds:
        How long (in seconds) the distributed lock is held while a
        workflow is being driven.  Should be larger than the expected
        wall-clock time of the longest workflow.

    Example
    -------
    ::

        engine = WorkflowEngine(db, steps=[step_stock, step_payment], mode="SAGA")
        record = engine.start("wf-abc123", context={"user_id": "u1", ...})
        if record.state == STATE_COMPLETED:
            mark_order_paid(order_id)
    """

    def __init__(
        self,
        db:               redis.Redis,
        steps:            Sequence[Step],
        mode:             str = MODE_SAGA,
        lock_ttl_seconds: int = DEFAULT_LOCK_TTL_SECONDS,
    ):
        if not steps:
            raise ValueError("WorkflowEngine requires at least one Step")
        mode = mode.upper()
        if mode not in (MODE_SAGA, MODE_2PC):
            raise ValueError(f"mode must be 'SAGA' or '2PC', got {mode!r}")
        if mode == MODE_2PC:
            missing = [s.name for s in steps if s.commit is None]
            if missing:
                raise ValueError(
                    f"2PC mode requires a commit callable on every Step. "
                    f"Missing on: {missing}"
                )

        self._db      = db
        self._steps   = list(steps)
        self._mode    = mode
        self._lock_ttl = lock_ttl_seconds
        self._step_map = {s.name: s for s in steps}

    # ─────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────

    def start(self, workflow_id: str, context: Context) -> WorkflowRecord:
        """
        Create and immediately drive a new workflow.

        If a record with *workflow_id* already exists (idempotent
        restart / duplicate request) the existing record is resumed
        instead of creating a duplicate.

        Returns the final WorkflowRecord (always in a terminal state
        unless an unexpected exception escapes from a Step callable).
        """
        existing = load_workflow(self._db, workflow_id)
        if existing is not None:
            logger.info("[engine] workflow_id=%s already exists — resuming", workflow_id)
            return self.resume(workflow_id)

        record = WorkflowRecord(
            workflow_id  = workflow_id,
            mode         = self._mode,
            state        = STATE_CREATED,
            context_json = json.dumps(context),
            step_names   = [s.name for s in self._steps],
            step_states  = {s.name: STEP_PENDING for s in self._steps},
        )
        save_workflow(self._db, record)
        logger.info("[engine] created workflow_id=%s mode=%s steps=%s",
                    workflow_id, self._mode, record.step_names)
        return self._drive(record)

    def resume(self, workflow_id: str) -> WorkflowRecord:
        """
        Re-drive a non-terminal workflow.

        Called by the recovery thread and by any explicit /resume
        endpoint.  Safe to call on a terminal workflow (returns
        immediately without doing anything).
        """
        record = load_workflow(self._db, workflow_id)
        if record is None:
            raise KeyError(f"Workflow not found: {workflow_id}")
        if record.state in TERMINAL_STATES:
            logger.info("[engine] resume no-op: workflow_id=%s is already terminal (%s)",
                        workflow_id, record.state)
            return record
        logger.info("[engine] resuming workflow_id=%s from state=%s", workflow_id, record.state)
        return self._drive(record)

    def get(self, workflow_id: str) -> WorkflowRecord | None:
        """Return the current WorkflowRecord without driving it."""
        return load_workflow(self._db, workflow_id)

    # ─────────────────────────────────────────────────────────────
    # Internal driver — acquires lock then dispatches to SAGA / 2PC
    # ─────────────────────────────────────────────────────────────

    def _drive(self, record: WorkflowRecord) -> WorkflowRecord:
        """
        Acquire the distributed lock for this workflow, drive it to a
        terminal state, then release the lock.

        If the lock is already held (another process is driving this
        workflow concurrently) we reload and return the current state
        without blocking — the other process will finish eventually
        and the recovery thread will pick it up if it crashes.
        """
        token = lock_module.acquire(self._db, record.workflow_id, self._lock_ttl)
        if token is None:
            # Another process is driving — reload current state and return.
            fresh = load_workflow(self._db, record.workflow_id)
            return fresh if fresh is not None else record

        try:
            # Reload after acquiring the lock — state may have changed
            # between our load and the lock acquisition.
            record = load_workflow(self._db, record.workflow_id)
            if record is None or record.state in TERMINAL_STATES:
                return record

            if record.mode == MODE_SAGA:
                return self._drive_saga(record)
            else:
                return self._drive_2pc(record)

        except StepFailed:
            # StepFailed should always be caught inside _drive_saga / _drive_2pc.
            # If it bubbles up here something went wrong in the rollback itself.
            logger.exception("[engine] unexpected StepFailed outside driver  workflow_id=%s",
                             record.workflow_id)
            record.state = STATE_FAILED
            record.error = "Unexpected StepFailed during rollback"
            save_workflow(self._db, record)
            return record

        except Exception as exc:
            logger.exception("[engine] unexpected error  workflow_id=%s", record.workflow_id)
            record.state = STATE_FAILED
            record.error = str(exc)
            save_workflow(self._db, record)
            return record

        finally:
            lock_module.release(self._db, record.workflow_id, token)

    # ─────────────────────────────────────────────────────────────
    # SAGA driver
    # ─────────────────────────────────────────────────────────────

    def _drive_saga(self, record: WorkflowRecord) -> WorkflowRecord:
        """
        Execute all steps sequentially.
        On first failure: roll back all completed steps in reverse order.

        SAGA has no separate commit phase — each execute() permanently
        commits its local transaction immediately.

        Crash-safe: we persist after every step.  On resume, steps
        already in STEP_EXECUTED are skipped.
        """
        logger.info("[engine][SAGA] starting  workflow_id=%s", record.workflow_id)
        record.state = STATE_PREPARING
        save_workflow(self._db, record)

        ctx       = self._load_context(record)
        completed : list[Step] = []

        for step in self._steps:
            state = record.step_states.get(step.name, STEP_PENDING)

            if state == STEP_EXECUTED:
                # Already done (resumed after crash)
                logger.info("[engine][SAGA] skip (already executed)  step=%s", step.name)
                completed.append(step)
                continue

            if state == STEP_ROLLEDBACK:
                # We were mid-rollback when we crashed — skip forward steps
                logger.info("[engine][SAGA] skip (already rolled back)  step=%s", step.name)
                continue

            logger.info("[engine][SAGA] execute  step=%s  workflow_id=%s",
                        step.name, record.workflow_id)
            try:
                step.execute(ctx)
                self._save_context(record, ctx)
                record.step_states[step.name] = STEP_EXECUTED
                save_workflow(self._db, record)
                completed.append(step)
                logger.info("[engine][SAGA] executed OK  step=%s", step.name)

            except StepFailed as exc:
                logger.warning("[engine][SAGA] step FAILED  step=%s  reason=%s",
                               step.name, exc.reason)
                return self._rollback(record, ctx, completed, reason=exc.reason)

        record.state = STATE_COMPLETED
        save_workflow(self._db, record)
        logger.info("[engine][SAGA] completed  workflow_id=%s", record.workflow_id)
        return record

    # ─────────────────────────────────────────────────────────────
    # 2PC driver
    # ─────────────────────────────────────────────────────────────

    def _drive_2pc(self, record: WorkflowRecord) -> WorkflowRecord:
        """
        Phase 1 — PREPARE: run execute() on every step.
                  Any failure → abort all prepared steps.
        Phase 2 — COMMIT:  run commit() on every prepared step.
                  Commit failures abort the affected step and fail
                  the workflow (commit should rarely fail if prepare
                  was correctly implemented).

        Crash-safe: persisted after every individual step.
        On resume: skips steps already in STEP_EXECUTED or STEP_COMMITTED.
        """
        logger.info("[engine][2PC] starting  workflow_id=%s", record.workflow_id)

        ctx = self._load_context(record)

        # ── Phase 1: PREPARE ──────────────────────────────────────
        if record.state not in (STATE_COMMITTING,):
            record.state = STATE_PREPARING
            save_workflow(self._db, record)

            prepared: list[Step] = []

            for step in self._steps:
                state = record.step_states.get(step.name, STEP_PENDING)

                if state in (STEP_EXECUTED, STEP_COMMITTED):
                    logger.info("[engine][2PC] prepare skip (already done)  step=%s", step.name)
                    prepared.append(step)
                    continue

                if state == STEP_ROLLEDBACK:
                    logger.info("[engine][2PC] prepare skip (already aborted)  step=%s", step.name)
                    continue

                logger.info("[engine][2PC] prepare  step=%s  workflow_id=%s",
                            step.name, record.workflow_id)
                try:
                    step.execute(ctx)
                    self._save_context(record, ctx)
                    record.step_states[step.name] = STEP_EXECUTED
                    save_workflow(self._db, record)
                    prepared.append(step)
                    logger.info("[engine][2PC] prepared OK  step=%s", step.name)

                except StepFailed as exc:
                    logger.warning("[engine][2PC] prepare FAILED  step=%s  reason=%s",
                                   step.name, exc.reason)
                    return self._rollback(record, ctx, prepared, reason=exc.reason)

            record.state = STATE_COMMITTING
            save_workflow(self._db, record)

        # ── Phase 2: COMMIT ───────────────────────────────────────
        logger.info("[engine][2PC] committing  workflow_id=%s", record.workflow_id)

        for step in self._steps:
            state = record.step_states.get(step.name, STEP_PENDING)

            if state == STEP_COMMITTED:
                logger.info("[engine][2PC] commit skip (already committed)  step=%s", step.name)
                continue

            if state != STEP_EXECUTED:
                # Was never prepared (e.g. it was rolled back in a previous attempt)
                continue

            logger.info("[engine][2PC] commit  step=%s  workflow_id=%s",
                        step.name, record.workflow_id)
            try:
                step.commit(ctx)   # type: ignore[misc]  — guaranteed non-None in 2PC mode
                self._save_context(record, ctx)
                record.step_states[step.name] = STEP_COMMITTED
                save_workflow(self._db, record)
                logger.info("[engine][2PC] committed OK  step=%s", step.name)

            except StepFailed as exc:
                # A commit failure is serious — we roll back what we can
                # but the workflow ends in STATE_FAILED (not ROLLED_BACK)
                # because partial commit is not cleanly reversible.
                logger.error("[engine][2PC] commit FAILED  step=%s  reason=%s",
                             step.name, exc.reason)
                record.state = STATE_FAILED
                record.error = f"Commit failed on step '{step.name}': {exc.reason}"
                save_workflow(self._db, record)
                return record

        record.state = STATE_COMPLETED
        save_workflow(self._db, record)
        logger.info("[engine][2PC] completed  workflow_id=%s", record.workflow_id)
        return record

    # ─────────────────────────────────────────────────────────────
    # Shared rollback
    # ─────────────────────────────────────────────────────────────

    def _rollback(
        self,
        record:    WorkflowRecord,
        ctx:       Context,
        completed: list[Step],
        reason:    str,
    ) -> WorkflowRecord:
        """
        Call rollback() on every completed step in reverse order.
        Persists state after each rollback so a crash mid-rollback
        is safely resumable.

        If a rollback callable itself raises, we log the error and
        continue with the remaining rollbacks (best-effort) rather
        than leaving other resources locked indefinitely.
        """
        logger.warning("[engine] rolling back  workflow_id=%s  reason=%s",
                       record.workflow_id, reason)
        record.state = STATE_ROLLING_BACK
        record.error = reason
        save_workflow(self._db, record)

        for step in reversed(completed):
            if record.step_states.get(step.name) == STEP_ROLLEDBACK:
                logger.info("[engine] rollback skip (already done)  step=%s", step.name)
                continue

            logger.info("[engine] rollback  step=%s", step.name)
            try:
                step.rollback(ctx)
                self._save_context(record, ctx)
                record.step_states[step.name] = STEP_ROLLEDBACK
                save_workflow(self._db, record)
                logger.info("[engine] rolled back OK  step=%s", step.name)

            except Exception as rb_exc:
                # Rollback failure — log and continue; do not re-raise
                logger.error("[engine] rollback FAILED  step=%s  error=%s",
                             step.name, rb_exc)
                record.step_states[step.name] = STEP_ROLLEDBACK   # mark anyway
                record.error += f" | rollback error on '{step.name}': {rb_exc}"
                save_workflow(self._db, record)

        record.state = STATE_ROLLED_BACK
        save_workflow(self._db, record)
        logger.info("[engine] rolled back  workflow_id=%s", record.workflow_id)
        return record

    # ─────────────────────────────────────────────────────────────
    # Context serialisation helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _load_context(record: WorkflowRecord) -> Context:
        return json.loads(record.context_json)

    @staticmethod
    def _save_context(record: WorkflowRecord, ctx: Context) -> None:
        record.context_json = json.dumps(ctx)