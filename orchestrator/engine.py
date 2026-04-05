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

# Maximum lock duration
DEFAULT_LOCK_TTL_SECONDS = 120


class WorkflowEngine:
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


    def start(self, workflow_id: str, context: Context) -> WorkflowRecord:
        """ Start (or idempotently continue) a workflow with the given ID and context. """
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
        """ Continue an existing workflow not yet completed, used by recovery flow. """
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
    

    def _drive(self, record: WorkflowRecord) -> WorkflowRecord:
        """ Acquires lock for the resources, runs the workflow with the appropriate protocol, and releases the lock. """
        token = lock_module.acquire(self._db, record.workflow_id, self._lock_ttl)
        if token is None:
            # Another process is blocking the resources, return the current state.
            fresh = load_workflow(self._db, record.workflow_id)
            return fresh if fresh is not None else record

        try:
            # Refresh the record
            record = load_workflow(self._db, record.workflow_id)
            if record is None or record.state in TERMINAL_STATES:
                return record

            if record.mode == MODE_SAGA:
                return self._drive_saga(record)
            else:
                return self._drive_2pc(record)

        except StepFailed:
            # StepFailed should always be caught inside protocol drivers
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


    def _drive_saga(self, record: WorkflowRecord) -> WorkflowRecord:
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
    

    def _drive_2pc(self, record: WorkflowRecord) -> WorkflowRecord:
        logger.info("[engine][2PC] starting  workflow_id=%s", record.workflow_id)

        ctx = self._load_context(record)

        # PREPARE phase
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

        # COMMIT phase
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
                step.commit(ctx)   # type: ignore[misc] - guaranteed non-None in 2PC mode
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
    

    def _rollback(
        self,
        record:    WorkflowRecord,
        ctx:       Context,
        completed: list[Step],
        reason:    str,
    ) -> WorkflowRecord:
        """ Roll back all completed steps in reverse order. """
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


    # Serialization utilities
    @staticmethod
    def _load_context(record: WorkflowRecord) -> Context:
        return json.loads(record.context_json)

    @staticmethod
    def _save_context(record: WorkflowRecord, ctx: Context) -> None:
        record.context_json = json.dumps(ctx)