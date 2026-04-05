"""
orchestrator
============
A generic, Redis-backed distributed-transaction executor.

Public API
----------
::

    from orchestrator import WorkflowEngine, Step, StepFailed
    from orchestrator import (
        STATE_COMPLETED, STATE_ROLLED_BACK, STATE_FAILED,
        start_recovery_thread,
    )

Everything else is considered internal.
"""

from .engine   import WorkflowEngine          # noqa: F401
from .models   import (                       # noqa: F401
    Step,
    StepFailed,
    WorkflowRecord,
    STATE_COMPLETED,
    STATE_ROLLED_BACK,
    STATE_FAILED,
    STATE_CREATED,
    STATE_PREPARING,
    STATE_COMMITTING,
    STATE_ROLLING_BACK,
    TERMINAL_STATES,
    STEP_PENDING,
    STEP_EXECUTED,
    STEP_COMMITTED,
    STEP_ROLLEDBACK,
)
from .recovery import start_recovery_thread   # noqa: F401
from .queue import RedisCommandQueue, queued_json, queued_text  # noqa: F401

__all__ = [
    "WorkflowEngine",
    "Step",
    "StepFailed",
    "WorkflowRecord",
    "start_recovery_thread",
    "RedisCommandQueue",
    "queued_json",
    "queued_text",
    # state constants
    "STATE_COMPLETED",
    "STATE_ROLLED_BACK",
    "STATE_FAILED",
    "STATE_CREATED",
    "STATE_PREPARING",
    "STATE_COMMITTING",
    "STATE_ROLLING_BACK",
    "TERMINAL_STATES",
    # step state constants
    "STEP_PENDING",
    "STEP_EXECUTED",
    "STEP_COMMITTED",
    "STEP_ROLLEDBACK",
]
