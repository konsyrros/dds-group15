"""
orchestrator/models.py
======================
Pure data models for the orchestrator library.
No Redis, no HTTP, no business logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from msgspec import Struct

# ─────────────────────────────────────────────────────────────────
# Workflow lifecycle states
# ─────────────────────────────────────────────────────────────────
#
#  created ──► preparing ──► committing ──► completed
#                   │                           (terminal)
#                   ▼
#             rolling_back ──► rolled_back
#                                  (terminal)
#                   │
#                   ▼ (unrecoverable)
#                 failed
#                  (terminal)

STATE_CREATED       = "created"
STATE_PREPARING     = "preparing"       # executing forward steps
STATE_COMMITTING    = "committing"      # 2PC: sending commits
STATE_ROLLING_BACK  = "rolling_back"    # compensating / aborting
STATE_COMPLETED     = "completed"       # terminal — all steps done
STATE_ROLLED_BACK   = "rolled_back"     # terminal — compensated cleanly
STATE_FAILED        = "failed"          # terminal — compensation itself failed

TERMINAL_STATES = {STATE_COMPLETED, STATE_ROLLED_BACK, STATE_FAILED}

# ─────────────────────────────────────────────────────────────────
# Individual step states
# ─────────────────────────────────────────────────────────────────

STEP_PENDING     = "pending"
STEP_EXECUTED    = "executed"    # forward action completed
STEP_COMMITTED   = "committed"   # 2PC commit completed
STEP_ROLLEDBACK  = "rolledback"  # compensation / abort completed

# ─────────────────────────────────────────────────────────────────
# Context type alias
# ─────────────────────────────────────────────────────────────────
# The caller passes a plain dict as context.  The engine treats it
# as opaque: it serialises it to JSON and stores it alongside the
# workflow record.  Steps receive the dict and may read/write it.

Context = dict


# ─────────────────────────────────────────────────────────────────
# StepFailed — the only exception the engine handles specially
# ─────────────────────────────────────────────────────────────────

class StepFailed(Exception):
    """
    Raise this inside any Step callable to signal a recoverable failure.
    The engine will trigger rollback / abort for all completed steps.

    Any other exception propagates up and is treated as an unexpected
    error (the workflow enters STATE_FAILED without rolling back).
    """
    def __init__(self, reason: str, status_code: int = 400):
        super().__init__(reason)
        self.reason      = reason
        self.status_code = status_code


# ─────────────────────────────────────────────────────────────────
# Step — the caller defines one per unit of work
# ─────────────────────────────────────────────────────────────────

@dataclass
class Step:
    """
    A single unit of work in a workflow.

    Parameters
    ----------
    name:
        Unique identifier within the workflow. Used as the key in
        step_states and in log messages.  Must be stable across
        restarts (do not use UUIDs here).

    execute:
        Forward action.  Called during the PREPARING phase.
        In SAGA mode this also permanently commits the change.
        In 2PC mode this is the PREPARE (vote) phase — do not
        permanently commit yet; just validate and reserve.
        Raise StepFailed on any business-level error.

    rollback:
        Compensating action (SAGA) or ABORT action (2PC).
        Called when a later step fails, in reverse order.
        Must be idempotent: the engine may call it more than once
        if it crashes and recovers mid-rollback.

    commit:
        2PC COMMIT action.  Only called in 2PC mode, after ALL
        steps have voted YES.  Leave as None for SAGA workflows.
        Must be idempotent.
    """
    name:     str
    execute:  Callable[[Context], None]
    rollback: Callable[[Context], None]
    commit:   Callable[[Context], None] | None = None


# ─────────────────────────────────────────────────────────────────
# WorkflowRecord — persisted to Redis; engine-internal
# ─────────────────────────────────────────────────────────────────

class WorkflowRecord(Struct):
    """
    Generic workflow state persisted by the engine.
    Contains NO business-domain fields — only orchestration metadata.

    context_json:
        The caller's Context dict, serialised as a JSON string.
        The engine deserialises it back to a dict before passing it
        to Step callables and re-serialises after each step so that
        steps can propagate data to subsequent steps via the context.

    step_names:
        Ordered list of step names, mirroring the Step list the
        caller passed to WorkflowEngine.  Stored so the recovery
        thread can verify the step list hasn't changed between
        restarts (future: version checking).

    step_states:
        Maps step_name → STEP_* constant.  Updated after every
        individual step so a crash always leaves a consistent view.
    """
    workflow_id:  str
    mode:         str                   # "SAGA" | "2PC"
    state:        str                   # STATE_* constant
    context_json: str                   # JSON-encoded caller Context
    step_names:   list[str]
    step_states:  dict[str, str]        # step_name → STEP_*
    error:        str = ""