from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from msgspec import Struct

# Workflow states
STATE_CREATED       = "created"
STATE_PREPARING     = "preparing"
STATE_COMMITTING    = "committing" # 2PC only
STATE_ROLLING_BACK  = "rolling_back"
STATE_COMPLETED     = "completed"
STATE_ROLLED_BACK   = "rolled_back" # workflow failed, successful compensation
STATE_FAILED        = "failed" # workflow failed, compensation failed

TERMINAL_STATES = {STATE_COMPLETED, STATE_ROLLED_BACK, STATE_FAILED}

# Step states
STEP_PENDING     = "pending"
STEP_EXECUTED    = "executed"
STEP_COMMITTED   = "committed" # 2PC only
STEP_ROLLEDBACK  = "rolledback"

Context = dict


class StepFailed(Exception):
    """ Indicates failure which can trigger a rollback. Other exceptions are unrecoverable and are raised fully. """
    def __init__(self, reason: str, status_code: int = 400):
        super().__init__(reason)
        self.reason      = reason
        self.status_code = status_code


# ─────────────────────────────────────────────────────────────────
# Step — the caller defines one per unit of work
# ─────────────────────────────────────────────────────────────────

@dataclass
class Step:
    """ An action in the workflow, with its compensating action and 2PC commit action if needed. """
    name:     str
    execute:  Callable[[Context], None]
    rollback: Callable[[Context], None]
    commit:   Callable[[Context], None] | None = None


class WorkflowRecord(Struct):
    """ Workflow state record as stored in Redis. """
    workflow_id:  str
    mode:         str                   # "SAGA" | "2PC"
    state:        str                   # STATE_* constant
    context_json: str                   # JSON-encoded caller Context
    step_names:   list[str]
    step_states:  dict[str, str]        # step_name → STEP_*
    error:        str = ""