# Orchestrator Library

A generic, Redis-backed distributed-transaction executor for Python microservices.

Supports both **SAGA** (compensating transactions) and **2PC** (Two-Phase Commit) protocols,
selectable at runtime via an environment variable. Provides crash recovery, distributed
locking, idempotent step execution, and a clean separation between orchestration
mechanics and business logic.

---

## Repository layout

```
orchestrator/               ← the reusable library (import this)
    __init__.py             ← public API surface
    models.py               ← Step, WorkflowRecord, state constants
    engine.py               ← WorkflowEngine — run / resume / recover
    persistence.py          ← Redis save / load / scan, no business logic
    lock.py                 ← distributed lock (SET NX EX + Lua release)
    recovery.py             ← background thread that resumes stuck workflows

order_service/
    app.py                  ← example consumer: checkout using the library
```

---

## Core concepts

### The problem

A checkout involves two independent services with separate databases:

```
Order → subtract stock  (Stock DB)
Order → charge payment  (Payment DB)
```

If stock succeeds but payment then fails, stock must be rolled back.
Since there is no shared database, we need a coordination protocol.

### What the library does

The library is a **task executor**. You give it an ordered list of `Step` objects.
Each `Step` has:

- `execute()` — the forward action
- `rollback()` — the compensating / abort action
- `commit()` — the 2PC commit action (required in 2PC mode only)

The engine calls them in order, persists state after every step, and handles
failure and recovery. It knows **nothing** about stock, payment, orders, or URLs.
All of that stays in your application code.

### What the library does NOT do

- It does not know what HTTP calls to make
- It does not know what "paid" means
- It does not know the difference between stock and payment
- It does not define step order — you do
- It does not call `mark_order_paid` — you do, after `engine.start()` returns

This is intentional. The library is a protocol engine, not a business-logic container.

---

## Public API

```python
from orchestrator import WorkflowEngine, Step, StepFailed, start_recovery_thread
from orchestrator import STATE_COMPLETED, STATE_ROLLED_BACK, STATE_FAILED
```

### `Step`

```python
@dataclass
class Step:
    name:     str                          # unique within the workflow
    execute:  Callable[[dict], None]       # forward action
    rollback: Callable[[dict], None]       # compensation / abort
    commit:   Callable[[dict], None] | None = None   # 2PC only
```

All three callables receive the **context dict** — a plain Python dict you
populate before calling `engine.start()`. Steps may read from and write to
it. The engine persists it to Redis after every step, so changes survive crashes.

Raise `StepFailed(reason)` inside any callable to signal a business-level
failure. The engine will trigger rollback for all completed steps and set the
workflow state to `STATE_ROLLED_BACK`. Any other exception is treated as an
unexpected error and sets `STATE_FAILED` without attempting rollback.

### `WorkflowEngine`

```python
engine = WorkflowEngine(
    db               = redis_client,   # redis.Redis instance
    steps            = [step1, step2], # ordered list of Step objects
    mode             = "SAGA",         # "SAGA" or "2PC" (case-insensitive)
    lock_ttl_seconds = 120,            # how long the distributed lock is held
)
```

```python
record = engine.start(workflow_id, context)
# workflow_id: str — a unique ID you generate (e.g. str(uuid.uuid4()))
# context:     dict — arbitrary data your steps need
# returns:     WorkflowRecord (always in a terminal state after start())
```

```python
record = engine.resume(workflow_id)
# Re-drive a non-terminal workflow. Called by the recovery thread
# and optionally by a /resume HTTP endpoint.
```

```python
record = engine.get(workflow_id)
# Return current state without driving. Returns None if not found.
```

### `WorkflowRecord` (returned by engine methods)

```python
record.workflow_id   # str
record.mode          # "SAGA" | "2PC"
record.state         # STATE_* constant — check this after engine.start()
record.step_states   # dict[step_name, STEP_*] — per-step progress
record.context_json  # JSON string of your context dict
record.error         # human-readable failure reason (empty on success)
```

### State constants

```python
# Workflow lifecycle
STATE_CREATED       = "created"
STATE_PREPARING     = "preparing"
STATE_COMMITTING    = "committing"    # 2PC only
STATE_ROLLING_BACK  = "rolling_back"
STATE_COMPLETED     = "completed"     # ← success
STATE_ROLLED_BACK   = "rolled_back"   # ← clean failure
STATE_FAILED        = "failed"        # ← unrecoverable

# Step lifecycle
STEP_PENDING    = "pending"
STEP_EXECUTED   = "executed"
STEP_COMMITTED  = "committed"   # 2PC only
STEP_ROLLEDBACK = "rolledback"
```

### `start_recovery_thread`

```python
thread = start_recovery_thread(
    engine           = engine,
    db               = redis_client,
    interval_seconds = 5.0,   # how often to scan for stuck workflows
    daemon           = True,
)
```

Starts a background thread that periodically scans Redis for non-terminal
workflows and calls `engine.resume()` on each one. Call this once at
application startup.

---

## SAGA mode

In SAGA mode each `execute()` call **permanently commits** its local
transaction immediately. There is no separate commit phase.

```
Step 1 execute() → committed to Stock DB immediately
Step 2 execute() → committed to Payment DB immediately
```

If Step 2 fails, the engine calls `rollback()` on Step 1 (in reverse order)
to undo the already-committed change.

```
Failure:
  Step 2 execute() → StepFailed
  Step 1 rollback() → stock restored
  workflow.state = STATE_ROLLED_BACK
```

**Consistency model:** eventual. There is a window between Step 1 committing
and Step 1's rollback completing where the data appears inconsistent. The
system always converges to a consistent state.

**Step order in SAGA matters for failure cost.** Put the step most likely to fail
first (e.g. payment) so cheaper rollbacks are avoided.

### SAGA example

```python
from orchestrator import WorkflowEngine, Step, StepFailed, STATE_COMPLETED
import requests

STOCK_URL   = "http://stock-service:5000"
PAYMENT_URL = "http://payment-service:5000"

def make_stock_step(tx_id, item_id, qty):
    def execute(ctx):
        r = requests.post(f"{STOCK_URL}/transactions/start/{tx_id}/{item_id}/{qty}")
        if r.status_code != 200:
            raise StepFailed(r.text)

    def rollback(ctx):
        requests.post(f"{STOCK_URL}/transactions/rollback/{tx_id}")

    return Step(name=f"stock:{item_id}", execute=execute, rollback=rollback)

def make_payment_step(workflow_id, user_id, total):
    def execute(ctx):
        r = requests.post(f"{PAYMENT_URL}/transactions/start/{workflow_id}/{user_id}/{total}")
        if r.status_code != 200:
            raise StepFailed(r.text)

    def rollback(ctx):
        requests.post(f"{PAYMENT_URL}/transactions/rollback/{workflow_id}")

    return Step(name="payment", execute=execute, rollback=rollback)

# In your checkout handler:
workflow_id = str(uuid.uuid4())
steps = [
    make_payment_step(workflow_id, user_id, total_cost),
    make_stock_step(workflow_id, item_id, quantity),
]
engine = WorkflowEngine(db, steps, mode="SAGA")
record = engine.start(workflow_id, context={"order_id": order_id})

if record.state == STATE_COMPLETED:
    mark_order_paid(order_id)   # your business logic, not the engine's
```

---

## 2PC mode

In 2PC mode `execute()` is the **PREPARE** phase: validate, reserve, and lock
— but do **not** permanently commit yet. The engine only calls `commit()` after
every step has successfully prepared. If any step fails during prepare, the engine
calls `rollback()` (= ABORT) on all prepared steps.

```
Phase 1 — PREPARE:
  Step 1 execute() → stock reserved, row locked
  Step 2 execute() → credit reserved, row locked

Phase 2 — COMMIT:
  Step 1 commit() → stock deducted
  Step 2 commit() → credit deducted
```

```
Failure in Phase 1:
  Step 2 execute() → StepFailed (insufficient credit)
  Step 1 rollback() → lock released, reservation cancelled
  workflow.state = STATE_ROLLED_BACK
```

**Consistency model:** strong. No intermediate state is ever visible to other
transactions because locks are held until all participants commit or abort.

**Trade-off:** lower throughput than SAGA under high concurrency because row
locks are held across a network round trip.

### 2PC example

```python
def make_stock_step_2pc(tx_id, item_id, qty):
    def execute(ctx):   # PREPARE: lock and validate
        r = requests.post(f"{STOCK_URL}/transactions/start/{tx_id}/{item_id}/{qty}")
        if r.status_code != 200:
            raise StepFailed(r.text)

    def rollback(ctx):  # ABORT: release lock
        requests.post(f"{STOCK_URL}/transactions/rollback/{tx_id}")

    def commit(ctx):    # COMMIT: permanently deduct
        r = requests.post(f"{STOCK_URL}/transactions/commit/{tx_id}")
        if r.status_code != 200:
            raise StepFailed(r.text)

    return Step(name=f"stock:{item_id}", execute=execute, rollback=rollback, commit=commit)

engine = WorkflowEngine(db, steps, mode="2PC")
record = engine.start(workflow_id, context)
```

**Important:** In 2PC mode every `Step` must have a non-None `commit` callable.
The engine raises `ValueError` at construction time if any step is missing one.

---

## Idempotency and crash safety

Every step is persisted to Redis immediately after it completes. The `step_states`
dict in `WorkflowRecord` maps step names to their current state. On resume after
a crash, the engine skips steps that are already in `STEP_EXECUTED` or
`STEP_COMMITTED`.

This means your `execute()`, `rollback()`, and `commit()` callables **must be
idempotent**. If the engine crashes after calling `execute()` but before
persisting `STEP_EXECUTED`, it will call `execute()` again on the next resume.
Your service should handle a duplicate call with the same `tx_id` gracefully
(return 200 without applying the operation twice).

The `context_json` field is also persisted after every step, so steps can pass
data to subsequent steps by writing to the `ctx` dict.

---

## Distributed lock

The engine acquires a per-workflow distributed lock (Redis `SET NX EX`) before
driving a workflow. This prevents two processes (e.g. two gunicorn workers or
the recovery thread and a fresh request) from driving the same workflow
concurrently.

The lock has a TTL (`lock_ttl_seconds`, default 120 s). If the lock-holding
process crashes, the lock expires automatically and another process can
acquire it. Set this to comfortably exceed your longest expected checkout
duration.

The lock is released using a Lua script that only deletes the key if the
token matches the one we set — so an expired-and-reacquired lock is never
accidentally released by the original holder after recovery.

---

## Recovery thread

Call `start_recovery_thread()` once at application startup. It runs a
background daemon thread that:

1. Calls `db.scan_iter("orchestrator:workflow:*")` every `interval_seconds`.
2. For each non-terminal workflow, calls `engine.resume(workflow_id)`.
3. Logs warnings for any workflows it cannot resume.

The recovery thread tolerates Redis being temporarily unavailable — it logs
a warning and retries on the next interval.

**Important:** The recovery thread receives a `WorkflowEngine` instance.
Because `Step` callables are Python functions that may close over
workflow-specific variables (like `order_id`), the recovery thread needs to
reconstruct the step list from the persisted context before calling
`engine.resume()`. See `order_service/app.py` → `_recover_workflow()` for
the recommended pattern: store all parameters your steps need in the context
dict, then reconstruct steps from context in the recovery handler.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `TX_MODE` | `SAGA` | Transaction mode: `SAGA` or `2PC` |
| `REDIS_HOST` | — | Redis hostname for orchestrator state |
| `REDIS_PORT` | — | Redis port |
| `REDIS_PASSWORD` | — | Redis password |
| `REDIS_DB` | — | Redis logical DB index |
| `WORKFLOW_LOCK_SECONDS` | `120` | Distributed lock TTL |
| `RECOVERY_SCAN_INTERVAL_SECONDS` | `5` | How often the recovery thread scans |
| `REQUEST_TIMEOUT_SECONDS` | `15` | HTTP timeout for step callables |

---

## How the old orchestrator maps to the new design

| Old code (business logic in orchestrator) | New code (where it lives) |
|---|---|
| `STOCK_SERVICE_URL`, `PAYMENT_SERVICE_URL` | `order_service/app.py` |
| `start_stock_transactions()` | `_make_stock_step().execute()` in app.py |
| `commit_stock_transactions()` | `_make_stock_step().commit()` in app.py |
| `rollback_workflow()` | `engine._rollback()` in library |
| `start_payment_transaction()` | `_make_payment_step().execute()` in app.py |
| `mark_order_paid()` | app.py, called after `engine.start()` returns |
| `aggregate_items()` | `app.py` |
| `CheckoutWorkflow` (domain fields) | removed — `WorkflowRecord` is generic |
| `drive_saga()` / `drive_2pc()` | `engine._drive_saga()` / `engine._drive_2pc()` |
| `recover_unfinished_workflows()` | `recovery.py` → `start_recovery_thread()` |
| `WorkflowEngine` as Flask app | `WorkflowEngine` as importable Python class |

The key rule: **if it mentions stock, payment, order, URL, or item, it belongs
in the application, not the library.**

---

## Adding a new service

To plug a completely different service (e.g. hotel booking) into the same engine:

```python
# hotel_service/app.py
from orchestrator import WorkflowEngine, Step, StepFailed, STATE_COMPLETED

def make_room_reservation_step(booking_id, room_id, nights):
    def execute(ctx):
        r = requests.post(f"{HOTEL_URL}/reserve/{booking_id}/{room_id}/{nights}")
        if r.status_code != 200:
            raise StepFailed(r.text)
    def rollback(ctx):
        requests.post(f"{HOTEL_URL}/cancel/{booking_id}")
    return Step(name=f"room:{room_id}", execute=execute, rollback=rollback)

# The engine is identical — only the steps change
engine = WorkflowEngine(db, steps=[make_room_reservation_step(...)], mode="SAGA")
record = engine.start(str(uuid.uuid4()), context={...})
```

Zero changes to the library. This is what "no business logic in the orchestrator"
means in practice.

---

## Testing the library in isolation

Because `WorkflowEngine` takes a list of callables, you can unit-test it with
in-memory fakes and a real (or fake) Redis:

```python
import fakeredis
from orchestrator import WorkflowEngine, Step, StepFailed, STATE_COMPLETED, STATE_ROLLED_BACK

def test_saga_rollback_on_second_step_failure():
    db      = fakeredis.FakeRedis()
    log     = []

    steps = [
        Step(
            name     = "step_a",
            execute  = lambda ctx: log.append("a_execute"),
            rollback = lambda ctx: log.append("a_rollback"),
        ),
        Step(
            name     = "step_b",
            execute  = lambda ctx: (_ for _ in ()).throw(StepFailed("b failed")),
            rollback = lambda ctx: log.append("b_rollback"),
        ),
    ]

    engine = WorkflowEngine(db, steps, mode="SAGA")
    record = engine.start("wf-test-1", context={})

    assert record.state == STATE_ROLLED_BACK
    assert "a_execute"  in log    # step_a ran
    assert "a_rollback" in log    # step_a was compensated
    assert "b_rollback" not in log  # step_b never executed, so no rollback
```

No mocking of HTTP, no Flask test client needed — the library is a plain Python
class that can be tested independently of any web framework.