"""
order_service/app.py
====================
Order microservice — rewritten to delegate all transaction coordination
to the orchestrator library.

What lives here (business logic — ONLY here):
  - Order CRUD (create, find, addItem)
  - URL patterns for stock and payment services
  - Step definitions: what HTTP calls each step makes
  - Step ordering for SAGA vs 2PC
  - mark_order_paid() — setting order.paid = True after success
  - Startup recovery: reconstruct steps from context and resume

What does NOT live here (engine concerns — in the library):
  - SAGA vs 2PC protocol mechanics
  - Rollback ordering and looping
  - Distributed locking
  - State persistence
  - WAL / crash recovery scanning

Checkout test commands:
  curl -X POST http://localhost:8000/payment/create_user
  curl -X POST http://localhost:8000/payment/add_funds/USER_ID/1000
  curl -X POST http://localhost:8000/stock/item/create/10
  curl -X POST http://localhost:8000/stock/add/ITEM_ID/50
  curl -X POST http://localhost:8000/orders/create/USER_ID
  curl -X POST http://localhost:8000/orders/addItem/ORDER_ID/ITEM_ID/2
  curl -X POST http://localhost:8000/orders/checkout/ORDER_ID
  curl -i http://localhost:8000/payment/find_user/USER_ID
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import random
import uuid
from collections import defaultdict

import redis
import requests
from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack

from orchestrator import (
    STATE_COMPLETED,
    Step,
    StepFailed,
    WorkflowEngine,
    start_recovery_thread,
)

# ─────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────

GATEWAY_URL = os.environ["GATEWAY_URL"]
TX_MODE     = os.getenv("TX_MODE", "SAGA").upper()

if TX_MODE not in ("SAGA", "2PC"):
    raise ValueError(f"Invalid TX_MODE={TX_MODE}. Use SAGA or 2PC.")

LOCK_TTL          = int(os.getenv("WORKFLOW_LOCK_SECONDS",            "120"))
RECOVERY_INTERVAL = float(os.getenv("RECOVERY_SCAN_INTERVAL_SECONDS", "5"))
REQUEST_TIMEOUT   = float(os.getenv("REQUEST_TIMEOUT_SECONDS",        "15"))

DB_ERROR_STR  = "DB error"
REQ_ERROR_STR = "Requests error"

app = Flask("order-service")

# ─────────────────────────────────────────────────────────────────
# Redis clients
#
# db        — used by WorkflowEngine to persist workflow records
# (order records are stored in the same db for simplicity, but
#  keyed differently so they never collide with orchestrator keys)
# ─────────────────────────────────────────────────────────────────

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

atexit.register(db.close)

# ─────────────────────────────────────────────────────────────────
# Order domain model
# ─────────────────────────────────────────────────────────────────

class OrderValue(Struct):
    paid:       bool
    items:      list[tuple[str, int]]
    user_id:    str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    if not entry:
        abort(400, f"Order: {order_id} not found!")
    return msgpack.decode(entry, type=OrderValue)


def mark_order_paid(order_id: str) -> None:
    """
    Atomically flip paid=True on the order record.
    Uses WATCH/MULTI/EXEC so concurrent checkouts don't double-pay.
    """
    for _ in range(16):
        with db.pipeline() as pipe:
            try:
                pipe.watch(order_id)
                raw = pipe.get(order_id)
                if not raw:
                    abort(400, f"Order: {order_id} not found!")
                order = msgpack.decode(raw, type=OrderValue)
                if order.paid:
                    return          # already paid — idempotent
                order.paid = True
                pipe.multi()
                pipe.set(order_id, msgpack.encode(order))
                pipe.execute()
                return
            except redis.WatchError:
                continue
            except redis.exceptions.RedisError:
                abort(400, DB_ERROR_STR)
    abort(409, f"Concurrent update on order {order_id}, please retry")


def aggregate_items(items: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """Merge duplicate item_ids and sort for deterministic step naming."""
    grouped: defaultdict[str, int] = defaultdict(int)
    for item_id, qty in items:
        grouped[item_id] += int(qty)
    return sorted(grouped.items())


# ─────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────

def _post(url: str) -> requests.Response:
    """
    Send a POST request and convert network errors to StepFailed
    so the engine handles them as a recoverable step failure.
    """
    try:
        return requests.post(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        raise StepFailed(f"{REQ_ERROR_STR}: {url} — {exc}") from exc


def _get(url: str) -> requests.Response:
    try:
        return requests.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        abort(400, f"{REQ_ERROR_STR}: {url}")


# ─────────────────────────────────────────────────────────────────
# Step factories
#
# Each factory returns a Step whose callables close over only the
# parameters they need.  All parameters also live in the context
# dict so the recovery thread can reconstruct the exact same steps.
# ─────────────────────────────────────────────────────────────────

def _item_tx_id(workflow_id: str, item_id: str) -> str:
    """Stable, unique transaction ID for a single stock item in a workflow."""
    return f"{workflow_id}:{item_id}"


def _make_stock_step(workflow_id: str, item_id: str, quantity: int) -> Step:
    """
    One Step per unique item in the order.

    SAGA:
      execute  → POST /stock/subtract_tx/{tx_id}/{item_id}/{quantity}
                 Immediately deducts stock. Gateway routes this to stock service.
      rollback → POST /stock/add_tx/{tx_id}/{item_id}
                 Adds the stock back (compensation).  Idempotent on the stock side.

    2PC:
      execute  → POST /stock/transaction/prepare/{tx_id}/{item_id}/{quantity}
                 Reserves stock and acquires a row lock. Does NOT deduct yet.
      rollback → POST /stock/transaction/cancel/{tx_id}
                 Releases the reservation/lock without deducting.
      commit   → POST /stock/transaction/commit/{tx_id}
                 Permanently deducts the reserved stock.
    """
    tx_id = _item_tx_id(workflow_id, item_id)

    def execute(ctx: dict) -> None:
        if TX_MODE == "SAGA":
            url = f"{GATEWAY_URL}/stock/subtract_tx/{tx_id}/{item_id}/{quantity}"
            app.logger.info("[order][SAGA] stock subtract  item=%s  qty=%d  tx=%s",
                            item_id, quantity, tx_id)
        else:
            url = f"{GATEWAY_URL}/stock/transaction/prepare/{tx_id}/{item_id}/{quantity}"
            app.logger.info("[order][2PC]  stock prepare   item=%s  qty=%d  tx=%s",
                            item_id, quantity, tx_id)
        r = _post(url)
        if r.status_code != 200:
            raise StepFailed(
                r.text or f"Stock {'subtract' if TX_MODE == 'SAGA' else 'prepare'} "
                          f"failed for item {item_id}",
                r.status_code,
            )

    def rollback(ctx: dict) -> None:
        if TX_MODE == "SAGA":
            url = f"{GATEWAY_URL}/stock/add_tx/{tx_id}/{item_id}"
            app.logger.info("[order][SAGA] stock add-back  item=%s  tx=%s", item_id, tx_id)
        else:
            url = f"{GATEWAY_URL}/stock/transaction/cancel/{tx_id}"
            app.logger.info("[order][2PC]  stock cancel    item=%s  tx=%s", item_id, tx_id)
        r = _post(url)
        if r.status_code != 200:
            # Log but do not raise — the engine continues rolling back
            # remaining steps even if one rollback call fails.
            app.logger.error(
                "[order] stock rollback FAILED  item=%s  tx=%s  status=%d  body=%s",
                item_id, tx_id, r.status_code, r.text,
            )

    def commit(ctx: dict) -> None:
        # Only called in 2PC mode
        url = f"{GATEWAY_URL}/stock/transaction/commit/{tx_id}"
        app.logger.info("[order][2PC] stock commit  item=%s  tx=%s", item_id, tx_id)
        r = _post(url)
        if r.status_code != 200:
            raise StepFailed(
                r.text or f"Stock commit failed for item {item_id}",
                r.status_code,
            )

    return Step(
        name     = f"stock:{item_id}",
        execute  = execute,
        rollback = rollback,
        commit   = commit,          # engine ignores this in SAGA mode
    )


def _make_payment_step(workflow_id: str, user_id: str, total_cost: int) -> Step:
    """
    One Step for charging the user.

    SAGA:
      execute  → POST /payment/pay_tx/{tx_id}/{user_id}/{total_cost}
      rollback → POST /payment/refund_tx/{tx_id}

    2PC:
      execute  → POST /payment/2pc/prepare/{user_id}/{total_cost}/{tx_id}
      rollback → POST /payment/2pc/abort/{tx_id}
      commit   → POST /payment/2pc/commit/{tx_id}
    """
    tx_id = workflow_id      # payment uses the workflow_id directly as its tx_id

    def execute(ctx: dict) -> None:
        if TX_MODE == "SAGA":
            url = f"{GATEWAY_URL}/payment/pay_tx/{tx_id}/{user_id}/{total_cost}"
            app.logger.info("[order][SAGA] payment charge  user=%s  cost=%d  tx=%s",
                            user_id, total_cost, tx_id)
        else:
            url = f"{GATEWAY_URL}/payment/2pc/prepare/{user_id}/{total_cost}/{tx_id}"
            app.logger.info("[order][2PC]  payment prepare user=%s  cost=%d  tx=%s",
                            user_id, total_cost, tx_id)
        r = _post(url)
        if r.status_code != 200:
            raise StepFailed(
                r.text or "Payment failed",
                r.status_code,
            )

    def rollback(ctx: dict) -> None:
        if TX_MODE == "SAGA":
            url = f"{GATEWAY_URL}/payment/refund_tx/{tx_id}"
            app.logger.info("[order][SAGA] payment refund  tx=%s", tx_id)
        else:
            url = f"{GATEWAY_URL}/payment/2pc/abort/{tx_id}"
            app.logger.info("[order][2PC]  payment abort   tx=%s", tx_id)
        r = _post(url)
        if r.status_code != 200:
            app.logger.error(
                "[order] payment rollback FAILED  tx=%s  status=%d  body=%s",
                tx_id, r.status_code, r.text,
            )

    def commit(ctx: dict) -> None:
        url = f"{GATEWAY_URL}/payment/2pc/commit/{tx_id}"
        app.logger.info("[order][2PC] payment commit  tx=%s", tx_id)
        r = _post(url)
        if r.status_code != 200:
            raise StepFailed(
                r.text or "Payment commit failed",
                r.status_code,
            )

    return Step(
        name     = "payment",
        execute  = execute,
        rollback = rollback,
        commit   = commit,
    )


def build_steps(
    workflow_id: str,
    items:       list[tuple[str, int]],
    user_id:     str,
    total_cost:  int,
) -> list[Step]:
    """
    Assemble the ordered Step list for one checkout.

    SAGA step order: payment FIRST, then stock items.
      Rationale: payment is cheapest to roll back (one refund call).
      If we charge first and stock then fails, we only need to refund.
      If we subtract stock first and payment fails, we need to add back
      every item — more calls and more surface area for partial failure.

    2PC step order: stock items FIRST, then payment.
      In 2PC all steps prepare (lock) before any commit, so ordering
      only affects which resources get locked first.  Stock-first
      matches the original implementation and avoids locking credit
      while stock prepare is still in flight.
    """
    stock_steps  = [_make_stock_step(workflow_id, item_id, qty)
                    for item_id, qty in items]
    payment_step = _make_payment_step(workflow_id, user_id, total_cost)

    if TX_MODE == "SAGA":
        return [payment_step] + stock_steps   # pay first
    else:
        return stock_steps + [payment_step]   # stock prepare first


# ─────────────────────────────────────────────────────────────────
# Checkout
# ─────────────────────────────────────────────────────────────────

@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    app.logger.info("[order] checkout  order_id=%s  mode=%s", order_id, TX_MODE)

    order = get_order_from_db(order_id)

    if order.paid:
        app.logger.info("[order] already paid  order_id=%s", order_id)
        return Response("Already paid", status=200)

    items       = aggregate_items(order.items)
    workflow_id = str(uuid.uuid4())

    # Context is persisted inside WorkflowRecord so the recovery thread
    # can reconstruct the exact same steps after a crash.
    context = {
        "workflow_id": workflow_id,
        "order_id":    order_id,
        "user_id":     order.user_id,
        "total_cost":  order.total_cost,
        # Store items so recovery can rebuild steps without re-reading the order
        "items":       items,
    }

    steps  = build_steps(workflow_id, items, order.user_id, order.total_cost)
    engine = WorkflowEngine(db, steps, mode=TX_MODE, lock_ttl_seconds=LOCK_TTL)

    app.logger.info(
        "[order] starting workflow  workflow_id=%s  steps=%s",
        workflow_id, [s.name for s in steps],
    )

    record = engine.start(workflow_id, context)

    if record.state == STATE_COMPLETED:
        mark_order_paid(order_id)
        app.logger.info("[order] checkout SUCCESS  order_id=%s  workflow_id=%s",
                        order_id, workflow_id)
        return Response(f"Checkout successful ({TX_MODE})", status=200)

    app.logger.warning(
        "[order] checkout FAILED  order_id=%s  workflow_id=%s  state=%s  error=%s",
        order_id, workflow_id, record.state, record.error,
    )
    abort(400, record.error or f"Checkout failed (state={record.state})")


# ─────────────────────────────────────────────────────────────────
# Standard order CRUD endpoints (unchanged from original)
# ─────────────────────────────────────────────────────────────────

@app.post("/create/<user_id>")
def create_order(user_id: str):
    key   = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n          = int(n)
    n_items    = int(n_items)
    n_users    = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id  = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(generate_entry()) for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    order = get_order_from_db(order_id)
    return jsonify({
        "order_id":   order_id,
        "paid":       order.paid,
        "items":      order.items,
        "user_id":    order.user_id,
        "total_cost": order.total_cost,
    })


@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    order      = get_order_from_db(order_id)
    item_reply = _get(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order.items.append((item_id, int(quantity)))
    order.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order.total_cost}",
        status=200,
    )


# ─────────────────────────────────────────────────────────────────
# Recovery
#
# The recovery thread scans for non-terminal WorkflowRecords and
# calls engine.resume() on each one.  Because steps are stateless
# Python functions, we reconstruct them from the context that was
# persisted inside the WorkflowRecord.
# ─────────────────────────────────────────────────────────────────

def _rebuild_engine_for_recovery(workflow_id: str) -> WorkflowEngine | None:
    """
    Load a WorkflowRecord from Redis, deserialise its context, and build a
    WorkflowEngine with exactly the same steps the original checkout used.

    Returns None if the record is gone or its context is malformed.
    """
    # We need a temporary engine just to call get()
    _probe = WorkflowEngine(db, steps=_noop_steps(), mode=TX_MODE)
    record = _probe.get(workflow_id)
    if record is None:
        return None

    try:
        ctx        = json.loads(record.context_json)
        wf_id      = ctx["workflow_id"]
        items      = [tuple(pair) for pair in ctx["items"]]   # JSON gives lists
        user_id    = ctx["user_id"]
        total_cost = ctx["total_cost"]
    except (KeyError, ValueError, TypeError) as exc:
        app.logger.error("[order][recovery] malformed context  workflow_id=%s: %s",
                         workflow_id, exc)
        return None

    steps = build_steps(wf_id, items, user_id, total_cost)
    return WorkflowEngine(db, steps, mode=record.mode, lock_ttl_seconds=LOCK_TTL)


def _recover_workflow(workflow_id: str) -> None:
    """Resume a single workflow and finalize the order if it completes."""
    engine = _rebuild_engine_for_recovery(workflow_id)
    if engine is None:
        return

    try:
        record = engine.resume(workflow_id)
    except Exception as exc:
        app.logger.warning("[order][recovery] resume failed  workflow_id=%s: %s",
                           workflow_id, exc)
        return

    if record.state == STATE_COMPLETED:
        # The workflow finished but mark_order_paid may not have run yet.
        order_id = json.loads(record.context_json).get("order_id")
        if order_id:
            try:
                mark_order_paid(order_id)
                app.logger.info("[order][recovery] marked paid  order_id=%s  workflow_id=%s",
                                order_id, workflow_id)
            except Exception as exc:
                app.logger.error("[order][recovery] mark_order_paid failed  order_id=%s: %s",
                                 order_id, exc)


def _noop_steps() -> list[Step]:
    """
    A single no-op step so WorkflowEngine doesn't raise on an empty list.
    Only used to probe the engine's get() method during recovery.
    """
    return [Step(
        name     = "_noop",
        execute  = lambda ctx: None,
        rollback = lambda ctx: None,
        commit   = lambda ctx: None,
    )]


# The recovery thread calls engine.resume() for every non-terminal record
# it finds.  We wrap it with our _recover_workflow so we also call
# mark_order_paid when a workflow completes during recovery.

class _RecoveryAdapter:
    """
    Wraps WorkflowEngine.resume() to inject our order-level finalization
    (mark_order_paid) without putting that logic inside the library.
    """
    def resume(self, workflow_id: str):
        _recover_workflow(workflow_id)


_recovery_adapter = _RecoveryAdapter()

# Uses a no-op engine to do the Redis scan; actual driving is done
# via _RecoveryAdapter which rebuilds the real engine per workflow.
_scan_engine = WorkflowEngine(db, steps=_noop_steps(), mode=TX_MODE)

recovery_thread = start_recovery_thread(
    engine           = _recovery_adapter,
    db               = db,
    interval_seconds = RECOVERY_INTERVAL,
)

app.logger.info("[order] recovery thread started  interval=%.1fs", RECOVERY_INTERVAL)


# ─────────────────────────────────────────────────────────────────
# Gunicorn / direct run
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info("[order] service started  TX_MODE=%s", TX_MODE)