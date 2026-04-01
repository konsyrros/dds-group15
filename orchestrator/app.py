import atexit
import logging
import os
import threading
import time
import uuid
from collections import defaultdict

import redis
import requests
from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack
from werkzeug.exceptions import HTTPException

from common.redis_queue import RedisCommandQueue, queued_json


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Dependency request failed"
MAX_RETRIES = 16
MODE_SAGA = "SAGA"
MODE_2PC = "2PC"
WORKFLOW_STATE_CREATED = "created"
WORKFLOW_STATE_PREPARING = "preparing"
WORKFLOW_STATE_COMMITTING = "committing"
WORKFLOW_STATE_ROLLING_BACK = "rolling_back"
WORKFLOW_STATE_COMPLETED = "completed"
WORKFLOW_STATE_ROLLED_BACK = "rolled_back"
WORKFLOW_STATE_FAILED = "failed"
STEP_PENDING = "pending"
STEP_STARTED = "started"
STEP_COMMITTED = "committed"
STEP_ROLLED_BACK = "rolled_back"
RECOVERY_SCAN_INTERVAL_SECONDS = float(os.getenv("RECOVERY_SCAN_INTERVAL_SECONDS", "5"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
WORKFLOW_LOCK_SECONDS = int(os.getenv("WORKFLOW_LOCK_SECONDS", "120"))
RECOVERY_LEADER_LOCK_SECONDS = int(
    os.getenv("RECOVERY_LEADER_LOCK_SECONDS", str(max(5, int(RECOVERY_SCAN_INTERVAL_SECONDS * 2))))
)
STOCK_SERVICE_URL = os.environ.get("STOCK_SERVICE_URL", "http://stock-service:5000")
PAYMENT_SERVICE_URL = os.environ.get("PAYMENT_SERVICE_URL", "http://payment-service:5000")
TX_MODE = os.getenv("TX_MODE", MODE_SAGA).upper()
RECOVERY_LEADER_KEY = "orchestrator:recovery:leader"

if TX_MODE not in (MODE_SAGA, MODE_2PC):
    raise ValueError(f"Invalid TX_MODE={TX_MODE}. Use SAGA or 2PC.")

_RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

app = Flask("orchestrator-service")


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        password=os.environ["REDIS_PASSWORD"],
        db=int(os.environ["REDIS_DB"])
    )


def create_order_db_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ["ORDER_REDIS_HOST"],
        port=int(os.environ["ORDER_REDIS_PORT"]),
        password=os.environ["ORDER_REDIS_PASSWORD"],
        db=int(os.environ["ORDER_REDIS_DB"])
    )


db: redis.Redis = create_redis_client()
order_db: redis.Redis = create_order_db_client()


def close_db_connections():
    db.close()
    order_db.close()


atexit.register(close_db_connections)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class CheckoutWorkflow(Struct):
    workflow_id: str
    order_id: str
    mode: str
    state: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    stock_states: dict[str, str]
    payment_state: str
    order_finalized: bool = False
    error: str = ""


def workflow_key(workflow_id: str) -> str:
    return f"orchestrator:workflow:{workflow_id}"


def workflow_lock_key(workflow_id: str) -> str:
    return f"orchestrator:lock:{workflow_id}"


def active_order_workflow_key(order_id: str) -> str:
    return f"orchestrator:order:{order_id}"


def recovery_leader_token() -> str:
    return f"{os.getpid()}:{threading.current_thread().name}"


def stock_tx_id(workflow_id: str, item_id: str) -> str:
    return f"{workflow_id}:{item_id}"


def decode_entry(entry: bytes | None, entry_type: type[Struct]):
    return msgpack.decode(entry, type=entry_type) if entry else None


def load_workflow(workflow_id: str) -> CheckoutWorkflow | None:
    try:
        raw = db.get(workflow_key(workflow_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return decode_entry(raw, CheckoutWorkflow)


def save_workflow(workflow: CheckoutWorkflow):
    try:
        db.set(workflow_key(workflow.workflow_id), msgpack.encode(workflow))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def is_terminal_state(state: str) -> bool:
    return state in (WORKFLOW_STATE_COMPLETED, WORKFLOW_STATE_ROLLED_BACK, WORKFLOW_STATE_FAILED)


def load_order(order_id: str) -> OrderValue:
    try:
        raw = order_db.get(order_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    order_entry = decode_entry(raw, OrderValue)
    if order_entry is None:
        abort(404, f"Order: {order_id} not found!")
    return order_entry


def mark_order_paid(order_id: str) -> bool:
    for _ in range(MAX_RETRIES):
        with order_db.pipeline() as pipe:
            try:
                pipe.watch(order_id)
                raw = pipe.get(order_id)
                order_entry = decode_entry(raw, OrderValue)
                if order_entry is None:
                    abort(404, f"Order: {order_id} not found!")
                if order_entry.paid:
                    return False
                order_entry.paid = True
                pipe.multi()
                pipe.set(order_id, msgpack.encode(order_entry))
                pipe.execute()
                return True
            except redis.exceptions.WatchError:
                continue
            except redis.exceptions.RedisError:
                abort(400, DB_ERROR_STR)
    abort(409, f"Concurrent order update for {order_id}, please retry")


def clear_active_order_workflow(order_id: str, workflow_id: str):
    try:
        if db.get(active_order_workflow_key(order_id)) == workflow_id.encode():
            db.delete(active_order_workflow_key(order_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def aggregate_items(items: list[tuple[str, int]]) -> list[tuple[str, int]]:
    grouped: defaultdict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        grouped[item_id] += int(quantity)
    return sorted(grouped.items())


def request(method: str, url: str) -> requests.Response:
    try:
        return requests.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.exceptions.RequestException:
        abort(400, f"{REQ_ERROR_STR}: {url}")


def initialize_workflow(order_id: str) -> CheckoutWorkflow:
    order_entry = load_order(order_id)
    if order_entry.paid:
        abort(409, "Order already paid")
    items = aggregate_items(order_entry.items)
    return CheckoutWorkflow(
        workflow_id=str(uuid.uuid4()),
        order_id=order_id,
        mode=TX_MODE,
        state=WORKFLOW_STATE_CREATED,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=items,
        stock_states={item_id: STEP_PENDING for item_id, _ in items},
        payment_state=STEP_PENDING
    )


def build_terminal_response(workflow: CheckoutWorkflow) -> dict[str, object]:
    if workflow.state == WORKFLOW_STATE_COMPLETED:
        return queued_json(
            {
                "workflow_id": workflow.workflow_id,
                "order_id": workflow.order_id,
                "mode": workflow.mode,
                "state": workflow.state
            }
        )
    abort(409 if workflow.state == WORKFLOW_STATE_FAILED else 400, workflow.error or workflow.state)


def persist_failure(workflow: CheckoutWorkflow, state: str, error: str):
    workflow.state = state
    workflow.error = error
    save_workflow(workflow)
    clear_active_order_workflow(workflow.order_id, workflow.workflow_id)


def finalize_workflow_success(workflow: CheckoutWorkflow) -> dict[str, object]:
    workflow.state = WORKFLOW_STATE_COMPLETED
    workflow.error = ""
    save_workflow(workflow)
    clear_active_order_workflow(workflow.order_id, workflow.workflow_id)
    return queued_json(
        {
            "workflow_id": workflow.workflow_id,
            "order_id": workflow.order_id,
            "mode": workflow.mode,
            "state": workflow.state
        }
    )


def rollback_workflow(workflow: CheckoutWorkflow, reason: str):
    workflow.state = WORKFLOW_STATE_ROLLING_BACK
    workflow.error = reason
    save_workflow(workflow)

    for item_id, _ in workflow.items:
        if workflow.stock_states.get(item_id) not in (STEP_STARTED, STEP_COMMITTED):
            continue
        response = request("POST", f"{STOCK_SERVICE_URL}/transactions/rollback/{stock_tx_id(workflow.workflow_id, item_id)}")
        if response.status_code != 200:
            abort(response.status_code, response.text or f"Failed to roll back stock transaction for {item_id}")
        workflow.stock_states[item_id] = STEP_ROLLED_BACK
        save_workflow(workflow)

    if workflow.payment_state in (STEP_STARTED, STEP_COMMITTED):
        response = request("POST", f"{PAYMENT_SERVICE_URL}/transactions/rollback/{workflow.workflow_id}")
        if response.status_code != 200:
            abort(response.status_code, response.text or "Failed to roll back payment transaction")
        workflow.payment_state = STEP_ROLLED_BACK
        save_workflow(workflow)

    persist_failure(workflow, WORKFLOW_STATE_ROLLED_BACK, reason)
    abort(400, reason)


def start_stock_transactions(workflow: CheckoutWorkflow, rollback_on_failure: bool):
    for item_id, quantity in workflow.items:
        if workflow.stock_states.get(item_id) != STEP_PENDING:
            continue
        response = request(
            "POST",
            f"{STOCK_SERVICE_URL}/transactions/start/{stock_tx_id(workflow.workflow_id, item_id)}/{item_id}/{quantity}"
        )
        if response.status_code != 200:
            if rollback_on_failure:
                rollback_workflow(workflow, response.text or f"Failed to start stock transaction for {item_id}")
            abort(response.status_code, response.text or f"Failed to start stock transaction for {item_id}")
        workflow.stock_states[item_id] = STEP_STARTED
        save_workflow(workflow)


def commit_stock_transactions(workflow: CheckoutWorkflow):
    for item_id, _ in workflow.items:
        if workflow.stock_states.get(item_id) != STEP_STARTED:
            continue
        response = request(
            "POST",
            f"{STOCK_SERVICE_URL}/transactions/commit/{stock_tx_id(workflow.workflow_id, item_id)}"
        )
        if response.status_code != 200:
            abort(response.status_code, response.text or f"Failed to commit stock transaction for {item_id}")
        workflow.stock_states[item_id] = STEP_COMMITTED
        save_workflow(workflow)


def start_payment_transaction(workflow: CheckoutWorkflow, rollback_on_failure: bool):
    if workflow.payment_state != STEP_PENDING:
        return
    response = request(
        "POST",
        f"{PAYMENT_SERVICE_URL}/transactions/start/{workflow.workflow_id}/{workflow.user_id}/{workflow.total_cost}"
    )
    if response.status_code != 200:
        if rollback_on_failure:
            rollback_workflow(workflow, response.text or "Failed to start payment transaction")
        persist_failure(workflow, WORKFLOW_STATE_FAILED, response.text or "Failed to start payment transaction")
        abort(response.status_code, workflow.error)
    workflow.payment_state = STEP_STARTED
    save_workflow(workflow)


def commit_payment_transaction(workflow: CheckoutWorkflow):
    if workflow.payment_state != STEP_STARTED:
        return
    response = request("POST", f"{PAYMENT_SERVICE_URL}/transactions/commit/{workflow.workflow_id}")
    if response.status_code != 200:
        abort(response.status_code, response.text or "Failed to commit payment transaction")
    workflow.payment_state = STEP_COMMITTED
    save_workflow(workflow)


def drive_saga(workflow: CheckoutWorkflow) -> dict[str, object]:
    workflow.state = WORKFLOW_STATE_PREPARING
    save_workflow(workflow)

    start_payment_transaction(workflow, rollback_on_failure=False)
    commit_payment_transaction(workflow)
    start_stock_transactions(workflow, rollback_on_failure=True)

    workflow.state = WORKFLOW_STATE_COMMITTING
    save_workflow(workflow)

    commit_stock_transactions(workflow)

    if not workflow.order_finalized:
        mark_order_paid(workflow.order_id)
        workflow.order_finalized = True
        save_workflow(workflow)

    return finalize_workflow_success(workflow)


def drive_2pc(workflow: CheckoutWorkflow) -> dict[str, object]:
    workflow.state = WORKFLOW_STATE_PREPARING
    save_workflow(workflow)

    start_stock_transactions(workflow, rollback_on_failure=True)
    start_payment_transaction(workflow, rollback_on_failure=True)

    workflow.state = WORKFLOW_STATE_COMMITTING
    save_workflow(workflow)

    commit_stock_transactions(workflow)
    commit_payment_transaction(workflow)

    if not workflow.order_finalized:
        mark_order_paid(workflow.order_id)
        workflow.order_finalized = True
        save_workflow(workflow)

    return finalize_workflow_success(workflow)


def run_workflow_by_id(workflow_id: str) -> dict[str, object]:
    workflow = load_workflow(workflow_id)
    if workflow is None:
        abort(404, f"Workflow: {workflow_id} not found!")
    if is_terminal_state(workflow.state):
        return build_terminal_response(workflow)

    lock_token = uuid.uuid4().hex
    try:
        acquired = db.set(workflow_lock_key(workflow_id), lock_token, nx=True, ex=WORKFLOW_LOCK_SECONDS)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    if not acquired:
        workflow = load_workflow(workflow_id)
        if workflow is not None and is_terminal_state(workflow.state):
            return build_terminal_response(workflow)
        abort(409, f"Workflow: {workflow_id} is already in progress")

    try:
        workflow = load_workflow(workflow_id)
        if workflow is None:
            abort(404, f"Workflow: {workflow_id} not found!")
        if workflow.mode == MODE_SAGA:
            return drive_saga(workflow)
        return drive_2pc(workflow)
    finally:
        try:
            db.eval(_RELEASE_LOCK_SCRIPT, 1, workflow_lock_key(workflow_id), lock_token)
        except redis.exceptions.RedisError:
            app.logger.warning("Failed to release lock for workflow %s", workflow_id)


def op_checkout(order_id: str):
    order_entry = load_order(order_id)
    if order_entry.paid:
        return queued_json({"order_id": order_id, "state": WORKFLOW_STATE_COMPLETED, "mode": TX_MODE})

    active_key = active_order_workflow_key(order_id)
    workflow_id: str | None = None

    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(active_key)
                raw_workflow_id = pipe.get(active_key)
                if raw_workflow_id:
                    workflow_id = raw_workflow_id.decode()
                    existing_workflow = load_workflow(workflow_id)
                    if existing_workflow is not None and not is_terminal_state(existing_workflow.state):
                        break
                    pipe.multi()
                    pipe.delete(active_key)
                    pipe.execute()
                    continue

                workflow = initialize_workflow(order_id)
                workflow_id = workflow.workflow_id
                pipe.multi()
                pipe.set(workflow_key(workflow.workflow_id), msgpack.encode(workflow))
                pipe.set(active_key, workflow.workflow_id)
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                continue
            except redis.exceptions.RedisError:
                abort(400, DB_ERROR_STR)
    else:
        abort(409, f"Concurrent checkout initialization for order {order_id}")

    return run_workflow_by_id(workflow_id)


def op_resume_workflow(workflow_id: str):
    return run_workflow_by_id(workflow_id)


command_queue = RedisCommandQueue(
    name="orchestrator-service",
    redis_factory=create_redis_client,
    handlers={
        "checkout": op_checkout,
        "resume_workflow": op_resume_workflow,
    },
    logger=app.logger
)


def recover_unfinished_workflows():
    leader_token = recovery_leader_token()
    while True:
        try:
            acquired = db.set(RECOVERY_LEADER_KEY, leader_token, nx=True, ex=RECOVERY_LEADER_LOCK_SECONDS)
            if not acquired:
                current_leader = db.get(RECOVERY_LEADER_KEY)
                if current_leader == leader_token.encode():
                    db.expire(RECOVERY_LEADER_KEY, RECOVERY_LEADER_LOCK_SECONDS)
                else:
                    time.sleep(RECOVERY_SCAN_INTERVAL_SECONDS)
                    continue

            for key in db.scan_iter("orchestrator:workflow:*"):
                raw = db.get(key)
                if not raw:
                    continue
                workflow = msgpack.decode(raw, type=CheckoutWorkflow)
                if is_terminal_state(workflow.state):
                    continue
                try:
                    run_workflow_by_id(workflow.workflow_id)
                except HTTPException as exc:
                    if exc.code == 409 and "already in progress" in exc.description:
                        continue
                    app.logger.warning(
                        "Recovery retry for workflow %s failed: %s %s",
                        workflow.workflow_id,
                        exc.code,
                        exc.description,
                    )
                except Exception as exc:
                    app.logger.warning(
                        "Recovery retry for workflow %s failed: %s",
                        workflow.workflow_id,
                        exc
                    )
        except redis.exceptions.RedisError:
            app.logger.warning("Failed to scan unfinished workflows during recovery")
        time.sleep(RECOVERY_SCAN_INTERVAL_SECONDS)


recovery_thread = threading.Thread(
    target=recover_unfinished_workflows,
    name="orchestrator-recovery",
    daemon=True
)
recovery_thread.start()


def execute_queued_command(operation: str, **payload):
    try:
        result = command_queue.execute(operation, **payload)
    except TimeoutError:
        abort(400, "Queue timeout")
    except RuntimeError:
        abort(400, DB_ERROR_STR)
    if not result["ok"]:
        abort(int(result["status"]), result["body"])
    response = result["response"]
    flask_response = jsonify(response["body"])
    flask_response.status_code = int(response["status"])
    return flask_response


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    return execute_queued_command("checkout", order_id=order_id)


@app.post("/workflow/<workflow_id>/resume")
def resume_workflow(workflow_id: str):
    return execute_queued_command("resume_workflow", workflow_id=workflow_id)


@app.get("/workflow/<workflow_id>")
def find_workflow(workflow_id: str):
    workflow = load_workflow(workflow_id)
    if workflow is None:
        abort(404, f"Workflow: {workflow_id} not found!")
    return jsonify(
        {
            "workflow_id": workflow.workflow_id,
            "order_id": workflow.order_id,
            "mode": workflow.mode,
            "state": workflow.state,
            "user_id": workflow.user_id,
            "total_cost": workflow.total_cost,
            "items": workflow.items,
            "stock_states": workflow.stock_states,
            "payment_state": workflow.payment_state,
            "order_finalized": workflow.order_finalized,
            "error": workflow.error,
        }
    )



if __name__ == "__main__":
    app.logger.info("Orchestrator running with TX_MODE=%s", TX_MODE)
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.info("Orchestrator running with TX_MODE=%s", TX_MODE)
