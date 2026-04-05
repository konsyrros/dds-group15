import atexit
import logging
import os
import uuid
from typing import Any

import redis
from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack

from common.redis_queue import RedisCommandQueue, queued_json, queued_text


DB_ERROR_STR = "DB error"
MAX_RETRIES = 16
TX_STATE_PENDING = "pending"
TX_STATE_COMMITTED = "committed"
TX_STATE_ROLLED_BACK = "rolled_back"
FAIL_ONCE_BEFORE_ROLLBACK_COMMIT = os.getenv("PAYMENT_FAIL_ONCE_BEFORE_ROLLBACK_COMMIT", "0") == "1"
FAIL_ONCE_BEFORE_ROLLBACK_COMMIT_KEY = "fault:payment:rollback_once_before_commit"

app = Flask("payment-service")


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        password=os.environ["REDIS_PASSWORD"],
        db=int(os.environ["REDIS_DB"]),
    )


db: redis.Redis = create_redis_client()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


class PaymentTransaction(Struct):
    user_id: str
    amount: int
    state: str


def get_transaction_key(tx_id: str) -> str:
    return f"transaction:{tx_id}"


def decode_entry(entry: bytes | None, entry_type: type[Struct]):
    return msgpack.decode(entry, type=entry_type) if entry else None


def get_user_from_db(client: redis.Redis | redis.client.Pipeline, user_id: str) -> UserValue:
    try:
        entry: bytes | None = client.get(user_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    user_entry = decode_entry(entry, UserValue)
    if user_entry is None:
        abort(400, f"User: {user_id} not found!")
    return user_entry


def load_transaction(client: redis.Redis | redis.client.Pipeline, tx_id: str) -> PaymentTransaction | None:
    try:
        entry: bytes | None = client.get(get_transaction_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return decode_entry(entry, PaymentTransaction)


def validate_existing_transaction(tx_entry: PaymentTransaction, user_id: str, amount: int):
    if tx_entry.user_id != user_id or tx_entry.amount != amount:
        abort(409, "Transaction id reused with different parameters")


def execute_queued_command(operation: str, **payload: Any):
    try:
        result = command_queue.execute(operation, **payload)
    except TimeoutError:
        abort(400, "Queue timeout")
    except RuntimeError:
        abort(400, DB_ERROR_STR)
    if not result["ok"]:
        abort(int(result["status"]), result["body"])
    response = result["response"]
    if response["kind"] == "json":
        flask_response = jsonify(response["body"])
        flask_response.status_code = int(response["status"])
        return flask_response
    return Response(response["body"], status=int(response["status"]))


def op_create_user():
    key = str(uuid.uuid4())
    try:
        db.set(key, msgpack.encode(UserValue(credit=0)))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_json({"user_id": key})


def op_batch_init_users(n: int, starting_money: int):
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=int(starting_money))) for i in range(int(n))
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_json({"msg": "Batch init for users successful"})


def op_add_credit(user_id: str, amount: int):
    user_entry = get_user_from_db(db, user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_text(f"User: {user_id} credit updated to: {user_entry.credit}")


def op_remove_credit(user_id: str, amount: int):
    user_entry = get_user_from_db(db, user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_text(f"User: {user_id} credit updated to: {user_entry.credit}")


def op_start_transaction(tx_id: str, user_id: str, amount: int):
    amount = int(amount)
    tx_key = get_transaction_key(tx_id)

    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(user_id, tx_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is not None:
                    validate_existing_transaction(tx_entry, user_id, amount)
                    if tx_entry.state in (TX_STATE_PENDING, TX_STATE_COMMITTED):
                        return queued_text(f"Transaction: {tx_id} already {tx_entry.state}")
                    abort(409, f"Transaction: {tx_id} has already been rolled back")

                user_entry = get_user_from_db(pipe, user_id)
                if user_entry.credit < amount:
                    abort(400, f"User: {user_id} insufficient credit!")

                user_entry.credit -= amount
                tx_entry = PaymentTransaction(user_id=user_id, amount=amount, state=TX_STATE_PENDING)

                pipe.multi()
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.set(tx_key, msgpack.encode(tx_entry))
                pipe.execute()
                return queued_text(f"Started transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent payment transaction, please retry")


def op_commit_transaction(tx_id: str):
    tx_key = get_transaction_key(tx_id)
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(tx_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    abort(404, f"Transaction: {tx_id} not found!")
                if tx_entry.state == TX_STATE_COMMITTED:
                    return queued_text(f"Transaction: {tx_id} already committed")
                if tx_entry.state == TX_STATE_ROLLED_BACK:
                    abort(409, f"Transaction: {tx_id} has already been rolled back")

                tx_entry.state = TX_STATE_COMMITTED
                pipe.multi()
                pipe.set(tx_key, msgpack.encode(tx_entry))
                pipe.execute()
                return queued_text(f"Committed transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent payment transaction, please retry")


def op_rollback_transaction(tx_id: str):
    tx_key = get_transaction_key(tx_id)
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(tx_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    return queued_text(f"Transaction: {tx_id} already rolled back or never created")
                if tx_entry.state == TX_STATE_ROLLED_BACK:
                    return queued_text(f"Transaction: {tx_id} already rolled back")

                pipe.watch(tx_entry.user_id)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    return queued_text(f"Transaction: {tx_id} already rolled back or never created")
                if tx_entry.state == TX_STATE_ROLLED_BACK:
                    return queued_text(f"Transaction: {tx_id} already rolled back")

                user_entry = get_user_from_db(pipe, tx_entry.user_id)

                if FAIL_ONCE_BEFORE_ROLLBACK_COMMIT and db.set(FAIL_ONCE_BEFORE_ROLLBACK_COMMIT_KEY, tx_id, nx=True):
                    app.logger.error("Injected crash before rollback commit for tx_id=%s", tx_id)
                    os._exit(1)

                user_entry.credit += tx_entry.amount
                tx_entry.state = TX_STATE_ROLLED_BACK

                pipe.multi()
                pipe.set(tx_entry.user_id, msgpack.encode(user_entry))
                pipe.set(tx_key, msgpack.encode(tx_entry))
                pipe.execute()
                return queued_text(f"Rolled back transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent payment transaction, please retry")


command_queue = RedisCommandQueue(
    name="payment-service",
    redis_factory=create_redis_client,
    handlers={
        "create_user": op_create_user,
        "batch_init_users": op_batch_init_users,
        "add_credit": op_add_credit,
        "remove_credit": op_remove_credit,
        "start_transaction": op_start_transaction,
        "commit_transaction": op_commit_transaction,
        "rollback_transaction": op_rollback_transaction,
    },
    logger=app.logger,
)


@app.post("/create_user")
def create_user():
    return execute_queued_command("create_user")


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    return execute_queued_command("batch_init_users", n=int(n), starting_money=int(starting_money))


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_entry = get_user_from_db(db, user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    return execute_queued_command("add_credit", user_id=user_id, amount=int(amount))


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    return execute_queued_command("remove_credit", user_id=user_id, amount=int(amount))


@app.get("/transactions/<tx_id>")
def find_transaction(tx_id: str):
    tx_entry = load_transaction(db, tx_id)
    if tx_entry is None:
        abort(404, f"Transaction: {tx_id} not found!")
    return jsonify({"user_id": tx_entry.user_id, "amount": tx_entry.amount, "state": tx_entry.state})


@app.post("/transactions/start/<tx_id>/<user_id>/<amount>")
def start_transaction(tx_id: str, user_id: str, amount: int):
    return execute_queued_command("start_transaction", tx_id=tx_id, user_id=user_id, amount=int(amount))


@app.post("/transactions/commit/<tx_id>")
def commit_transaction(tx_id: str):
    return execute_queued_command("commit_transaction", tx_id=tx_id)


@app.post("/transactions/rollback/<tx_id>")
def rollback_transaction(tx_id: str):
    return execute_queued_command("rollback_transaction", tx_id=tx_id)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
