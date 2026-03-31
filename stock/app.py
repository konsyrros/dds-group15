import atexit
import logging
import os
import uuid
from typing import Any, Callable

import redis
from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack

from common.redis_queue import RedisCommandQueue, queued_json, queued_text


DB_ERROR_STR = "DB error"
MAX_RETRIES = 16
TX_STATE_PENDING = "pending"
TX_STATE_COMMITTED = "committed"
TX_STATE_ROLLED_BACK = "rolled_back"

app = Flask("stock-service")


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


class StockValue(Struct):
    stock: int
    price: int


class StockTransaction(Struct):
    item_id: str
    amount: int
    state: str


def get_item_key(item_id: str) -> str:
    return f"item:{item_id}"


def get_transaction_key(tx_id: str) -> str:
    return f"transaction:{tx_id}"


def decode_entry(entry: bytes | None, entry_type: type[Struct]):
    return msgpack.decode(entry, type=entry_type) if entry else None


def load_item(client: redis.Redis | redis.client.Pipeline, item_id: str) -> StockValue:
    try:
        entry: bytes | None = client.get(get_item_key(item_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    item_entry = decode_entry(entry, StockValue)
    if item_entry is None:
        abort(400, f"Item: {item_id} not found!")
    return item_entry


def load_transaction(client: redis.Redis | redis.client.Pipeline, tx_id: str) -> StockTransaction | None:
    try:
        entry: bytes | None = client.get(get_transaction_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return decode_entry(entry, StockTransaction)


def mutate_item(item_id: str, mutator: Callable[[StockValue], None]) -> StockValue:
    item_key = get_item_key(item_id)
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(item_key)
                item_entry = load_item(pipe, item_id)
                mutator(item_entry)
                pipe.multi()
                pipe.set(item_key, msgpack.encode(item_entry))
                pipe.execute()
                return item_entry
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock update, please retry")


def validate_existing_transaction(tx_entry: StockTransaction, item_id: str, amount: int):
    if tx_entry.item_id != item_id or tx_entry.amount != amount:
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


def op_create_item(price: int):
    key = str(uuid.uuid4())
    try:
        db.set(get_item_key(key), msgpack.encode(StockValue(stock=0, price=int(price))))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_json({"item_id": key})


def op_batch_init_users(n: int, starting_stock: int, item_price: int):
    kv_pairs: dict[str, bytes] = {
        get_item_key(f"{i}"): msgpack.encode(StockValue(stock=int(starting_stock), price=int(item_price)))
        for i in range(int(n))
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_json({"msg": "Batch init for stock successful"})


def op_add_stock(item_id: str, amount: int):
    item_entry = mutate_item(item_id, lambda entry: setattr(entry, "stock", entry.stock + int(amount)))
    return queued_text(f"Item: {item_id} stock updated to: {item_entry.stock}")


def op_remove_stock(item_id: str, amount: int):
    def subtract(entry: StockValue):
        entry.stock -= int(amount)
        if entry.stock < 0:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

    item_entry = mutate_item(item_id, subtract)
    return queued_text(f"Item: {item_id} stock updated to: {item_entry.stock}")


def op_start_transaction(tx_id: str, item_id: str, amount: int):
    amount = int(amount)
    item_key = get_item_key(item_id)
    tx_key = get_transaction_key(tx_id)

    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(item_key, tx_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is not None:
                    validate_existing_transaction(tx_entry, item_id, amount)
                    if tx_entry.state in (TX_STATE_PENDING, TX_STATE_COMMITTED):
                        return queued_text(f"Transaction: {tx_id} already {tx_entry.state}")
                    abort(409, f"Transaction: {tx_id} has already been rolled back")

                item_entry = load_item(pipe, item_id)
                if item_entry.stock < amount:
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

                item_entry.stock -= amount
                tx_entry = StockTransaction(item_id=item_id, amount=amount, state=TX_STATE_PENDING)

                pipe.multi()
                pipe.set(item_key, msgpack.encode(item_entry))
                pipe.set(tx_key, msgpack.encode(tx_entry))
                pipe.execute()
                return queued_text(f"Started transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


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
    abort(409, "Concurrent stock transaction, please retry")


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

                item_key = get_item_key(tx_entry.item_id)
                pipe.watch(item_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    return queued_text(f"Transaction: {tx_id} already rolled back or never created")
                if tx_entry.state == TX_STATE_ROLLED_BACK:
                    return queued_text(f"Transaction: {tx_id} already rolled back")

                item_entry = load_item(pipe, tx_entry.item_id)
                item_entry.stock += tx_entry.amount
                tx_entry.state = TX_STATE_ROLLED_BACK

                pipe.multi()
                pipe.set(item_key, msgpack.encode(item_entry))
                pipe.set(tx_key, msgpack.encode(tx_entry))
                pipe.execute()
                return queued_text(f"Rolled back transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


command_queue = RedisCommandQueue(
    name="stock-service",
    redis_factory=create_redis_client,
    handlers={
        "create_item": op_create_item,
        "batch_init_users": op_batch_init_users,
        "add_stock": op_add_stock,
        "remove_stock": op_remove_stock,
        "start_transaction": op_start_transaction,
        "commit_transaction": op_commit_transaction,
        "rollback_transaction": op_rollback_transaction,
    },
    logger=app.logger,
)


@app.post("/item/create/<price>")
def create_item(price: int):
    return execute_queued_command("create_item", price=int(price))


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    return execute_queued_command(
        "batch_init_users",
        n=int(n),
        starting_stock=int(starting_stock),
        item_price=int(item_price),
    )


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item_entry = load_item(db, item_id)
    return jsonify({"stock": item_entry.stock, "price": item_entry.price})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    return execute_queued_command("add_stock", item_id=item_id, amount=int(amount))


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    return execute_queued_command("remove_stock", item_id=item_id, amount=int(amount))


@app.get("/transactions/<tx_id>")
def find_transaction(tx_id: str):
    tx_entry = load_transaction(db, tx_id)
    if tx_entry is None:
        abort(404, f"Transaction: {tx_id} not found!")
    return jsonify({"item_id": tx_entry.item_id, "amount": tx_entry.amount, "state": tx_entry.state})


@app.post("/transactions/start/<tx_id>/<item_id>/<amount>")
def start_transaction(tx_id: str, item_id: str, amount: int):
    return execute_queued_command("start_transaction", tx_id=tx_id, item_id=item_id, amount=int(amount))


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
