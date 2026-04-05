import atexit
import logging
import os
import uuid
from typing import Any, Callable

import redis
from flask import Flask, Response, abort, jsonify
from msgspec import Struct, msgpack

from orchestrator import RedisCommandQueue, queued_json, queued_text


DB_ERROR_STR = "DB error"
MAX_RETRIES = 16
TX_PROTOCOL_2PC = "2pc"
TX_PROTOCOL_SAGA = "saga"
STATE_PREPARED = "prepared"
STATE_COMMITTED = "committed"
STATE_CANCELLED = "cancelled"
STATE_APPLIED = "applied"
STATE_COMPLETED = "completed"
STATE_COMPENSATED = "compensated"

app = Flask("stock-service")


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        password=os.environ["REDIS_PASSWORD"],
        db=int(os.environ["REDIS_DB"])
    )


db: redis.Redis = create_redis_client()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

class StockTxValue(Struct):
    item_id: str
    amount: int
    subtracted: bool
    compensated: bool

def tx_key(tx_id: str, item_id: str):
    return f"stock_tx:{tx_id}:{item_id}"


class StockTransaction(Struct):
    item_id: str
    amount: int
    protocol: str
    state: str


def get_item_key(item_id: str) -> str:
    return f"item:{item_id}"


def get_transaction_key(tx_id: str) -> str:
    return f"transaction:{tx_id}"


def decode_entry(entry: bytes | None, entry_type: type[Struct]):
    return msgpack.decode(entry, type=entry_type) if entry else None


def load_legacy_saga_transaction(
    client: redis.Redis | redis.client.Pipeline,
    tx_id: str,
    item_id: str
) -> StockTxValue | None:
    try:
        entry: bytes = client.get(tx_key(tx_id, item_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return decode_entry(entry, StockTxValue)


def load_item(client: redis.Redis | redis.client.Pipeline, item_id: str) -> StockValue:
    try:
        entry: bytes = client.get(get_item_key(item_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    entry = decode_entry(entry, StockValue)
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


def load_transaction(client: redis.Redis | redis.client.Pipeline, tx_id: str) -> StockTransaction | None:
    try:
        entry: bytes = client.get(get_transaction_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return decode_entry(entry, StockTransaction)


def set_item_and_transaction(
    pipe: redis.client.Pipeline,
    item_id: str,
    item_entry: StockValue,
    tx_id: str | None = None,
    tx_entry: StockTransaction | None = None
):
    pipe.multi()
    pipe.set(get_item_key(item_id), msgpack.encode(item_entry))
    if tx_id and tx_entry:
        pipe.set(get_transaction_key(tx_id), msgpack.encode(tx_entry))
    pipe.execute()


def update_transaction(
    pipe: redis.client.Pipeline,
    tx_id: str,
    tx_entry: StockTransaction,
    item_id: str | None = None,
    item_entry: StockValue | None = None
):
    pipe.multi()
    if item_id and item_entry:
        pipe.set(get_item_key(item_id), msgpack.encode(item_entry))
    pipe.set(get_transaction_key(tx_id), msgpack.encode(tx_entry))
    pipe.execute()


def mutate_item(item_id: str, mutator: Callable[[StockValue], None]):
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(get_item_key(item_id))
                item_entry = load_item(pipe, item_id)
                mutator(item_entry)
                pipe.multi()
                pipe.set(get_item_key(item_id), msgpack.encode(item_entry))
                pipe.execute()
                return item_entry
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock update, please retry")


def validate_existing_transaction(
    tx_entry: StockTransaction,
    item_id: str,
    amount: int,
    protocol: str
):
    if tx_entry.item_id != item_id or tx_entry.amount != amount or tx_entry.protocol != protocol:
        abort(409, "Transaction id reused with different parameters")


def execute_queued_command(operation: str, **payload: Any):
    try:
        result = command_queue.execute(operation, **payload)
    except TimeoutError:
        abort(503, "Queue timeout")
    except RuntimeError:
        abort(503, DB_ERROR_STR)
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
    app.logger.debug("Item: %s created", key)
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(get_item_key(key), value)
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
        app.logger.debug("Item: %s stock updated to: %s", item_id, entry.stock)
        if entry.stock < 0:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

    item_entry = mutate_item(item_id, subtract)
    return queued_text(f"Item: {item_id} stock updated to: {item_entry.stock}")


def op_prepare_stock(tx_id: str, item_id: str, amount: int):
    amount = int(amount)
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(get_item_key(item_id), get_transaction_key(tx_id))
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is not None:
                    validate_existing_transaction(tx_entry, item_id, amount, TX_PROTOCOL_2PC)
                    if tx_entry.state in (STATE_PREPARED, STATE_COMMITTED):
                        return queued_text(f"Transaction: {tx_id} already {tx_entry.state}")
                    if tx_entry.state == STATE_CANCELLED:
                        abort(409, f"Transaction: {tx_id} has already been cancelled")
                    abort(409, f"Transaction: {tx_id} cannot be prepared from state {tx_entry.state}")

                item_entry = load_item(pipe, item_id)
                if item_entry.stock < amount:
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
                item_entry.stock -= amount
                tx_entry = StockTransaction(
                    item_id=item_id,
                    amount=amount,
                    protocol=TX_PROTOCOL_2PC,
                    state=STATE_PREPARED
                )
                set_item_and_transaction(pipe, item_id, item_entry, tx_id, tx_entry)
                return queued_text(f"Prepared transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


def op_commit_stock(tx_id: str):
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(get_transaction_key(tx_id))
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    abort(404, f"Transaction: {tx_id} not found!")
                if tx_entry.state == STATE_COMMITTED:
                    return queued_text(f"Transaction: {tx_id} already committed")
                if tx_entry.state != STATE_PREPARED:
                    abort(409, f"Transaction: {tx_id} cannot be committed from state {tx_entry.state}")
                tx_entry.state = STATE_COMMITTED
                update_transaction(pipe, tx_id, tx_entry)
                return queued_text(f"Committed transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


def op_cancel_stock(tx_id: str):
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                tx_key = get_transaction_key(tx_id)
                pipe.watch(tx_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    return queued_text(f"Transaction: {tx_id} already cancelled or never created")
                if tx_entry.state == STATE_CANCELLED:
                    return queued_text(f"Transaction: {tx_id} already cancelled")
                if tx_entry.state == STATE_COMMITTED:
                    abort(409, f"Transaction: {tx_id} has already been committed")
                item_id = tx_entry.item_id
                pipe.watch(get_item_key(item_id))
                item_entry = load_item(pipe, item_id)
                item_entry.stock += tx_entry.amount
                tx_entry.state = STATE_CANCELLED
                update_transaction(pipe, tx_id, tx_entry, item_id, item_entry)
                return queued_text(f"Cancelled transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


def op_execute_stock(tx_id: str, item_id: str, amount: int):
    amount = int(amount)
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(get_item_key(item_id), get_transaction_key(tx_id))
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is not None:
                    validate_existing_transaction(tx_entry, item_id, amount, TX_PROTOCOL_SAGA)
                    if tx_entry.state in (STATE_APPLIED, STATE_COMPLETED):
                        return queued_text(f"Transaction: {tx_id} already {tx_entry.state}")
                    if tx_entry.state == STATE_COMPENSATED:
                        abort(409, f"Transaction: {tx_id} has already been compensated")
                    abort(409, f"Transaction: {tx_id} cannot be executed from state {tx_entry.state}")

                item_entry = load_item(pipe, item_id)
                if item_entry.stock < amount:
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
                item_entry.stock -= amount
                tx_entry = StockTransaction(
                    item_id=item_id,
                    amount=amount,
                    protocol=TX_PROTOCOL_SAGA,
                    state=STATE_APPLIED
                )
                set_item_and_transaction(pipe, item_id, item_entry, tx_id, tx_entry)
                return queued_text(f"Executed transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


def op_complete_stock(tx_id: str):
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(get_transaction_key(tx_id))
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    abort(404, f"Transaction: {tx_id} not found!")
                if tx_entry.state == STATE_COMPLETED:
                    return queued_text(f"Transaction: {tx_id} already completed")
                if tx_entry.state != STATE_APPLIED:
                    abort(409, f"Transaction: {tx_id} cannot be completed from state {tx_entry.state}")
                tx_entry.state = STATE_COMPLETED
                update_transaction(pipe, tx_id, tx_entry)
                return queued_text(f"Completed transaction: {tx_id}")
            except redis.exceptions.WatchError:
                continue
    abort(409, "Concurrent stock transaction, please retry")


def op_compensate_stock(tx_id: str):
    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                tx_key = get_transaction_key(tx_id)
                pipe.watch(tx_key)
                tx_entry = load_transaction(pipe, tx_id)
                if tx_entry is None:
                    return queued_text(f"Transaction: {tx_id} already compensated or never created")
                if tx_entry.state == STATE_COMPENSATED:
                    return queued_text(f"Transaction: {tx_id} already compensated")
                if tx_entry.state not in (STATE_APPLIED, STATE_COMPLETED):
                    abort(409, f"Transaction: {tx_id} cannot be compensated from state {tx_entry.state}")
                item_id = tx_entry.item_id
                pipe.watch(get_item_key(item_id))
                item_entry = load_item(pipe, item_id)
                item_entry.stock += tx_entry.amount
                tx_entry.state = STATE_COMPENSATED
                update_transaction(pipe, tx_id, tx_entry, item_id, item_entry)
                return queued_text(f"Compensated transaction: {tx_id}")
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
        "prepare_stock": op_prepare_stock,
        "commit_stock": op_commit_stock,
        "cancel_stock": op_cancel_stock,
        "execute_stock": op_execute_stock,
        "complete_stock": op_complete_stock,
        "compensate_stock": op_compensate_stock,
    },
    logger=app.logger
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
        item_price=int(item_price)
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


@app.get("/transaction/<tx_id>")
def find_transaction(tx_id: str):
    tx_entry = load_transaction(db, tx_id)
    if tx_entry is None:
        abort(404, f"Transaction: {tx_id} not found!")
    return jsonify(
        {
            "item_id": tx_entry.item_id,
            "amount": tx_entry.amount,
            "protocol": tx_entry.protocol,
            "state": tx_entry.state
        }
    )


@app.post("/transaction/prepare/<tx_id>/<item_id>/<amount>")
def prepare_stock(tx_id: str, item_id: str, amount: int):
    return execute_queued_command("prepare_stock", tx_id=tx_id, item_id=item_id, amount=int(amount))


@app.post("/transaction/commit/<tx_id>")
def commit_stock(tx_id: str):
    return execute_queued_command("commit_stock", tx_id=tx_id)

@app.post('/subtract_tx/<tx_id>/<item_id>/<amount>')
def subtract_stock_tx(tx_id: str, item_id: str, amount: int):
    amount = int(amount)
    item_storage_key = get_item_key(item_id)
    legacy_tx_key = tx_key(tx_id, item_id)

    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(item_storage_key, legacy_tx_key)
                tx_entry = load_legacy_saga_transaction(pipe, tx_id, item_id)
                if tx_entry is not None:
                    if tx_entry.amount != amount:
                        abort(409, "Transaction id reused with different parameters")
                    if tx_entry.compensated:
                        abort(409, f"Transaction: {tx_id} has already been compensated")
                    if tx_entry.subtracted:
                        return Response("Stock already subtracted (idempotent)", status=200)

                item_entry = load_item(pipe, item_id)
                if item_entry.stock < amount:
                    abort(400, f"Item: {item_id} out of stock")

                item_entry.stock -= amount
                pipe.multi()
                pipe.set(item_storage_key, msgpack.encode(item_entry))
                pipe.set(
                    legacy_tx_key,
                    msgpack.encode(
                        StockTxValue(item_id=item_id, amount=amount, subtracted=True, compensated=False)
                    )
                )
                pipe.execute()
                return Response("Stock subtraction successful", status=200)
            except redis.exceptions.WatchError:
                continue

    abort(409, "Concurrent stock transaction, please retry")



@app.post('/add_tx/<tx_id>/<item_id>')
def add_stock_tx(tx_id: str, item_id: str):
    item_storage_key = get_item_key(item_id)
    legacy_tx_key = tx_key(tx_id, item_id)

    for _ in range(MAX_RETRIES):
        with db.pipeline() as pipe:
            try:
                pipe.watch(item_storage_key, legacy_tx_key)
                tx_entry = load_legacy_saga_transaction(pipe, tx_id, item_id)
                if tx_entry is None:
                    abort(400, "Unknown tx_id")
                if tx_entry.compensated:
                    return Response("Stock already compensated (idempotent)", status=200)
                if not tx_entry.subtracted:
                    abort(409, f"Transaction: {tx_id} has not subtracted stock")

                item_entry = load_item(pipe, tx_entry.item_id)
                item_entry.stock += tx_entry.amount
                tx_entry.compensated = True

                pipe.multi()
                pipe.set(get_item_key(tx_entry.item_id), msgpack.encode(item_entry))
                pipe.set(legacy_tx_key, msgpack.encode(tx_entry))
                pipe.execute()
                return Response("Stock compensation successful", status=200)
            except redis.exceptions.WatchError:
                continue

    abort(409, "Concurrent stock transaction, please retry")

@app.post("/transaction/cancel/<tx_id>")
def cancel_stock(tx_id: str):
    return execute_queued_command("cancel_stock", tx_id=tx_id)


@app.post("/transaction/execute/<tx_id>/<item_id>/<amount>")
def execute_stock(tx_id: str, item_id: str, amount: int):
    return execute_queued_command("execute_stock", tx_id=tx_id, item_id=item_id, amount=int(amount))


@app.post("/transaction/complete/<tx_id>")
def complete_stock(tx_id: str):
    return execute_queued_command("complete_stock", tx_id=tx_id)


@app.post("/transaction/compensate/<tx_id>")
def compensate_stock(tx_id: str):
    return execute_queued_command("compensate_stock", tx_id=tx_id)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
