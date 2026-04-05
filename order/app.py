import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Checkout test commands
# curl -X POST http://localhost:8000/payment/create_user
# curl -X POST http://localhost:8000/payment/add_funds/USER_ID/1000
# curl -X POST http://localhost:8000/stock/item/create/10
# curl -X POST http://localhost:8000/stock/add/ITEM_ID/50
# curl -X POST http://localhost:8000/orders/create/USER_ID
# curl -X POST http://localhost:8000/orders/addItem/ORDER_ID/ITEM_ID/2
# curl -X POST http://localhost:8000/orders/checkout/ORDER_ID

# checking that payment did not decrease (refund): curl -i http://localhost:8000/payment/find_user/USER_ID

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
TX_MODE = os.getenv("TX_MODE", "SAGA").upper()
if TX_MODE not in ("SAGA", "2PC"):
    raise ValueError(f"Invalid TX_MODE={TX_MODE}. Use SAGA or 2PC.")


app = Flask("order-service")
app.logger.info(f"Transaction mode: {TX_MODE}")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class SagaState(Struct):
    order_id: str
    tx_id: str
    state: str
    removed_items: list[tuple[str, int]]
    
    
class TxLog(Struct):
    tx_id: str
    order_id: str
    phase: str
    stock_votes: dict[str, str]
    payment_vote: str
    
    
def item_tx_id(tx_id: str, item_id: str) -> str:
    return f"{tx_id}:{item_id}"
    
    
def wal_key(tx_id: str) -> str:
    return f"2pc:{tx_id}"


def wal_write(tx_log: TxLog):
    """Persist transaction state before acting on it (write-ahead)."""
    try:
        db.set(wal_key(tx_log.tx_id), msgpack.encode(tx_log), ex=300)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def wal_read(tx_id: str) -> TxLog | None:
    """Read back a transaction log entry, returns None if not found."""
    try:
        raw = db.get(wal_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(raw, type=TxLog) if raw else None


def recover_incomplete_2pc():
    app.logger.info("Scanning for incomplete 2PC transactions...")
    try:
        for key in db.scan_iter("2pc:*"):
            raw = db.get(key)
            if not raw:
                continue
            log = msgpack.decode(raw, type=TxLog)
            if log.phase not in ("COMMITTED", "ABORTED"):
                app.logger.warning(
                    f"[2PC] In-doubt tx_id={log.tx_id} order_id={log.order_id} "
                    f"phase={log.phase} stock_votes={log.stock_votes} "
                    f"payment={log.payment_vote}"
                )
    except redis.exceptions.RedisError:
        app.logger.error("[2PC] Failed to scan WAL records during startup")


def saga_key(tx_id: str):
    return f"saga:{tx_id}"

def save_saga(saga: SagaState):
    try:
        db.set(saga_key(saga.tx_id), msgpack.encode(saga))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def load_saga(key: str) -> SagaState | None:
    try:
        raw = db.get(key)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(raw, type=SagaState) if raw else None

def recover_incomplete_sagas():
    app.logger.info("Scanning for incomplete sagas...")

    try:
        for key in db.scan_iter("saga:*"):
            raw = db.get(key)
            if not raw:
                continue

            saga = msgpack.decode(raw, type=SagaState)

            if saga.state in ("COMPLETED", "COMPENSATED"):
                continue

            app.logger.warning(
                f"[SAGA-RECOVERY] tx_id={saga.tx_id} state={saga.state} order_id={saga.order_id}"
            )

            # Case 1: payment and stock already done, but order may not have been marked paid
            if saga.state == "STOCK_DONE":
                order_entry = get_order_from_db(saga.order_id)
                if not order_entry.paid:
                    order_entry.paid = True
                    db.set(saga.order_id, msgpack.encode(order_entry))
                saga.state = "COMPLETED"
                db.set(saga_key(saga.tx_id), msgpack.encode(saga))
                app.logger.info(f"[SAGA-RECOVERY] Completed order {saga.order_id}")
                continue

            # Case 2: anything after payment started but before completion -> compensate
            if saga.state in ("PAYING", "PAYMENT_DONE", "SUBTRACTING"):
                for removed_item_id, _quantity in saga.removed_items:
                    send_post_request(f"{GATEWAY_URL}/stock/add_tx/{saga.tx_id}/{removed_item_id}")

                refund_reply = send_post_request(f"{GATEWAY_URL}/payment/refund_tx/{saga.tx_id}")
                # refund_tx is idempotent; even if it was already refunded this is safe enough

                saga.state = "COMPENSATED"
                db.set(saga_key(saga.tx_id), msgpack.encode(saga))
                app.logger.info(f"[SAGA-RECOVERY] Compensated tx_id={saga.tx_id}")
                continue

            # Case 3: started but no external effects confirmed
            if saga.state == "STARTED":
                saga.state = "COMPENSATED"
                db.set(saga_key(saga.tx_id), msgpack.encode(saga))
                app.logger.info(f"[SAGA-RECOVERY] Marked STARTED saga as compensated tx_id={saga.tx_id}")

    except redis.exceptions.RedisError:
        app.logger.error("Failed to scan saga records during startup")


def run_startup_recovery():
    if TX_MODE == "SAGA":
        recover_incomplete_sagas()
    else:
        recover_incomplete_2pc()



def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.info(f"checkout called for order_id={order_id} with TX_MODE={TX_MODE}")
    if TX_MODE == "SAGA":
        app.logger.info("Entering SAGA checkout")
        return checkout_saga(order_id)
    else:  # "2PC"
        app.logger.info("Entering 2PC checkout")
        return checkout_2pc(order_id)
    
def checkout_saga(order_id: str):
    app.logger.info(f"[SAGA] Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    # If already paid, make checkout idempotent at order level too
    if order_entry.paid:
        return Response("Already paid (idempotent)", status=200)

    # Group quantities per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    tx_id = str(uuid.uuid4())
    saga = SagaState(
        order_id=order_id,
        tx_id=tx_id,
        state="STARTED",
        removed_items=[]
    )
    save_saga(saga)
    app.logger.info(f"[SAGA] order_id={order_id} tx_id={tx_id}")

    # 1) Mark that we are about to call payment
    saga.state = "PAYING"
    save_saga(saga)

    # 2) Pay first
    pay_reply = send_post_request(
        f"{GATEWAY_URL}/payment/pay_tx/{tx_id}/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if pay_reply.status_code != 200:
        saga.state = "COMPENSATED"
        save_saga(saga)
        abort(400, "Payment failed")

    # 3) Payment succeeded
    saga.state = "PAYMENT_DONE"
    save_saga(saga)

    # 4) Mark that we are about to start stock subtraction
    saga.state = "SUBTRACTING"
    save_saga(saga)

    # 5) Subtract stock item by item
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/subtract_tx/{tx_id}/{item_id}/{quantity}"
        )

        if stock_reply.status_code != 200:
            # compensate only what was already removed
            for removed_item_id, _quantity in saga.removed_items:
                send_post_request(
                    f"{GATEWAY_URL}/stock/add_tx/{tx_id}/{removed_item_id}"
                )

            send_post_request(f"{GATEWAY_URL}/payment/refund_tx/{tx_id}")

            saga.state = "COMPENSATED"
            save_saga(saga)
            abort(400, f"Out of stock on item_id: {item_id}")

        # record each successful stock subtraction immediately
        saga.removed_items.append((item_id, quantity))
        save_saga(saga)

    # 6) All stock steps finished
    saga.state = "STOCK_DONE"
    save_saga(saga)

    # 7) Finalize order
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    # 8) Final state
    saga.state = "COMPLETED"
    save_saga(saga)

    return Response("Checkout successful (SAGA)", status=200)


def checkout_2pc(order_id: str):
    app.logger.info(f"[2PC] Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    if order_entry.paid:
        return Response("Already paid (idempotent)", status=200)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    tx_id = str(uuid.uuid4())

    log = TxLog(
        tx_id=tx_id,
        order_id=order_id,
        phase="PREPARING",
        stock_votes={item_id: "PENDING" for item_id in items_quantities},
        payment_vote="PENDING"
    )
    wal_write(log)

    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(
            f"{GATEWAY_URL}/stock/transaction/prepare/{item_tx_id(tx_id, item_id)}/{item_id}/{quantity}"
        )
        log.stock_votes[item_id] = "YES" if stock_reply.status_code == 200 else "NO"
        wal_write(log)
        if log.stock_votes[item_id] == "NO":
            break

    if all(v == "YES" for v in log.stock_votes.values()):
        payment_reply = send_post_request(
            f"{GATEWAY_URL}/payment/2pc/prepare/{order_entry.user_id}/{order_entry.total_cost}/{tx_id}"
        )
        log.payment_vote = "YES" if payment_reply.status_code == 200 else "NO"
        wal_write(log)

    log.phase = "PREPARED"
    wal_write(log)

    all_yes = (
        all(v == "YES" for v in log.stock_votes.values())
        and log.payment_vote == "YES"
    )

    if not all_yes:
        log.phase = "ABORTING"
        wal_write(log)

        for item_id, vote in log.stock_votes.items():
            if vote == "YES":
                send_post_request(
                    f"{GATEWAY_URL}/stock/transaction/cancel/{item_tx_id(tx_id, item_id)}"
                )

        if log.payment_vote == "YES":
            send_post_request(f"{GATEWAY_URL}/payment/2pc/abort/{tx_id}")

        log.phase = "ABORTED"
        wal_write(log)
        abort(400, f"2PC aborted: stock_votes={log.stock_votes} payment={log.payment_vote}")

    log.phase = "COMMITTING"
    wal_write(log)

    for item_id in items_quantities:
        send_post_request(
            f"{GATEWAY_URL}/stock/transaction/commit/{item_tx_id(tx_id, item_id)}"
        )

    send_post_request(f"{GATEWAY_URL}/payment/2pc/commit/{tx_id}")

    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    log.phase = "COMMITTED"
    wal_write(log)

    return Response("Checkout successful (2PC)", status=200)

with app.app_context():
    run_startup_recovery()
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
