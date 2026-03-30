import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int

class TxValue(Struct):
    paid: bool
    refunded: bool
    user_id: str
    amount: int

def tx_key(tx_id: str) -> str:
    return f"tx:{tx_id}"

def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.post('/pay_tx/<tx_id>/<user_id>/<amount>')
def pay_tx(tx_id: str, user_id: str, amount: int):
    amount = int(amount)

    # If we've seen this tx before, return the same outcome (idempotent)
    try:
        raw = db.get(tx_key(tx_id))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    if raw:
        tx = msgpack.decode(raw, type=TxValue)
        if tx.paid:
            return Response("Already paid (idempotent)", status=200)
        else:
            abort(400, "Previous payment attempt failed")

    # First time seeing this tx: attempt to charge user
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= amount
    if user_entry.credit < 0:
        # Store a tx record so retries behave consistently
        try:
            db.set(tx_key(tx_id), msgpack.encode(TxValue(
                paid=False, refunded=False, user_id=user_id, amount=amount
            )))
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        abort(400, "User out of credit")

    try:
        db.set(user_id, msgpack.encode(user_entry))
        db.set(tx_key(tx_id), msgpack.encode(TxValue(
            paid=True, refunded=False, user_id=user_id, amount=amount
        )))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return Response("Payment successful", status=200)

@app.post('/refund_tx/<tx_id>')
def refund_tx(tx_id: str):
    # Refund based on stored tx record (safer: no user/amount params needed)
    try:
        raw = db.get(tx_key(tx_id))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    if not raw:
        abort(400, "Unknown tx_id")

    tx = msgpack.decode(raw, type=TxValue)

    # If never paid, refund is a no-op/fail depending on your preference
    if not tx.paid:
        abort(400, "Cannot refund: tx not paid")

    if tx.refunded:
        return Response("Already refunded (idempotent)", status=200)

    user_entry: UserValue = get_user_from_db(tx.user_id)
    user_entry.credit += tx.amount
 
    try:
        db.set(tx.user_id, msgpack.encode(user_entry))
        tx.refunded = True
        db.set(tx_key(tx_id), msgpack.encode(tx))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return Response("Refund successful", status=200)



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
