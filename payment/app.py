import logging
import os
import atexit
import uuid
from typing import Any

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from orchestrator import RedisCommandQueue, queued_json, queued_text

# curl.exe -s -X POST http://localhost:8000/payment/create_user
# curl.exe -s -X POST http://localhost:8000/payment/add_funds/<NEW_USER_ID>/100
# curl.exe -s http://localhost:8000/payment/find_user/<NEW_USER_ID>

# curl.exe -s -X POST http://localhost:8000/payment/add_funds/<>/100

# curl.exe -s http://localhost:8000/payment/find_user/<>

# SAGA 
# curl.exe -s -X POST http://localhost:8000/payment/saga/pay/<>/30/saga-001

# curl.exe -s http://localhost:8000/payment/find_user/<>

# curl.exe -s -X POST http://localhost:8000/payment/saga/compensate/pay/<>/30/saga-001
# curl.exe -s http://localhost:8000/payment/find_user/<>

# 2PC

# curl.exe -s -X POST http://localhost:8000/payment/2pc/prepare/<>/40/tx-001

# curl.exe -s http://localhost:8000/payment/find_user/<>

# curl.exe -s -X POST http://localhost:8000/payment/2pc/commit/tx-001
# curl.exe -s http://localhost:8000/payment/find_user/<>

# curl.exe -s -X POST http://localhost:8000/payment/2pc/prepare/<>/40/tx-002
# curl.exe -s -X POST http://localhost:8000/payment/2pc/abort/tx-002
# curl.exe -s http://localhost:8000/payment/find_user/<>


DB_ERROR_STR = "DB error"
WAL_STREAM = "wal:payment"
WAL_ENTRY_PREFIX = "wal:entry:"

app = Flask("payment-service")

def create_redis_client() -> redis.Redis:
    return redis.Redis(host=os.environ['REDIS_HOST'],
                       port=int(os.environ['REDIS_PORT']),
                       password=os.environ['REDIS_PASSWORD'],
                       db=int(os.environ['REDIS_DB']))

db: redis.Redis = create_redis_client()

def close_db_connection():
    db.close()

atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int

# stores 2PC prepare stuff so this are saved in redis and commit can read even if reset between prepare/commit
class PrepareRecord(Struct):
    user_id: str
    amount: int
      
class TxValue(Struct):
    paid: bool
    refunded: bool
    user_id: str
    amount: int

def tx_key(tx_id: str) -> str:
    return f"tx:{tx_id}"

#this is for 2PC lock release due to 
_RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

def get_user_from_db(user_id: str) -> UserValue:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


# Write Ahead log as seen in the html file so before mutation on data, you record it as pending. afterwards make it done. If crash-restart replay 

def wal_append(op: str, ref_id: str, user_id: str = "", amount: int = 0) -> str:
    eid = str(uuid.uuid4())
    pipe = db.pipeline(transaction=True)
    pipe.hset(f"{WAL_ENTRY_PREFIX}{eid}", mapping={
        "op": op,
        "user_id": user_id,
        "amount": str(amount),
        "ref_id": ref_id,
        "status": "pending",
    })
    pipe.xadd(WAL_STREAM, {"eid": eid})
    pipe.execute()
    return eid

# this just makes it from pending to done
def wal_done(eid: str):
    db.hset(f"{WAL_ENTRY_PREFIX}{eid}", "status", "done")

# SAGAG

# this uses optmistic locking 
def _apply_pay(user_id: str, amount: int, done_key: str) -> bool:
    for _ in range(10):
        try:
            # uses watch
            with db.pipeline() as pipe:
                pipe.watch(user_id, done_key)
                if pipe.exists(done_key):
                    pipe.reset()
                    return False  # already processed 
                raw = pipe.get(user_id) # read users data
                if not raw:
                    pipe.reset()
                    abort(400, f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                new_credit = user_entry.credit - amount
                if new_credit < 0:
                    pipe.reset()
                    abort(400, f"User: {user_id} insufficient credit!")
                pipe.multi()
                pipe.set(user_id, msgpack.encode(UserValue(credit=new_credit))) 
                pipe.set(done_key, "1", ex=86400)  # idempotency key
                pipe.execute()
                return True
        except redis.WatchError:
            continue
    abort(500, "whomp whomp")

def _apply_refund(user_id: str, amount: int, comp_key: str) -> bool:
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id, comp_key) # basically see if there are changes
                if pipe.exists(comp_key):
                    pipe.reset()
                    return False
                raw = pipe.get(user_id)
                if not raw:
                    pipe.reset()
                    abort(400, f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                pipe.multi()
                pipe.set(user_id, msgpack.encode(UserValue(credit=user_entry.credit + amount)))
                pipe.set(comp_key, "1", ex=86400)
                pipe.execute()
                return True
        except redis.WatchError:
            continue
    abort(500, "whomp whomp")


# 2PC
def _apply_2pc_commit(tx_id: str):
    prepare_key = f"prepare:{tx_id}"
    prepare_raw = db.get(prepare_key)
    if not prepare_raw:
        return  
    record = msgpack.decode(prepare_raw, type=PrepareRecord)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(record.user_id, prepare_key)
                if not pipe.exists(prepare_key):
                    pipe.reset()
                    return 
                raw = pipe.get(record.user_id)

                if not raw:
                    pipe.reset()
                    return
                user_entry = msgpack.decode(raw, type=UserValue)
                new_credit = user_entry.credit - record.amount

                if new_credit < 0:
                    pipe.reset()
                    return  
                pipe.multi()
                pipe.set(record.user_id, msgpack.encode(UserValue(credit=new_credit)))
                pipe.delete(prepare_key)
                pipe.execute()
                db.eval(_RELEASE_LOCK_SCRIPT, 1, f"lock:{record.user_id}", tx_id)
                return
        except redis.WatchError:
            continue
    abort(500, "dun dun dun")

# deletes prepare record and locks are released
def _apply_2pc_abort(tx_id: str):
    prepare_key = f"prepare:{tx_id}"
    prepare_raw = db.get(prepare_key)
    if not prepare_raw:
        return  
    record = msgpack.decode(prepare_raw, type=PrepareRecord)
    db.delete(prepare_key)
    db.eval(_RELEASE_LOCK_SCRIPT, 1, f"lock:{record.user_id}", tx_id)


# Crash Revory for Write ahead log
# this is so it checks the log for pending and replays them basically 
def recover_from_wal():
    try:
        entries = db.xrange(WAL_STREAM)
    except redis.exceptions.RedisError:
        app.logger.warning("y")
        return
    for _, fields in entries:
        eid = fields.get(b"eid", b"").decode()
        if not eid:
            continue
        entry = db.hgetall(f"{WAL_ENTRY_PREFIX}{eid}")
        if not entry or entry.get(b"status", b"").decode() != "pending":
            continue
        op = entry.get(b"op", b"").decode()
        user_id = entry.get(b"user_id", b"").decode()
        amount = int(entry.get(b"amount", b"0").decode())
        ref_id = entry.get(b"ref_id", b"").decode()
        app.logger.info(f"WAL recovery: replaying op={op} ref={ref_id}")

        try:
            if op == "saga_pay":
                _apply_pay(user_id, amount, f"saga:done:{ref_id}")
            elif op == "saga_compensate":
                _apply_refund(user_id, amount, f"saga:comp:{ref_id}")
            elif op == "2pc_commit":
                _apply_2pc_commit(ref_id)
            elif op == "2pc_abort":
                _apply_2pc_abort(ref_id)

        except Exception as e:
            app.logger.error(f"WAL eid={eid}: {e}")
            continue
        wal_done(eid)

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


def op_create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_json({'user_id': key})


def op_batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_json({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


def op_add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_text(f"User: {user_id} credit updated to: {user_entry.credit}")


def op_remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return queued_text(f"User: {user_id} credit updated to: {user_entry.credit}")

def op_pay_tx(tx_id: str, user_id: str, amount: int):
    amount = int(amount)

    # If we've seen this tx before, return the same outcome (idempotent)
    try:
        raw = db.get(tx_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if raw:
        tx = msgpack.decode(raw, type=TxValue)
        if tx.paid:
            return queued_text("Already paid (idempotent)")
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
            abort(400, DB_ERROR_STR)
        abort(400, "User out of credit")

    try:
        db.set(user_id, msgpack.encode(user_entry))
        db.set(tx_key(tx_id), msgpack.encode(TxValue(
            paid=True, refunded=False, user_id=user_id, amount=amount
        )))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return queued_text("Payment successful")

def op_refund_tx(tx_id: str):
    # Refund based on stored tx record (safer: no user/amount params needed)
    try:
        raw = db.get(tx_key(tx_id))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    if not raw:
        abort(400, "Unknown tx_id")

    tx = msgpack.decode(raw, type=TxValue)

    # If never paid, refund is a no-op/fail depending on your preference
    if not tx.paid:
        abort(400, "Cannot refund: tx not paid")

    if tx.refunded:
        return queued_text("Already refunded (idempotent)")

    user_entry: UserValue = get_user_from_db(tx.user_id)
    user_entry.credit += tx.amount

    try:
        db.set(tx.user_id, msgpack.encode(user_entry))
        tx.refunded = True
        db.set(tx_key(tx_id), msgpack.encode(tx))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return queued_text("Refund successful")



# Endpoints added but for SAGA

def op_saga_pay(user_id: str, amount: int, saga_id: str):
    done_key = f"saga:done:{saga_id}"
    eid = wal_append("saga_pay", saga_id, user_id, int(amount))
    already_done = not _apply_pay(user_id, int(amount), done_key)
    wal_done(eid)
    if already_done:
        return queued_text(f"Already processed saga: {saga_id}")
    return queued_text(f"User: {user_id} charged {amount}")


def op_saga_compensate_pay(user_id: str, amount: int, saga_id: str):
    comp_key = f"saga:comp:{saga_id}"
    eid = wal_append("saga_compensate", saga_id, user_id, int(amount))
    already_done = not _apply_refund(user_id, int(amount), comp_key)
    wal_done(eid)
    if already_done:
        return queued_text(f"Already compensated saga: {saga_id}")
    return queued_text(f"User: {user_id} refunded {amount}")


# 2PC endpoints added 

def op_twopc_prepare(user_id: str, amount: int, tx_id: str):
    amount = int(amount)
    lock_key = f"lock:{user_id}"
    acquired = db.set(lock_key, tx_id, nx=True, ex=30)
    if not acquired:
        abort(400, f"User: {user_id} is locked by another transaction")
    user_entry = get_user_from_db(user_id)
    if user_entry.credit < amount:
        db.eval(_RELEASE_LOCK_SCRIPT, 1, lock_key, tx_id)
        abort(400, f"User: {user_id} insufficient credit (has {user_entry.credit}, needs {amount})")
    try:
        db.set(f"prepare:{tx_id}", msgpack.encode(PrepareRecord(user_id=user_id, amount=amount)), ex=60)
    except redis.exceptions.RedisError:
        db.eval(_RELEASE_LOCK_SCRIPT, 1, lock_key, tx_id)
        abort(400, DB_ERROR_STR)
    return queued_json({"status": "prepared", "tx_id": tx_id})


def op_twopc_commit(tx_id: str):
    eid = wal_append("2pc_commit", tx_id)
    _apply_2pc_commit(tx_id)
    wal_done(eid)
    return queued_json({"status": "committed", "tx_id": tx_id})


def op_twopc_abort(tx_id: str):
    eid = wal_append("2pc_abort", tx_id)
    _apply_2pc_abort(tx_id)
    wal_done(eid)
    return queued_json({"status": "aborted", "tx_id": tx_id})


command_queue = RedisCommandQueue(
    name="payment-service",
    redis_factory=create_redis_client,
    handlers={
        "create_user": op_create_user,
        "batch_init_users": op_batch_init_users,
        "add_credit": op_add_credit,
        "remove_credit": op_remove_credit,
        "pay_tx": op_pay_tx,
        "refund_tx": op_refund_tx,
        "saga_pay": op_saga_pay,
        "saga_compensate_pay": op_saga_compensate_pay,
        "twopc_prepare": op_twopc_prepare,
        "twopc_commit": op_twopc_commit,
        "twopc_abort": op_twopc_abort,
    },
    logger=app.logger
)


@app.post('/create_user')
def create_user():
    return execute_queued_command("create_user")


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    return execute_queued_command("batch_init_users", n=int(n), starting_money=int(starting_money))


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    return execute_queued_command("add_credit", user_id=user_id, amount=int(amount))


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    return execute_queued_command("remove_credit", user_id=user_id, amount=int(amount))


@app.post('/pay_tx/<tx_id>/<user_id>/<amount>')
def pay_tx(tx_id: str, user_id: str, amount: int):
    return execute_queued_command("pay_tx", tx_id=tx_id, user_id=user_id, amount=int(amount))


@app.post('/refund_tx/<tx_id>')
def refund_tx(tx_id: str):
    return execute_queued_command("refund_tx", tx_id=tx_id)


@app.post('/saga/pay/<user_id>/<amount>/<saga_id>')
def saga_pay(user_id: str, amount: int, saga_id: str):
    return execute_queued_command("saga_pay", user_id=user_id, amount=int(amount), saga_id=saga_id)


@app.post('/saga/compensate/pay/<user_id>/<amount>/<saga_id>')
def saga_compensate_pay(user_id: str, amount: int, saga_id: str):
    return execute_queued_command("saga_compensate_pay", user_id=user_id, amount=int(amount), saga_id=saga_id)


@app.post('/2pc/prepare/<user_id>/<amount>/<tx_id>')
def twopc_prepare(user_id: str, amount: int, tx_id: str):
    return execute_queued_command("twopc_prepare", user_id=user_id, amount=int(amount), tx_id=tx_id)


@app.post('/2pc/commit/<tx_id>')
def twopc_commit(tx_id: str):
    return execute_queued_command("twopc_commit", tx_id=tx_id)


@app.post('/2pc/abort/<tx_id>')
def twopc_abort(tx_id: str):
    return execute_queued_command("twopc_abort", tx_id=tx_id)

if __name__ == '__main__':
    recover_from_wal()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    recover_from_wal()
