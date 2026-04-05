# Distributed Data Systems Project

This project implements a small e-commerce workflow with four services:

- `order-service`
- `stock-service`
- `payment-service`
- `orchestrator-service`

The main architectural goal of this version is that `SAGA` and `2PC` are not implemented inside the business services anymore. The orchestration protocol lives in a separate software artifact, the orchestrator.

## High-Level Overview

The system models a checkout flow:

1. A user creates an order in `order-service`.
2. The order references one or more items managed by `stock-service`.
3. The user balance is managed by `payment-service`.
4. Checkout is delegated to `orchestrator-service`.

At a high level:

- `order-service` owns order data and basic order APIs.
- `stock-service` owns stock quantities and item prices.
- `payment-service` owns user credit.
- `orchestrator-service` coordinates distributed checkout across services.

The gateway exposes all services on port `8000` and routes requests to the right backend.

## What Was Abstracted Away

The important design change in Phase 2 is not that the services became stateless. The important change is that protocol decisions moved out of them.

Before the refactoring, the business services could contain logic such as:

- when to call another service
- in which order remote calls should happen
- whether a failure means "abort" or "compensate"
- whether the transaction is running as `SAGA` or `2PC`
- how to recover an unfinished distributed checkout

After the refactoring, that logic belongs only to the orchestrator.

This means:

- `order-service` no longer decides how checkout is executed
- `stock-service` no longer exposes protocol-specific endpoints such as `/saga/...` or `/2pc/...`
- `payment-service` no longer exposes protocol-specific endpoints such as `/saga/...` or `/2pc/...`
- only the orchestrator knows the global coordination policy

What remains inside `stock-service` and `payment-service` is local transaction handling. That is expected and necessary. A participant still has to know how to:

- reserve its own local resource
- persist the local transaction record
- commit that local reservation
- roll it back safely

So the business services still contain transaction state, but they do not contain distributed transaction orchestration anymore.

## Why The Orchestrator Exists

The orchestrator centralizes distributed transaction policy.

Without it, the protocol logic leaks into application code:

- the order service decides when to call payment
- the order service decides when to subtract stock
- the participants expose protocol-specific endpoints like `/2pc/...` or `/saga/...`

In this refactoring, that protocol knowledge is removed from the business services.

Now:

- `order-service` delegates checkout to the orchestrator
- `stock-service` exposes neutral local transaction operations
- `payment-service` exposes neutral local transaction operations
- only `orchestrator-service` knows whether the global checkout runs as `SAGA` or `2PC`

## Service Responsibilities

### `order-service`

`order-service` stores:

- order id
- paid flag
- ordered items
- user id
- total cost

It exposes APIs for:

- creating orders
- adding items to orders
- retrieving orders
- delegating checkout to the orchestrator

It does not implement `SAGA` or `2PC`.

Its role is now intentionally narrow:

- maintain order data
- expose the public order API required by the assignment
- ask the orchestrator to perform checkout

In other words, `order-service` is now a business service again, not a transaction coordinator.

### `stock-service`

`stock-service` stores:

- stock quantity per item
- item price
- local stock transaction state

It exposes:

- direct item and stock APIs
- neutral transaction lifecycle APIs:
  - `POST /transactions/start/<tx_id>/<item_id>/<amount>`
  - `POST /transactions/commit/<tx_id>`
  - `POST /transactions/rollback/<tx_id>`

The stock service does not know whether a given local transaction belongs to a `SAGA` or a `2PC` workflow. It only knows how to reserve stock, commit it, or roll it back.

This is an important distinction:

- the stock service knows how to protect its own database
- the stock service does not know the global workflow policy

It therefore behaves like a participant resource manager:

- `start` means "reserve stock locally"
- `commit` means "make the reservation final"
- `rollback` means "undo the reservation"

### `payment-service`

`payment-service` stores:

- user credit
- local payment transaction state

It exposes:

- direct user credit APIs
- neutral transaction lifecycle APIs:
  - `POST /transactions/start/<tx_id>/<user_id>/<amount>`
  - `POST /transactions/commit/<tx_id>`
  - `POST /transactions/rollback/<tx_id>`
 
Like stock, payment does not know whether the orchestrator is running `SAGA` or `2PC`.

Its responsibility is purely local:

- verify sufficient credit
- reserve the debit locally
- commit that debit
- refund it on rollback

It does not decide when compensation should happen. That decision belongs to the orchestrator.

### `orchestrator-service`

`orchestrator-service` is the only service that knows:

- which protocol is active
- the current global workflow state
- which participant steps already started
- which steps committed
- which steps must be rolled back

It stores:

- a durable workflow record in Redis
- per-order active workflow ownership
- workflow execution locks

It exposes:

- `POST /checkout/<order_id>`
- `POST /workflow/<workflow_id>/resume`
- `GET /workflow/<workflow_id>`

This makes the orchestrator the single distributed transaction brain of the system.

At a conceptual level, the orchestrator is responsible for:

- choosing the protocol
- creating a workflow record for each checkout
- deciding which participant to contact next
- translating protocol rules into neutral participant operations
- deciding whether a failure means retry, rollback, or terminal failure
- resuming unfinished workflows after failures
- finalizing the order only when the workflow has reached a safe point

The orchestrator therefore separates business data ownership from coordination logic:

- `order`, `stock`, and `payment` own domain state
- `orchestrator` owns workflow state

## How The Orchestrator Works

### Workflow State

For every checkout, the orchestrator creates a workflow record containing:

- `workflow_id`
- `order_id`
- `mode`
- `state`
- `user_id`
- `total_cost`
- aggregated items
- per-item stock step state
- payment step state
- whether the order has already been finalized
- an error message if the workflow failed

This record is stored in Redis so progress survives service restarts.

This workflow record is what allows the orchestrator to act like a real coordinator instead of a stateless HTTP proxy. It is not simply forwarding requests. It is remembering:

- what has already happened
- what still needs to happen
- what must be undone if recovery is needed

### Local Transaction Model

The orchestrator treats stock and payment as local participants with the same three-step contract:

1. `start`
2. `commit`
3. `rollback`

That uniform contract is what abstracts the protocol away from the participants.

This is the key abstraction boundary in the project.

The participants expose one generic local contract:

1. reserve local state with `start`
2. make that reservation durable with `commit`
3. undo that reservation with `rollback`

The orchestrator then maps the global protocol onto that contract.

Because of this mapping:

- the same `stock-service` and `payment-service` can be used under `SAGA`
- the same `stock-service` and `payment-service` can be used under `2PC`
- no participant endpoint needs to be renamed when the protocol changes

### SAGA Mode

In `SAGA` mode the orchestrator does this:

1. Start payment transaction.
2. Commit payment transaction.
3. Start stock transactions for all items.
4. If stock start fails, roll back payment and any already-started stock transactions.
5. If all stock starts succeed, commit stock transactions.
6. Mark the order as paid.

This makes payment happen first and compensation happen if the rest of the workflow cannot complete.

The important point is that `payment-service` does not know it is being used in a saga. It only sees:

- `start`
- `commit`
- possibly later `rollback`

The meaning of those steps as "forward action" and "compensation" exists only in the orchestrator.

### 2PC Mode

In `2PC` mode the orchestrator does this:

1. Start all stock transactions.
2. Start payment transaction.
3. If any start fails, roll back every started participant.
4. If all starts succeed, commit stock.
5. Commit payment.
6. Mark the order as paid.

The participants do not know this is `2PC`. They only receive `start`, `commit`, and `rollback`.

Again, this is the abstraction in action.

The orchestrator interprets:

- `start` as the preparation phase
- `commit` as the final commit phase
- `rollback` as the abort phase

But the participant services do not need protocol-specific branching for that.

### Order Finalization

The order is marked as paid only after the participant commits have succeeded.

That matters because if a participant fails before commit persistence, the orchestrator should be able to recover without leaving an order marked paid while stock or payment is still unresolved.

This is one of the orchestrator’s most important roles: it enforces the boundary between "participant progress" and "business-visible completion".

In practice:

- participants finish their part first
- the orchestrator observes that safe point
- only then is the order marked paid

## Fault Tolerance Model

This design is intended to tolerate participant service failures.

### What Is Durable

- orders are stored in Redis
- stock data and stock local transactions are stored in Redis
- payment data and payment local transactions are stored in Redis
- orchestrator workflow state is stored in Redis

Because the Redis containers use persistent volumes in `docker-compose`, container restarts do not wipe state by default.

### What Happens If `stock-service` Or `payment-service` Fails

If one participant goes down during checkout:

- the orchestrator keeps the global workflow record
- the participant already-persisted local transaction record remains in Redis
- the orchestrator recovery loop scans unfinished workflows and retries

When the participant comes back:

- the orchestrator resumes the workflow
- retries `commit` or `rollback`
- converges to a terminal state

This works because participant operations are designed to be idempotent:

- repeating `start` with the same `tx_id` is safe
- repeating `commit` is safe
- repeating `rollback` is safe

This is why the orchestrator can be aggressive about retrying. It does not need to guess whether a previous attempt partially succeeded. Instead, it can call the participant again and rely on the participant’s local transaction record to answer safely.

### Recovery Thread

The orchestrator runs a background recovery loop that periodically scans all unfinished workflows and re-drives them.

That means incomplete workflows are not forgotten just because a participant or request path failed.

### Scope Of The Failure Assumption

This setup is primarily designed for:

- `stock-service` failure
- `payment-service` failure
- transient network or request failures between services

If Redis data is lost, durability is lost too. Redis persistence is therefore part of the fault-tolerance story.

## Queue Abstraction

The Redis queue implementation is also abstracted into a shared infrastructure module:

- `common/redis_queue.py`

It is used by:

- `stock-service`
- `payment-service`
- `orchestrator-service`

This keeps queue mechanics out of the business logic and avoids duplicating queue code across services.

The queue is an infrastructure detail, not part of the business protocol. That is why it is also abstracted into a shared module.

So there are two separate abstractions in this project:

- protocol abstraction:
  - `SAGA` and `2PC` are centralized in the orchestrator
- infrastructure abstraction:
  - Redis queue behavior is centralized in `common/redis_queue.py`

## Project Structure

- `common`
  Shared infrastructure code used by multiple services.
- `env`
  Environment files for Redis-backed local deployment.
- `order`
  Order service application and Dockerfile.
- `payment`
  Payment service application and Dockerfile.
- `stock`
  Stock service application and Dockerfile.
- `orchestrator`
  Orchestrator application and Dockerfile.
- `test`
  Tests for the system.
- `docker-compose.yml`
  Local multi-service deployment.
- `gateway_nginx.conf`
  Reverse proxy configuration.

## Running The System

From the project root:

```bash
docker compose down -v
docker compose up --build
```

This starts:

- gateway on `http://localhost:8000`
- order service
- stock service
- payment service
- orchestrator service
- one Redis instance per service domain

### Run In 2PC Mode

```bash
docker compose down -v
TX_MODE=2PC docker compose up --build
```

### Run In Detached Mode

```bash
docker compose up --build -d
```

### Inspect Logs

```bash
docker compose logs -f orchestrator-service
docker compose logs -f order-service stock-service payment-service
```

## Example Flow

Create a user:

```bash
curl -X POST http://localhost:8000/payment/create_user
```

Add funds:

```bash
curl -X POST http://localhost:8000/payment/add_funds/<USER_ID>/100
```

Create an item:

```bash
curl -X POST http://localhost:8000/stock/item/create/10
```

Add stock:

```bash
curl -X POST http://localhost:8000/stock/add/<ITEM_ID>/5
```

Create an order:

```bash
curl -X POST http://localhost:8000/orders/create/<USER_ID>
```

Add an item to the order:

```bash
curl -X POST http://localhost:8000/orders/addItem/<ORDER_ID>/<ITEM_ID>/2
```

Checkout:

```bash
curl -X POST http://localhost:8000/orders/checkout/<ORDER_ID>
```

At checkout time, `order-service` forwards the request to the orchestrator. The orchestrator then coordinates payment and stock according to the selected protocol.

The interaction can be summarized like this:

1. Client calls `POST /orders/checkout/<order_id>`.
2. `order-service` forwards the request to the orchestrator.
3. The orchestrator loads the order directly from the order Redis database.
4. The orchestrator builds a workflow record and persists it.
5. The orchestrator calls `payment-service` and `stock-service` through their neutral transaction APIs.
6. If all required participant steps succeed, the orchestrator marks the order as paid.
7. If a participant fails, the orchestrator drives rollback and keeps the workflow in durable state until the system converges.

## Summary

The key design points of this project are:

- business services no longer implement distributed transaction protocols
- the orchestrator is the single place that owns `SAGA` and `2PC`
- participants expose a neutral local transaction contract
- participants still manage local transaction state, but not protocol policy
- workflow state is durable
- retries and recovery are centralized
- the queue implementation is also shared infrastructure, not duplicated service logic
