from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

import redis


QUEUE_RESULT_TIMEOUT_SECONDS = int(os.getenv("REDIS_QUEUE_RESULT_TIMEOUT_SECONDS", "120"))
QUEUE_RESULT_TTL_SECONDS = int(os.getenv("REDIS_QUEUE_RESULT_TTL_SECONDS", "120"))
QUEUE_BLOCK_TIMEOUT_SECONDS = int(os.getenv("REDIS_QUEUE_BLOCK_TIMEOUT_SECONDS", "1"))
QUEUE_WORKERS = int(os.getenv("REDIS_QUEUE_WORKERS", "4"))


def queued_json(body: Any, status: int = 200) -> dict[str, Any]:
    return {"kind": "json", "status": status, "body": body}


def queued_text(body: str, status: int = 200) -> dict[str, Any]:
    return {"kind": "text", "status": status, "body": body}


class RedisCommandQueue:
    def __init__(
        self,
        *,
        name: str,
        redis_factory: Callable[[], redis.Redis],
        handlers: dict[str, Callable[..., dict[str, Any]]],
        logger: logging.Logger,
    ):
        self.name = name
        self.redis_factory = redis_factory
        self.handlers = handlers
        self.logger = logger
        self.queue_key = f"queue:{name}:commands"
        self.result_prefix = f"queue:{name}:result:"
        self.submit_client = redis_factory()
        self.worker_clients: list[redis.Redis] = []
        self.worker_threads: list[threading.Thread] = []
        for index in range(max(1, QUEUE_WORKERS)):
            worker_client = redis_factory()
            thread = threading.Thread(
                target=self._worker_loop,
                args=(worker_client,),
                name=f"{name}-queue-{index}",
                daemon=True,
            )
            thread.start()
            self.worker_clients.append(worker_client)
            self.worker_threads.append(thread)
        atexit.register(self.close)

    def close(self) -> None:
        self.submit_client.close()
        for worker_client in self.worker_clients:
            worker_client.close()

    def execute(self, operation: str, **payload: Any) -> dict[str, Any]:
        result_key = f"{self.result_prefix}{uuid.uuid4().hex}"
        command = {
            "operation": operation,
            "payload": payload,
            "result_key": result_key,
        }
        try:
            self.submit_client.rpush(self.queue_key, json.dumps(command))
            result = self.submit_client.blpop(result_key, timeout=QUEUE_RESULT_TIMEOUT_SECONDS)
        except redis.exceptions.RedisError as exc:
            raise RuntimeError("Redis queue unavailable") from exc
        if result is None:
            raise TimeoutError(f"Timed out waiting for queued operation {operation}")
        _, raw_result = result
        return json.loads(raw_result)

    def _worker_loop(self, worker_client: redis.Redis) -> None:
        while True:
            try:
                queued_entry = worker_client.blpop(self.queue_key, timeout=QUEUE_BLOCK_TIMEOUT_SECONDS)
            except redis.exceptions.RedisError:
                time.sleep(0.1)
                continue
            if queued_entry is None:
                continue
            _, raw_command = queued_entry
            command = json.loads(raw_command)
            response = self._dispatch(command["operation"], command["payload"])
            try:
                worker_client.rpush(command["result_key"], json.dumps(response))
                worker_client.expire(command["result_key"], QUEUE_RESULT_TTL_SECONDS)
            except redis.exceptions.RedisError:
                self.logger.exception("Failed to publish queued response for %s", command["operation"])

    def _dispatch(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        handler = self.handlers.get(operation)
        if handler is None:
            return {"ok": False, "status": 500, "body": f"Unknown queued operation: {operation}"}
        try:
            response = handler(**payload)
            return {"ok": True, "response": response}
        except Exception as exc:
            code = getattr(exc, "code", None)
            description = getattr(exc, "description", None)
            if isinstance(code, int) and isinstance(description, str):
                return {"ok": False, "status": code, "body": description}
            self.logger.exception("Unhandled error in queued operation %s", operation)
            return {"ok": False, "status": 500, "body": "Internal server error"}
