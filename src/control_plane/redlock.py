"""Redlock distributed locking primitives backed by Redis.

Provides mutual exclusion for high-concurrency checkpoint writes.
"""

from __future__ import annotations

import time as _time
import uuid

import redis
from loguru import logger


class CheckpointRedlock:
    """Distributed lock via Redis ``SET NX PX`` with exponential backoff.

    Uses a unique UUID token per acquisition attempt.  Release is performed
    atomically via a Lua script so only the current lock holder can release.

    Args:
        redis_client: An active ``redis.Redis`` connection (DB 0, no decode).
    """

    _BACKOFF_MS: tuple[int, ...] = (10, 20, 40, 80, 160)

    def __init__(self, redis_client: redis.Redis) -> None:
        self._client: redis.Redis = redis_client
        logger.debug("CheckpointRedlock initialised")

    def acquire(
        self,
        lock_name: str,
        lock_value: str,
        ttl_ms: int = 5000,
    ) -> bool:
        """Acquire a distributed lock via Redis ``SET NX PX``.

        Uses exponential backoff on contention: 10 → 20 → 40 → 80 → 160 ms
        (capped at 160 ms).  Retries up to 5 times.

        Args:
            lock_name: Redis key for the lock.
            lock_value: Unique token identifying the lock holder (UUID).
            ttl_ms: Lock expiry in milliseconds (px argument).

        Returns:
            ``True`` if the lock was acquired, ``False`` otherwise.
        """
        for attempt in range(len(self._BACKOFF_MS)):
            acquired_raw = self._client.set(
                lock_name, lock_value, px=ttl_ms, nx=True
            )
            acquired: bool = bool(acquired_raw)
            if acquired:
                logger.debug(
                    "Redlock acquired | lock_name={} | attempt={}",
                    lock_name,
                    attempt + 1,
                )
                return True

            backoff_ms: int = self._BACKOFF_MS[attempt]
            logger.debug(
                "Redlock contention | lock_name={} | attempt={} | "
                "backoff_ms={}",
                lock_name,
                attempt + 1,
                backoff_ms,
            )
            _time.sleep(backoff_ms / 1000.0)

        logger.warning(
            "Redlock acquisition failed after {} attempts | lock_name={}",
            len(self._BACKOFF_MS),
            lock_name,
        )
        return False

    def release(self, lock_name: str, lock_value: str) -> bool:
        """Release a distributed lock atomically via Lua script.

        The lock is only deleted if its current value matches *lock_value*,
        preventing accidental release of a lock held by another client.

        Args:
            lock_name: Redis key for the lock.
            lock_value: The unique token that was used to acquire the lock.

        Returns:
            ``True`` if the lock was successfully released, ``False`` if the
            value did not match (lock already expired or owned by someone else).
        """
        lua_script: str = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result: int = self._client.eval(lua_script, 1, lock_name, lock_value)
        released: bool = bool(result)
        if released:
            logger.debug("Redlock released | lock_name={}", lock_name)
        else:
            logger.warning(
                "Redlock release skipped — value mismatch | lock_name={}",
                lock_name,
            )
        return released
