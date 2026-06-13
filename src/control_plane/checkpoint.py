"""Distributed checkpoint manager backed by Redis + msgpack."""

from __future__ import annotations

import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from typing import Any, Dict

import msgpack
import redis
from loguru import logger

from src.control_plane.state import AgentState


class DistributedCheckpointManager:
    """Persist and restore AgentState snapshots via Redis DB 0."""

    _KEY_PREFIX = "checkpoint"

    def __init__(self) -> None:
        self._client = redis.Redis(
            host="127.0.0.1",
            port=6379,
            password="EnterpriseAI2024Module1",
            db=0,
            decode_responses=False,
        )
        logger.info("DistributedCheckpointManager connected to Redis DB 0")

    def save(self, thread_id: str, state: AgentState) -> str:
        timestamp_ms: int = int(time.time() * 1000)
        key: str = f"{self._KEY_PREFIX}:{thread_id}:{timestamp_ms}"
        payload: bytes = msgpack.packb(dict(state))
        self._client.set(key, payload)
        logger.debug(
            "Checkpoint saved | thread_id={} | key={} | size={} bytes",
            thread_id,
            key,
            len(payload),
        )
        return key

    def load_latest(self, thread_id: str) -> AgentState | None:
        pattern: str = f"{self._KEY_PREFIX}:{thread_id}:*"
        keys: list[bytes] = self._client.keys(pattern)

        if not keys:
            logger.warning("No checkpoint found for thread_id={}", thread_id)
            return None

        def _extract_ts(key: bytes) -> int:
            return int(key.decode().rsplit(":", 1)[-1])

        latest_key: bytes = max(keys, key=_extract_ts)

        raw: bytes | None = self._client.get(latest_key)
        if raw is None:
            return None

        data: Dict[str, Any] = msgpack.unpackb(raw, raw=False)
        logger.debug(
            "Checkpoint loaded | thread_id={} | key={} | fields={}",
            thread_id,
            latest_key.decode(),
            list(data.keys()),
        )
        return {
            "messages": data.get("messages", []),
            "current_node": data.get("current_node", ""),
            "code_delta": data.get("code_delta", ""),
            "execution_logs": data.get("execution_logs", []),
            "retry_count": data.get("retry_count", 0),
        }
