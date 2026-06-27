"""Distributed checkpoint manager backed by Redis + msgpack.

Provides two layers:

* :class:`DistributedCheckpointManager` — low-level Redis persistence with
  Redlock support.  Kept for backward compatibility.
* :class:`RedisCheckpointSaver` — LangGraph 1.x :class:`BaseCheckpointSaver`
  subclass that plugs into ``graph.compile(checkpointer=...)``.
"""

import time
import uuid

from typing import Any, Dict, Iterator, Sequence

import msgpack
import redis
from loguru import logger
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    RunnableConfig,
)

from config.settings import (
    REDIS_DB_CHECKPOINT,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)
from src.control_plane.state import AgentState
from src.control_plane.redlock import CheckpointRedlock


def _safe_decode(key: bytes | str) -> str:
    """Decode *key* to ``str``, handling ``bytes`` vs ``str`` union."""
    return key.decode() if isinstance(key, bytes) else key


# ═══════════════════════════════════════════════════════════════════════════════
# Low-level persistence (kept for backward compatibility)
# ═══════════════════════════════════════════════════════════════════════════════


class DistributedCheckpointManager:
    """Persist and restore AgentState snapshots via Redis DB 0."""

    _KEY_PREFIX = "legacy"

    def __init__(self) -> None:
        import threading

        if not REDIS_PASSWORD:
            raise ValueError(
                "REDIS_PASSWORD environment variable is not set"
            )

        self._client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB_CHECKPOINT,
            decode_responses=False,
        )
        self._seq_counter: int = 0
        self._seq_lock: threading.Lock = threading.Lock()
        logger.info("DistributedCheckpointManager connected to Redis DB 0")

    # ── Core persistence (lock-free) ─────────────────────────────────────

    def save(self, thread_id: str, state: AgentState) -> str:
        timestamp_ms: int = int(time.time() * 1000)
        with self._seq_lock:
            self._seq_counter += 1
            seq: int = self._seq_counter
        key: str = f"{self._KEY_PREFIX}:{thread_id}:{timestamp_ms}:{seq:06d}"
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
        keys: list[bytes | str] = self._client.keys(pattern)

        if not keys:
            logger.warning("No checkpoint found for thread_id={}", thread_id)
            return None

        # Key format: checkpoint:{thread_id}:{timestamp_ms}:{seq}
        # Lexicographic order = chronological order (fixed-width timestamp + seq)
        latest_key: bytes | str = max(keys)

        raw: bytes | str | None = self._client.get(latest_key)
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode()

        data: Dict[str, Any] = msgpack.unpackb(raw, raw=False)
        logger.debug(
            "Checkpoint loaded | thread_id={} | key={} | fields={}",
            thread_id,
            _safe_decode(latest_key),
            list(data.keys()),
        )
        return {
            "messages": data.get("messages", []),
            "current_node": data.get("current_node", ""),
            "code_delta": data.get("code_delta", ""),
            "execution_logs": data.get("execution_logs", []),
            "retry_count": data.get("retry_count", 0),
        }

    # ── Time-travel rollback ────────────────────────────────────────────

    def rollback(self, thread_id: str, checkpoint_id: str) -> AgentState | None:
        """Load a specific historical checkpoint by its id.

        Scans all checkpoints for *thread_id* and returns the one matching
        *checkpoint_id*.  The *checkpoint_id* is the ``"timestamp_ms:seq"``
        portion of the key returned by :meth:`save`
        (e.g. ``"1718759200123:2"``), or the full key string, or the legacy
        timestamp-only format.

        Args:
            thread_id: Unique session / conversation thread identifier.
            checkpoint_id: The ``"ts:seq"`` id (or full key) of the target
                checkpoint.

        Returns:
            The restored ``AgentState``, or ``None`` if no matching checkpoint
            exists.
        """
        pattern: str = f"{self._KEY_PREFIX}:{thread_id}:*"
        keys: list[bytes | str] = self._client.keys(pattern)

        target_key: str | None = None
        for key in keys:
            key_str: str = _safe_decode(key)
            # Match by exact key, by suffix (ts:seq), or by legacy timestamp
            if (
                key_str == checkpoint_id
                or key_str.endswith(f":{checkpoint_id}")
                or key_str == f"{self._KEY_PREFIX}:{thread_id}:{checkpoint_id}"
            ):
                target_key = key_str
                break

        if target_key is None:
            logger.warning(
                "rollback aborted — checkpoint not found | "
                "thread_id={} | checkpoint_id={}",
                thread_id,
                checkpoint_id,
            )
            return None

        raw: bytes | str | None = self._client.get(target_key)
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode()

        data: Dict[str, Any] = msgpack.unpackb(raw, raw=False)
        logger.info(
            "Checkpoint rolled back | thread_id={} | key={} | fields={}",
            thread_id,
            target_key,
            list(data.keys()),
        )
        return {
            "messages": data.get("messages", []),
            "current_node": data.get("current_node", ""),
            "code_delta": data.get("code_delta", ""),
            "execution_logs": data.get("execution_logs", []),
            "retry_count": data.get("retry_count", 0),
        }

    # ── Redlock distributed locking ───────────────────────────────────────

    _REDLOCK_PREFIX: str = "redlock:checkpoint"

    def save_with_lock(self, thread_id: str, state: AgentState) -> bool:
        """High-concurrency checkpoint save guarded by a Redlock.

        Acquires a distributed lock for *thread_id*, calls
        :meth:`save` under the lock, and releases atomically.

        Args:
            thread_id: Unique session / conversation thread identifier.
            state: The ``AgentState`` to persist.

        Returns:
            ``True`` if the lock was acquired and the state was saved.
            ``False`` if the lock could not be acquired (contention).
        """
        lock_name: str = f"{self._REDLOCK_PREFIX}:{thread_id}"
        lock_value: str = str(uuid.uuid4())
        redlock: CheckpointRedlock = CheckpointRedlock(self._client)

        if not redlock.acquire(lock_name, lock_value, ttl_ms=5000):
            logger.warning(
                "save_with_lock aborted — lock unavailable | "
                "thread_id={}",
                thread_id,
            )
            return False

        try:
            self.save(thread_id, state)
            logger.info(
                "save_with_lock succeeded | thread_id={}", thread_id
            )
            return True
        finally:
            redlock.release(lock_name, lock_value)


# ═══════════════════════════════════════════════════════════════════════════════
# LangGraph 1.x BaseCheckpointSaver — plugs into graph.compile(checkpointer=...)
# ═══════════════════════════════════════════════════════════════════════════════


class RedisCheckpointSaver(BaseCheckpointSaver):
    """LangGraph 1.x checkpointer backed by Redis + msgpack.

    Implements the :class:`BaseCheckpointSaver` interface so the control
    plane graph can be compiled with ``checkpointer=RedisCheckpointSaver()``.
    Internally delegates to the same Redis DB 0 and msgpack serialisation
    used by :class:`DistributedCheckpointManager`.

    Key schema::

        checkpoint:{thread_id}:{checkpoint_ns}:{checkpoint_id}  →  msgpack blob
        checkpoint:{thread_id}:{checkpoint_ns}:latest            →  checkpoint_id

    Args:
        client: Optional pre-configured ``redis.Redis`` connection.  When
            omitted a default connection to DB 0 is created.
    """

    _KEY_PREFIX = "checkpoint"

    def __init__(self, client: redis.Redis | None = None) -> None:
        super().__init__()
        if client is not None:
            self._client = client
        else:
            if not REDIS_PASSWORD:
                raise ValueError(
                    "REDIS_PASSWORD environment variable is not set"
                )
            self._client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                db=REDIS_DB_CHECKPOINT,
                decode_responses=False,
            )
        logger.info("RedisCheckpointSaver initialised (DB 0)")

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _thread_id(config: RunnableConfig) -> str:
        return config["configurable"]["thread_id"]

    @staticmethod
    def _checkpoint_ns(config: RunnableConfig) -> str:
        return config["configurable"].get("checkpoint_ns", "")

    def _make_data_key(
        self, thread_id: str, ns: str, checkpoint_id: str
    ) -> str:
        return f"{self._KEY_PREFIX}:{thread_id}:{ns}:{checkpoint_id}"

    def _make_latest_key(self, thread_id: str, ns: str) -> str:
        return f"{self._KEY_PREFIX}:{thread_id}:{ns}:latest"

    # ── BaseCheckpointSaver interface ─────────────────────────────────────

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Retrieve a checkpoint tuple.

        If *config* includes ``checkpoint_id`` the specific checkpoint is
        fetched; otherwise the latest for the thread is returned.
        """
        thread_id = self._thread_id(config)
        ns = self._checkpoint_ns(config)

        checkpoint_id = config["configurable"].get("checkpoint_id")
        if checkpoint_id is None:
            # Resolve latest pointer
            latest_raw = self._client.get(self._make_latest_key(thread_id, ns))
            if latest_raw is None:
                return None
            checkpoint_id = _safe_decode(latest_raw)

        key = self._make_data_key(thread_id, ns, checkpoint_id)
        raw: bytes | str | None = self._client.get(key)
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode()

        data = msgpack.unpackb(raw, raw=False)
        checkpoint: Checkpoint = data["checkpoint"]
        metadata: CheckpointMetadata = data.get("metadata", {})
        parent_config = data.get("parent_config")
        pending_writes = data.get("pending_writes", [])

        full_config: RunnableConfig = {
            "configurable": {
                **config["configurable"],
                "checkpoint_id": checkpoint_id,
            }
        }
        return CheckpointTuple(
            config=full_config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: dict,
    ) -> RunnableConfig:
        """Persist a checkpoint and update the latest pointer."""
        thread_id = self._thread_id(config)
        ns = self._checkpoint_ns(config)
        checkpoint_id = checkpoint["id"]

        key = self._make_data_key(thread_id, ns, checkpoint_id)
        parent_config = config["configurable"].get("checkpoint_id")

        payload = msgpack.packb({
            "checkpoint": dict(checkpoint),
            "metadata": metadata,
            "parent_config": parent_config,
            "pending_writes": [],
        })
        self._client.set(key, payload)

        # Update latest pointer
        self._client.set(
            self._make_latest_key(thread_id, ns), checkpoint_id
        )

        logger.debug(
            "RedisCheckpointSaver.put | thread_id={} | ns={!r} | "
            "checkpoint_id={}",
            thread_id,
            ns,
            checkpoint_id,
        )
        return {
            "configurable": {
                **config["configurable"],
                "checkpoint_id": checkpoint_id,
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store pending writes (no-op for linear pipeline — writes are
        applied synchronously, not queued)."""
        # The linear pipeline does not produce mid-node pending writes.
        # Implemented for interface compliance; stores nothing.

    def get_next_version(
        self, current: int | str | None, channel: None = None
    ) -> int | str:
        """Generate the next monotonic version number.

        When *current* is ``None`` returns ``1``; otherwise increments.
        """
        if current is None:
            return 1
        if isinstance(current, int):
            return current + 1
        if isinstance(current, str):
            # String versions (e.g. from MemorySaver) — parse and bump
            try:
                return str(int(current) + 1)
            except ValueError:
                return f"{current}.0"
        return 1

    def list(
        self,
        config: RunnableConfig | None = None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints for a thread, optionally filtered."""
        if config is None:
            return
        thread_id = self._thread_id(config)
        ns = self._checkpoint_ns(config)

        pattern = f"{self._KEY_PREFIX}:{thread_id}:{ns}:*"
        keys = self._client.keys(pattern)

        # Filter out "latest" pointer keys
        data_keys = [k for k in keys if not _safe_decode(k).endswith(":latest")]
        # Sort by Redis key (timestamp-based ordering from DCM)
        data_keys.sort(reverse=True)

        count = 0
        for key in data_keys:
            if limit is not None and count >= limit:
                break
            raw: bytes | str | None = self._client.get(key)
            if raw is None:
                continue
            if isinstance(raw, str):
                raw = raw.encode()
            data = msgpack.unpackb(raw, raw=False)
            checkpoint_id = _safe_decode(key).rsplit(":", 1)[-1]
            full_config: RunnableConfig = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": ns,
                    "checkpoint_id": checkpoint_id,
                }
            }
            yield CheckpointTuple(
                config=full_config,
                checkpoint=data["checkpoint"],
                metadata=data.get("metadata", {}),
                parent_config=data.get("parent_config"),
            )
            count += 1

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints for *thread_id*."""
        pattern = f"{self._KEY_PREFIX}:{thread_id}:*"
        keys = self._client.keys(pattern)
        if keys:
            self._client.delete(*keys)
            logger.info(
                "Deleted {} checkpoint keys for thread_id={}",
                len(keys),
                thread_id,
            )
