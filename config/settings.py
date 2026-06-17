"""Platform-wide configuration — loaded from environment variables.

All values are read once at import time via ``os.getenv`` with safe defaults.
No hardcoded credentials or API keys.
"""

from __future__ import annotations

import os

# ── Redis ──────────────────────────────────────────────────────────────────────

REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "EnterpriseAI2024Module1")

# DB partitions (生死线 — do not reassign)
REDIS_DB_CHECKPOINT: int = 0  # A 独占
REDIS_DB_SCHEMA: int = 1       # B 独占
REDIS_DB_SECURITY: int = 2     # C 独占

# ── LLM dual-track gateway ─────────────────────────────────────────────────────

PRIMARY_LLM: str = os.getenv("PRIMARY_LLM", "")
FAST_LLM: str = os.getenv("FAST_LLM", "")
