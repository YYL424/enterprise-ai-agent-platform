"""Node functions for the LangGraph control plane state machine.

Contains every graph node, the conditional router, and the
``_empty_state`` factory — extracted from ``graph.py`` so the
topology (``build_graph`` / ``StateMachineEngine``) and the
node implementations can evolve independently.
"""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from loguru import logger

from src.control_plane.state import AgentState


# ── Helpers ────────────────────────────────────────────────────────────────────


def _empty_state() -> AgentState:
    """Return a fresh AgentState with all fields initialised to their defaults."""
    return {
        "messages": [],
        "current_node": "",
        "code_delta": "",
        "execution_logs": [],
        "retry_count": 0,
    }


# ── Node functions ─────────────────────────────────────────────────────────────


def start_node(state: AgentState) -> dict:
    """First node — initialises the pipeline while preserving caller-set fields.

    ``code_delta`` and ``retry_count`` are carried forward from the incoming
    state so that tests (and external callers) can seed error scenarios.

    When ``current_node == "human_interrupt_resumed"`` the graph is replaying
    after a human approval/rejection — pass through without resetting any field.
    """
    if state.get("current_node", "") == "human_interrupt_resumed":
        return {"current_node": "human_interrupt_resumed"}

    return {
        "messages": ["[start_node] pipeline initialised"],
        "current_node": "start_node",
        "code_delta": state.get("code_delta", ""),
        "execution_logs": ["start_node completed"],
        "retry_count": state.get("retry_count", 0),
    }


def intent_parse_node(state: AgentState) -> dict:
    """Parse user intent, with retry-aware error handling.

    *Replay-safe behaviour* — when the graph loops back via the retry edge
    (``error_detect_node`` → conditional → ``intent_parse_node``):

    - ``code_delta == "error_v1"`` **and** ``retry_count > 0``: the node
      treats this as a one-shot recoverable error and repairs it by emitting
      the normal ``"intent_parsed_v1"``.
    - Any *other* ``code_delta`` containing the substring ``"error"`` is
      considered persistent and is preserved so that ``error_detect_node``
      can re-trigger — the max-retry guard in ``should_retry`` is the only
      escape hatch.
    - In all other cases the node emits ``"intent_parsed_v1"`` (standard path).

    When ``current_node == "human_interrupt_resumed"`` the graph is replaying
    after human approval — pass through without overwriting ``code_delta``.
    """
    if state.get("current_node", "") == "human_interrupt_resumed":
        return {"current_node": "human_interrupt_resumed"}

    parsed: str = f"[intent] parsed from {len(state['messages'])} message(s)"
    current_cd: str = state.get("code_delta", "")
    retry_count: int = state.get("retry_count", 0)

    if retry_count > 0 and "error" in current_cd:
        if current_cd == "error_v1":
            # Recoverable error — one retry clears it
            code_delta: str = "intent_parsed_v1"
            logger.info(
                "Recoverable error fixed on retry | code_delta={} → {}",
                current_cd,
                code_delta,
            )
        else:
            # Persistent error — keep alive for next error_detect pass
            code_delta = current_cd
            logger.warning(
                "Persistent error preserved on retry | code_delta={} | retry_count={}",
                current_cd,
                retry_count,
            )
    elif "error" not in current_cd:
        code_delta = "intent_parsed_v1"
    else:
        # First pass — carry the error forward so error_detect can see it
        code_delta = current_cd

    return {
        "messages": ["[intent_parse_node] " + parsed],
        "current_node": "intent_parse",
        "code_delta": code_delta,
        "execution_logs": ["intent_parse_node completed"],
        "retry_count": retry_count,
    }


def plan_generate_node(state: AgentState) -> dict:
    """Generate an execution plan based on parsed intent.

    When ``current_node == "human_interrupt_resumed"`` the graph is replaying
    after human approval — pass through to preserve the marker so downstream
    ``human_interrupt_node`` does not trigger a second interrupt.
    """
    if state.get("current_node", "") == "human_interrupt_resumed":
        return {"current_node": "human_interrupt_resumed"}

    plan: str = (
        f"[plan] generated plan for intent: {state.get('code_delta', 'unknown')}"
    )
    return {
        "messages": ["[plan_generate_node] " + plan],
        "current_node": "plan_generate",
        "code_delta": state.get("code_delta", ""),
        "execution_logs": ["plan_generate_node completed"],
        "retry_count": state.get("retry_count", 0),
    }


def human_interrupt_node(state: AgentState) -> dict:
    """Human-in-the-loop interrupt marker node.

    - If ``current_node == "human_interrupt_resumed"``: the graph is replaying
      after human approval — pass through silently to avoid a second interrupt.
    - If ``"error"`` is present in ``code_delta``: a retry / error-processing
      loop is in progress — skip the interrupt so ``error_detect_node`` and
      ``should_retry`` can handle the error normally.
    - Otherwise: set the ``"human_interrupt"`` marker so ``error_detect_node``
      and ``should_retry`` can pause execution, waiting for external human action
      via ``resume_after_human``.
    """
    if state.get("current_node", "") == "human_interrupt_resumed":
        logger.info(
            "human_interrupt_node — resumed marker detected, passing through"
        )
        return {"current_node": "human_interrupt_resumed"}

    # Error in flight — let error_detect / retry loop process it first
    if "error" in state.get("code_delta", ""):
        logger.info(
            "human_interrupt_node — error in code_delta, skipping interrupt | "
            "code_delta={}", state.get("code_delta", "")
        )
        return {"current_node": state.get("current_node", "")}

    logger.info("human_interrupt_node — setting interrupt marker")
    return {
        "current_node": "human_interrupt",
        "execution_logs": ["human_interrupt: waiting"],
        "messages": ["[human_interrupt] pending approval"],
    }


def error_detect_node(state: AgentState) -> dict:
    """Inspect ``code_delta`` for error markers and increment retry count.

    When an error is found the incoming ``code_delta`` value is **preserved**
    so that ``should_retry`` — which runs *after* the node return has been
    applied — still sees the error signal.  Without this preservation the
    ``add_conditional_edges`` router would never trigger the retry branch.

    When ``current_node == "human_interrupt"`` the graph is paused waiting
    for human approval — treat as no error and preserve the marker so
    ``should_retry`` can route to ``end_node``.
    """
    # ── Human interrupt passthrough ─────────────────────────────────────
    if state.get("current_node", "") == "human_interrupt":
        logger.info("Human interrupt pending — skipping error detection")
        return {
            "current_node": "human_interrupt",  # preserve for should_retry
            "execution_logs": ["[error_detect] human interrupt pending, skip"],
            "messages": ["[error_detect] pending"],
        }

    # ── Normal error detection ──────────────────────────────────────────
    current_cd: str = state.get("code_delta", "")

    if "error" in current_cd:
        new_retry: int = state.get("retry_count", 0) + 1
        logger.warning(
            "Error detected | code_delta={} | retry_count {}→{}",
            current_cd,
            state.get("retry_count", 0),
            new_retry,
        )
        return {
            "retry_count": new_retry,
            "current_node": "error_detect",
            "code_delta": current_cd,  # preserve — should_retry needs it
            "execution_logs": [
                "[error_detect] error found, retry_count incremented"
            ],
            "messages": ["[error_detect] error detected"],
        }

    logger.info("No error detected | code_delta={}", current_cd)
    return {
        "current_node": "error_detect",
        "execution_logs": ["[error_detect] no error"],
        "messages": ["[error_detect] clean"],
    }


def end_node(state: AgentState) -> dict:
    """Final node — marks the pipeline as finished."""
    return {
        "messages": ["[end_node] pipeline finished"],
        "current_node": "end",
        "code_delta": state.get("code_delta", ""),
        "execution_logs": ["end_node completed"],
        "retry_count": state.get("retry_count", 0),
    }


# ── Router ──────────────────────────────────────────────────────────────────────

_MAX_RETRIES: int = 3


def should_retry(state: AgentState) -> str:
    """Conditional router for ``error_detect_node``.

    Returns the *name* of the next node to execute (string form — LangGraph
    0.0.60 resolves it directly)::

        current_node == "human_interrupt"                →  "end_node"
        "error" in code_delta  AND  retry_count < 3       →  "intent_parse_node"
        otherwise                                         →  "end_node"
    """
    # ── Human interrupt — pause execution for manual approval ────────────
    if state.get("current_node", "") == "human_interrupt":
        logger.info("should_retry → end_node (human interrupt pending)")
        return "end_node"

    # ── Normal error / retry logic ──────────────────────────────────────
    has_error: bool = "error" in state.get("code_delta", "")
    under_limit: bool = state.get("retry_count", 0) < _MAX_RETRIES

    if has_error and under_limit:
        logger.info(
            "should_retry → intent_parse_node | retry_count={} | code_delta={}",
            state.get("retry_count", 0),
            state.get("code_delta", ""),
        )
        return "intent_parse_node"

    logger.info(
        "should_retry → end_node | has_error={} | retry_count={}",
        has_error,
        state.get("retry_count", 0),
    )
    return "end_node"
