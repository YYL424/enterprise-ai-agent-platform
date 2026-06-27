"""LangGraph 1.x state machine with native checkpointer and interrupt().

Topology (Day 7++)
------------------
::

    START → start_node → intent_parse_node → plan_generate_node → human_interrupt_node
                ↑                                                       │
                │                                         (interrupt if tool, else passthrough)
                │                                                       │
                │                                                 tool_execute_node
                │                                                       │
                │                                                 error_detect_node
                │                                                       │
                │                                          should_continue (conditional)
                │                                            │                │
                └── intent_parse_node ←──────────────────────┘                → end_node → END
                     (retry/repair)                             (answer / question / max retries)

Key changes from Day 7:
- ``RedisCheckpointSaver`` is passed to ``graph.compile(checkpointer=...)``.
  LangGraph automatically persists state after every node — no manual save/load.
- ``human_interrupt_node`` uses LangGraph native ``interrupt()`` to pause.
  ``Command(resume=...)`` resumes execution from the interrupt point.
- All marker-passthrough guards are deleted from nodes — LangGraph resumes
  from the last checkpoint, not from START.
- ``resume_after_human`` is a thin wrapper around ``Command(resume=...)``.
"""

from __future__ import annotations

from typing import Any, cast

from langgraph.graph import StateGraph, START, END
from langgraph.types import Command
from loguru import logger

from src.control_plane.state import AgentState
from src.control_plane.nodes import (
    _empty_state,
    end_node,
    error_detect_node,
    human_interrupt_node,
    intent_parse_node,
    plan_generate_node,
    should_continue,
    start_node,
    tool_execute_node,
)


# ── Graph assembly ──────────────────────────────────────────────────────────────


def build_graph(checkpointer: Any = None) -> Any:
    """Build and compile the state machine.

    Args:
        checkpointer: Optional ``BaseCheckpointSaver`` instance.  When
            provided, LangGraph automatically persists state after every
            node and resumes from the last checkpoint on re-invoke.

    Returns:
        A compiled ``CompiledStateGraph``.
    """
    graph = StateGraph(AgentState)

    # Register nodes
    graph.add_node("start_node", start_node)
    graph.add_node("intent_parse_node", intent_parse_node)
    graph.add_node("plan_generate_node", plan_generate_node)
    graph.add_node("human_interrupt_node", human_interrupt_node)
    graph.add_node("tool_execute_node", tool_execute_node)
    graph.add_node("error_detect_node", error_detect_node)
    graph.add_node("end_node", end_node)

    # Entry
    graph.add_edge(START, "start_node")

    # Linear chain
    graph.add_edge("start_node", "intent_parse_node")
    graph.add_edge("intent_parse_node", "plan_generate_node")
    graph.add_edge("plan_generate_node", "human_interrupt_node")

    # human_interrupt → tool_execute (linear — interrupt() pauses inside the node)
    graph.add_edge("human_interrupt_node", "tool_execute_node")

    # tool_execute → error_detect
    graph.add_edge("tool_execute_node", "error_detect_node")

    # Conditional branch after error_detect:
    #   error + retries_left     → intent_parse_node (repair, then loop)
    #   tool_result (success)    → plan_generate_node (next LLM thought)
    #   answer / question        → end_node (terminal)
    graph.add_conditional_edges(
        "error_detect_node",
        should_continue,
        path_map={
            "intent_parse_node": "intent_parse_node",
            "plan_generate_node": "plan_generate_node",
            "end_node": "end_node",
        },
    )

    # Terminal
    graph.add_edge("end_node", END)

    return graph.compile(checkpointer=checkpointer)


# Module-level compiled graph (lazy-init)
_app: Any | None = None


def get_app() -> Any:
    """Return the module-level compiled graph, building it on first call.

    Creates a ``RedisCheckpointSaver`` and passes it to ``build_graph()``
    so that LangGraph handles state persistence automatically.
    """
    global _app
    if _app is None:
        from src.control_plane.checkpoint import RedisCheckpointSaver

        saver = RedisCheckpointSaver()
        _app = build_graph(checkpointer=saver)
        logger.info("Graph compiled with RedisCheckpointSaver")
    return _app


def _make_config(thread_id: str) -> Any:
    """Build a LangGraph RunnableConfig dict."""
    return {"configurable": {"thread_id": thread_id}}


# ── Checkpoint-aware runner ─────────────────────────────────────────────────────


def run_with_checkpoint(
    thread_id: str,
    initial_state: AgentState,
    manager: Any = None,
) -> AgentState:
    """Run the graph with Redis-backed checkpoint persistence.

    Two checkpoint sources are supported:

    1. **manager (DistributedCheckpointManager)** — when provided, calls
       ``manager.load_latest(thread_id)`` to load a prior state saved
       via ``DistributedCheckpointManager.save()``.  This handles the
       legacy key format (``checkpoint:{thread_id}:{ts}:{seq:06d}``).
    2. **LangGraph native checkpointer** — when *manager* is ``None``,
       queries the compiled graph's ``RedisCheckpointSaver`` via
       ``app.get_state(config)``.

    In both cases, prior state is merged into *initial_state* with
    **new values taking priority** for scalar fields.

    Args:
        thread_id: Unique identifier for the session thread.
        initial_state: The starting state.
        manager: Optional pre-configured :class:`DistributedCheckpointManager`.

    Returns:
        The final ``AgentState`` after graph execution.
    """
    config = _make_config(thread_id)
    app = get_app()
    state: Any = dict(initial_state)

    # ── Load prior state ──────────────────────────────────────────────────
    prior: dict[str, Any] | None = None

    if manager is not None:
        try:
            loaded = manager.load_latest(thread_id)
            if loaded is not None:
                prior = dict(loaded)
                logger.info(
                    "Loaded prior checkpoint via manager | thread_id={}",
                    thread_id,
                )
        except Exception as exc:
            logger.warning(
                "manager.load_latest failed, falling back to native | "
                "thread_id={} | error={}",
                thread_id,
                exc,
            )

    if prior is None:
        # Fall back to LangGraph native checkpointer
        prior_state_snapshot = app.get_state(config)
        if prior_state_snapshot is not None and prior_state_snapshot.values:
            prior = dict(prior_state_snapshot.values)
            logger.info(
                "Checkpoint found via native checkpointer | thread_id={}",
                thread_id,
            )

    # ── Merge prior into state (new values take priority) ─────────────────
    if prior is not None:
        # Scalar fields: restore prior values only when new state is empty
        for key in ("current_node", "code_delta", "retry_count"):
            if state.get(key) in (None, "", 0) and prior.get(key):
                state[key] = prior[key]
        # messages / execution_logs: Annotated[..., operator.add].
        # Let LangGraph's reducer handle append.  Only seed messages
        # from prior when the new state has none at all.
        if not state.get("messages") and prior.get("messages"):
            state["messages"] = list(prior["messages"])
        if not state.get("execution_logs") and prior.get("execution_logs"):
            state["execution_logs"] = list(prior["execution_logs"])
    else:
        logger.info(
            "No checkpoint found — starting fresh | thread_id={}", thread_id,
        )

    result = app.invoke(state, config)

    logger.info(
        "Graph completed | thread_id={} | final_node={}",
        thread_id,
        result.get("current_node") or "?",
    )
    return result


# ── Human-in-the-loop resume ────────────────────────────────────────────────────


def resume_after_human(
    thread_id: str,
    approval: bool,
    manager: Any = None,
) -> AgentState:
    """Resume graph execution after ``interrupt()`` in ``human_interrupt_node``.

    Uses LangGraph native ``Command(resume=...)`` to continue execution
    from the interrupt point.  The *manager* parameter is kept for
    backward compatibility.

    Args:
        thread_id: The session thread that was paused.
        approval: ``True`` to approve the tool, ``False`` to reject.
        manager: Ignored (kept for backward compatibility).

    Returns:
        The final ``AgentState`` after the resumed graph completes.

    Raises:
        ValueError: If no interrupted state exists for *thread_id*.
    """
    config = _make_config(thread_id)
    app = get_app()

    # Verify the graph is paused (next points to pending node)
    state_snapshot = app.get_state(config)
    if state_snapshot is None or not state_snapshot.next:
        raise ValueError(
            f"No pending state found for thread_id={thread_id} — "
            f"cannot resume. Ensure the graph was paused at human_interrupt_node."
        )

    logger.info(
        "Resuming after human interrupt | thread_id={} | approval={}",
        thread_id,
        approval,
    )

    result: AgentState = app.invoke(Command(resume=approval), config)

    logger.info(
        "Resumed graph completed | thread_id={} | final_node={} | code_delta_len={}",
        thread_id,
        result.get("current_node") or "?",
        len(result.get("code_delta") or ""),
    )
    return result


# ── Time-travel resume ──────────────────────────────────────────────────────


def time_travel_resume(
    thread_id: str,
    checkpoint_id: str,
    manager: Any = None,
) -> AgentState:
    """Resume graph execution from a specific historical checkpoint.

    Loads the state snapshot identified by *checkpoint_id*, re-invokes the
    graph from that point, and persists the result as a **new** checkpoint
    (forming a new timeline branch).

    Args:
        thread_id: The session thread whose history contains *checkpoint_id*.
        checkpoint_id: The timestamp id returned by
            :meth:`DistributedCheckpointManager.save`.
        manager: Optional pre-configured :class:`DistributedCheckpointManager`.

    Returns:
        The final ``AgentState`` after the re-executed graph completes.

    Raises:
        ValueError: If *checkpoint_id* does not exist.
    """
    from src.control_plane.checkpoint import DistributedCheckpointManager

    if manager is None:
        manager = DistributedCheckpointManager()

    historical_state: AgentState | None = manager.rollback(thread_id, checkpoint_id)
    if historical_state is None:
        raise ValueError(
            f"Checkpoint {checkpoint_id} not found for "
            f"thread_id={thread_id} — cannot time-travel"
        )

    logger.info(
        "Time-travel resume | thread_id={} | checkpoint_id={} | "
        "historical_node={}",
        thread_id,
        checkpoint_id,
        historical_state.get("current_node") or "?",
    )

    config = _make_config(thread_id)
    result: AgentState = get_app().invoke(historical_state, config)

    logger.info(
        "Time-travel completed | thread_id={} | final_node={} | code_delta={}",
        thread_id,
        result.get("current_node") or "?",
        result.get("code_delta") or "",
    )
    return result


# ── StateMachineEngine — class wrapper ────────────────────────────────────────


class StateMachineEngine:
    """Object-oriented graph engine wrapper for B/C integration.

    When a *checkpoint_manager* is provided, the engine delegates to
    :func:`run_with_checkpoint` for checkpoint-aware execution.
    """

    def __init__(self, checkpoint_manager: Any = None) -> None:
        self._checkpoint_manager: Any = checkpoint_manager
        self.compiled_graph: Any = get_app()
        logger.info("StateMachineEngine compiled successfully")

    def invoke(self, initial_state: AgentState, thread_id: str) -> AgentState:
        """Run the compiled graph.

        When a *checkpoint_manager* was provided at construction time,
        delegates to :func:`run_with_checkpoint`.
        """
        if self._checkpoint_manager is not None:
            logger.info(
                "StateMachineEngine.invoke → run_with_checkpoint | thread_id={}",
                thread_id,
            )
            return run_with_checkpoint(
                thread_id, initial_state, self._checkpoint_manager,
            )

        logger.info(
            "StateMachineEngine.invoke → direct invoke | thread_id={}",
            thread_id,
        )
        return self.compiled_graph.invoke(
            initial_state,
            config={"configurable": {"thread_id": thread_id}},
        )


# ── Direct execution ────────────────────────────────────────────────────────────


if __name__ == "__main__":
    initial_state: AgentState = _empty_state()
    result = get_app().invoke(initial_state)
    logger.info("Final state:")
    for key, value in result.items():
        logger.info("  {}: {}", key, value)
