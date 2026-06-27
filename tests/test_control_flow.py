"""Pytest tests for the LangGraph 1.x control plane state machine.

Day 7++ — Native Checkpointer + interrupt()
Tests adapted for:
- No marker-passthrough guards (nodes no longer check
  ``current_node == "human_interrupt_resumed"``)
- ``should_retry`` removed — only ``should_continue`` router
- ``human_interrupt_node`` uses native ``interrupt()``
- ``graph.invoke()`` pauses at interrupt and returns interrupted state
"""

import pytest

from src.control_plane.graph import (
    get_app,
    build_graph,
    resume_after_human,
    run_with_checkpoint,
    StateMachineEngine,
)
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
from src.control_plane.state import AgentState


# ── Helpers ────────────────────────────────────────────────────────────────────


def _invoke_full_graph(initial_state: AgentState | None = None) -> AgentState:
    """Invoke the full graph, auto-resuming any HITL interrupts.

    With native ``interrupt()``, the graph pauses at ``human_interrupt_node``
    when a tool call is detected.  This helper detects the interruption and
    auto-resumes with approval=True.
    """
    from langgraph.types import Command

    if initial_state is None:
        initial_state = _empty_state()

    graph = get_app()
    config = {"configurable": {"thread_id": "test-full-graph-helper"}}
    result = graph.invoke(initial_state, config)

    # Auto-resume pauses (LangGraph: interrupt() sets snapshot.next, not .interrupted)
    while True:
        snapshot = graph.get_state(config)
        if not snapshot or not snapshot.next:
            break
        result = graph.invoke(Command(resume=True), config)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Individual node unit tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestStartNode:
    def test_returns_expected_keys(self) -> None:
        delta = start_node(_empty_state())
        assert "messages" in delta
        assert "current_node" in delta

    def test_current_node_is_start(self) -> None:
        delta = start_node(_empty_state())
        assert delta["current_node"] == "start_node"


class TestIntentParseNode:
    def test_current_node_is_intent_parse(self) -> None:
        pre_state = _empty_state()
        pre_state["messages"] = ["user: train a BERT model"]
        delta = intent_parse_node(pre_state)
        assert delta["current_node"] == "intent_parse"

    def test_code_delta_is_set(self) -> None:
        delta = intent_parse_node(_empty_state())
        assert delta["code_delta"] == "intent_parsed_v1"


class TestPlanGenerateNode:
    def test_current_node_is_plan_generate(self) -> None:
        pre_state = _empty_state()
        pre_state["code_delta"] = "intent_parsed_v1"
        delta = plan_generate_node(pre_state)
        assert delta["current_node"] == "plan_generate"

    def test_messages_increase(self) -> None:
        pre_state = _empty_state()
        pre_state["messages"] = ["msg1"]
        pre_state["code_delta"] = "intent_parsed_v1"
        delta = plan_generate_node(pre_state)
        merged_msgs = pre_state["messages"] + delta.get("messages", [])
        assert len(merged_msgs) > len(pre_state["messages"])


class TestEndNode:
    def test_current_node_is_end(self) -> None:
        delta = end_node(_empty_state())
        assert delta["current_node"] == "end"


# ═══════════════════════════════════════════════════════════════════════════════
# Full graph invocation
# ═══════════════════════════════════════════════════════════════════════════════


class TestFullGraphInvocation:
    def test_all_fields_present(self) -> None:
        result = _invoke_full_graph()
        expected = {"messages", "current_node", "code_delta",
                     "execution_logs", "retry_count"}
        assert set(result.keys()) == expected

    def test_final_current_node_is_end(self) -> None:
        result = _invoke_full_graph()
        assert result["current_node"] == "end"

    def test_messages_grew(self) -> None:
        result = _invoke_full_graph()
        assert len(result["messages"]) >= 5

    def test_execution_logs_populated(self) -> None:
        result = _invoke_full_graph()
        assert len(result["execution_logs"]) >= 4

    def test_retry_count_zero(self) -> None:
        result = _invoke_full_graph()
        assert result["retry_count"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Checkpoint integration — DistributedCheckpointManager save/load round-trip
# ═══════════════════════════════════════════════════════════════════════════════


class TestCheckpointIntegration:
    """Integration tests for checkpoint save/load round-trip and graph resume."""

    def test_checkpoint_save_and_load(self) -> None:
        """Verify save() → load_latest() restores all five AgentState fields."""
        from src.control_plane.checkpoint import DistributedCheckpointManager

        manager: DistributedCheckpointManager = DistributedCheckpointManager()
        thread_id: str = "test-save-load-001"
        state: AgentState = {
            "messages": ["msg-a", "msg-b"],
            "current_node": "plan_generate",
            "code_delta": "test_delta_value",
            "execution_logs": ["log-entry-1"],
            "retry_count": 3,
        }

        manager.save(thread_id, state)
        loaded: AgentState | None = manager.load_latest(thread_id)

        assert loaded is not None, "load_latest must return a saved state"
        assert loaded["messages"] == ["msg-a", "msg-b"]
        assert loaded["current_node"] == "plan_generate"
        assert loaded["code_delta"] == "test_delta_value"
        assert loaded["execution_logs"] == ["log-entry-1"]
        assert loaded["retry_count"] == 3

    def test_run_with_checkpoint_continues(self) -> None:
        """Graph picks up a warm checkpoint and appends new data via operator.add."""
        from src.control_plane.checkpoint import DistributedCheckpointManager

        thread_id: str = "test-continue-002"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()

        mid_state: AgentState = {
            "messages": ["[checkpoint] prior-partial-run"],
            "current_node": "intent_parse",
            "code_delta": "error_v1",
            "execution_logs": ["[checkpoint] prior-log-entry"],
            "retry_count": 1,
        }
        manager.save(thread_id, mid_state)

        initial: AgentState = {
            "messages": [],
            "current_node": "",
            "code_delta": "",
            "execution_logs": [],
            "retry_count": 0,
        }
        result: AgentState = run_with_checkpoint(thread_id, initial)

        assert "[checkpoint] prior-partial-run" in result["messages"], (
            "Checkpoint messages must be preserved in result"
        )
        assert result["current_node"] in ("end", "human_interrupt"), (
            f"Expected end or human_interrupt, got {result['current_node']}"
        )
        assert "[checkpoint] prior-log-entry" in result["execution_logs"]

        reloaded: AgentState | None = manager.load_latest(thread_id)
        assert reloaded is not None, "Result must be saved as new checkpoint"


# ═══════════════════════════════════════════════════════════════════════════════
# Retry loop — error-detect → should_continue → retry path
# ═══════════════════════════════════════════════════════════════════════════════


class TestRetryLoop:
    """Tests for the error-detect → should_continue retry loop.

    With native interrupt(), ``human_interrupt_node`` pauses at tool calls.
    Error paths skip the interrupt entirely.
    """

    @staticmethod
    def _count_node_runs(state: AgentState, node_label: str) -> int:
        """Count how many times *node_label* appears in messages."""
        return sum(1 for m in state["messages"] if node_label in m)

    # ── Recoverable error ────────────────────────────────────────────

    def test_retry_once(self) -> None:
        """``code_delta='error_v1'`` → detected, retried once via intent_parse, fixed."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["code_delta"] = "error_v1"
        graph = get_app()
        config = {"configurable": {"thread_id": "test-retry-once"}}
        result = graph.invoke(initial, config)

        # Auto-resume any pauses
        while True:
            snapshot = graph.get_state(config)
            if not snapshot or not snapshot.next:
                break
            result = graph.invoke(Command(resume=True), config)

        assert result["retry_count"] == 1, (
            f"Expected 1 retry, got {result['retry_count']}"
        )
        assert result["current_node"] == "end", (
            f"Expected end after error repaired, got {result['current_node']}"
        )
        assert self._count_node_runs(result, "[intent_parse_node]") >= 2, (
            "Expected >=2 intent_parse runs (1 initial + 1 retry)"
        )
        assert "error" not in result["code_delta"], (
            f"Error must be cleared after repair, got {result['code_delta']}"
        )

    # ── Persistent error — max-retry guard ───────────────────────────

    def test_retry_three_times(self) -> None:
        """Persistent error exhausts all three retries, then terminates."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["code_delta"] = "error_persistent"
        graph = get_app()
        config = {"configurable": {"thread_id": "test-retry-three"}}
        result = graph.invoke(initial, config)

        while True:
            snapshot = graph.get_state(config)
            if not snapshot or not (snapshot.next == ('human_interrupt_node',)):
                break
            result = graph.invoke(Command(resume=True), config)

        assert result["retry_count"] == 3, (
            f"Expected 3 retries (max), got {result['retry_count']}"
        )
        assert result["current_node"] == "end"
        assert self._count_node_runs(result, "[intent_parse_node]") >= 3, (
            "Expected >=3 intent_parse runs (initial + retries)"
        )

    def test_max_retry_exceeded(self) -> None:
        """``retry_count`` already at limit → no retry loop triggered."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["code_delta"] = "error_v1"
        initial["retry_count"] = 3
        graph = get_app()
        config = {"configurable": {"thread_id": "test-max-retry"}}
        result = graph.invoke(initial, config)

        while True:
            snapshot = graph.get_state(config)
            if not snapshot or not (snapshot.next == ('human_interrupt_node',)):
                break
            result = graph.invoke(Command(resume=True), config)

        assert result["retry_count"] == 3, (
            f"retry_count should stay at 3, got {result['retry_count']}"
        )
        assert result["current_node"] in ("end", "human_interrupt", "error_detect"), (
            f"Expected end/human_interrupt/error_detect, got {result['current_node']}"
        )

    def test_no_retry_when_clean(self) -> None:
        """Clean JSON tool → interrupt triggered → resume → completes."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        initial["current_node"] = "plan_generate"
        graph = get_app()
        config = {"configurable": {"thread_id": "test-no-retry-clean"}}

        # First invoke — pauses at interrupt
        result = graph.invoke(initial, config)
        snapshot = graph.get_state(config)
        assert (snapshot.next == ('human_interrupt_node',)), (
            "Graph must be paused at human_interrupt_node for JSON tool"
        )

        # Resume with approval
        result = graph.invoke(Command(resume=True), config)
        assert result["current_node"] in ("end", "tool_execute", "human_interrupt"), (
            f"Expected end/tool_execute/human_interrupt, got {result['current_node']}"
        )

    # ── should_continue router unit checks ───────────────────────────

    def test_should_continue_error_detect_route(self) -> None:
        """should_continue returns 'intent_parse_node' when error + retries left."""
        state: AgentState = _empty_state()
        state["code_delta"] = "error_v1"
        state["retry_count"] = 1
        assert should_continue(state) == "intent_parse_node"

    def test_should_continue_end_on_max(self) -> None:
        """should_continue returns 'end_node' when error + retries exhausted."""
        state: AgentState = _empty_state()
        state["code_delta"] = "error_persistent"
        state["retry_count"] = 3
        assert should_continue(state) == "end_node"

    def test_should_continue_end_when_clean(self) -> None:
        """should_continue returns 'end_node' for non-JSON, non-error code_delta."""
        state: AgentState = _empty_state()
        state["code_delta"] = "intent_parsed_v1"
        state["retry_count"] = 0
        assert should_continue(state) == "end_node"

    # ── error_detect_node unit checks ────────────────────────────────

    def test_error_detect_increments_retry(self) -> None:
        """error_detect_node bumps retry_count when error found."""
        state: AgentState = _empty_state()
        state["code_delta"] = "error_v1"
        state["retry_count"] = 1
        delta: dict = error_detect_node(state)
        assert delta["retry_count"] == 2
        assert delta["current_node"] == "error_detect"

    def test_error_detect_clean_when_no_error(self) -> None:
        """error_detect_node does not touch retry_count on clean code_delta."""
        state: AgentState = _empty_state()
        state["code_delta"] = "intent_parsed_v1"
        state["retry_count"] = 0
        delta: dict = error_detect_node(state)
        assert "retry_count" not in delta
        assert delta["current_node"] == "error_detect"


# ═══════════════════════════════════════════════════════════════════════════════
# Human-in-the-loop — native interrupt() scheme
# ═══════════════════════════════════════════════════════════════════════════════


class TestHumanInterrupt:
    """Tests for HITL via native LangGraph ``interrupt()``."""

    def test_interrupt_pauses_execution(self) -> None:
        """JSON tool code_delta → graph is interrupted."""
        initial = _empty_state()
        initial["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        initial["current_node"] = "plan_generate"

        graph = get_app()
        config = {"configurable": {"thread_id": "test-interrupt-pause"}}
        result = graph.invoke(initial, config)

        snapshot = graph.get_state(config)
        assert (snapshot.next == ('human_interrupt_node',)), (
            "Graph must be paused at human_interrupt_node for JSON tool"
        )

    def test_resume_approve_completes(self) -> None:
        """Interrupted graph resumes with approval → completes."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        initial["current_node"] = "plan_generate"

        graph = get_app()
        config = {"configurable": {"thread_id": "test-resume-approve"}}
        result = graph.invoke(initial, config)

        snapshot = graph.get_state(config)
        assert (snapshot.next == ('human_interrupt_node',))

        result = graph.invoke(Command(resume=True), config)
        assert result["current_node"] in ("end", "tool_execute", "human_interrupt"), (
            f"Expected end/tool_execute/human_interrupt, got {result['current_node']}"
        )

    def test_resume_reject_triggers_retry(self) -> None:
        """Interrupted graph resumed with rejection → error triggers retry."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        initial["current_node"] = "plan_generate"

        graph = get_app()
        config = {"configurable": {"thread_id": "test-resume-reject"}}
        result = graph.invoke(initial, config)

        snapshot = graph.get_state(config)
        assert (snapshot.next == ('human_interrupt_node',))

        result = graph.invoke(Command(resume=False), config)
        # Auto-resume any further interrupts
        while True:
            snapshot = graph.get_state(config)
            if not snapshot or not (snapshot.next == ('human_interrupt_node',)):
                break
            result = graph.invoke(Command(resume=True), config)

        assert result["retry_count"] >= 1, (
            f"Expected retry_count >= 1, got {result['retry_count']}"
        )
        assert result["current_node"] == "end", (
            f"Expected end after retry loop exhausted, got {result['current_node']}"
        )

    def test_error_in_flight_skips_interrupt(self) -> None:
        """code_delta contains 'error' → HITL node skips interrupt."""
        initial = _empty_state()
        initial["code_delta"] = "error: schema_invalid"

        graph = get_app()
        config = {"configurable": {"thread_id": "test-error-skip"}}
        result = graph.invoke(initial, config)

        snapshot = graph.get_state(config)
        assert not (snapshot.next == ('human_interrupt_node',)), (
            "Graph must NOT pause at human_interrupt_node when error is in flight"
        )
        assert "error" in result.get("code_delta", ""), (
            "Error code_delta must persist through the graph"
        )

    def test_multiple_interrupt_resume_cycles(self) -> None:
        """Two complete interrupt→resume cycles work correctly."""
        from langgraph.types import Command

        seed = _empty_state()
        seed["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        seed["current_node"] = "plan_generate"

        graph = get_app()

        # Cycle 1 — approve
        config1 = {"configurable": {"thread_id": "test-cycle-1"}}
        r1 = graph.invoke(seed.copy(), config1)
        assert graph.get_state(config1).next == ("human_interrupt_node",)
        r2 = graph.invoke(Command(resume=True), config1)
        assert r2["current_node"] in ("end", "tool_execute", "human_interrupt")

        # Cycle 2 — reject
        config2 = {"configurable": {"thread_id": "test-cycle-2"}}
        r3 = graph.invoke(seed.copy(), config2)
        assert graph.get_state(config2).next == ("human_interrupt_node",)
        r4 = graph.invoke(Command(resume=False), config2)
        # Auto-resume further interrupts
        while True:
            snapshot = graph.get_state(config2)
            if not snapshot or not (snapshot.next == ('human_interrupt_node',)):
                break
            r4 = graph.invoke(Command(resume=True), config2)
        assert r4["current_node"] == "end"
        assert r4["retry_count"] >= 1

    def test_resume_preserves_state(self) -> None:
        """State fields from pre-interrupt nodes survive resume."""
        from langgraph.types import Command

        initial = _empty_state()
        initial["messages"] = ["[custom] pre-seeded message"]
        initial["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        initial["current_node"] = "plan_generate"

        graph = get_app()
        config = {"configurable": {"thread_id": "test-resume-preserve"}}
        result = graph.invoke(initial, config)

        assert graph.get_state(config).next == ("human_interrupt_node",)
        result = graph.invoke(Command(resume=True), config)

        assert "[custom] pre-seeded message" in result["messages"], (
            "Pre-seeded messages must survive interrupt/resume cycle"
        )

    def test_human_interrupt_node_sets_interrupt(self) -> None:
        """human_interrupt_node calls interrupt() for tool decision."""
        state = _empty_state()
        state["current_node"] = "plan_generate"
        state["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'

        # interrupt() raises GraphInterrupt — test that the function calls it
        with pytest.raises(Exception):
            human_interrupt_node(state)
        # The GraphInterrupt exception is expected — it means interrupt() was called

    def test_human_interrupt_node_skips_on_error(self) -> None:
        """human_interrupt_node passes through when code_delta has error."""
        state = _empty_state()
        state["current_node"] = "plan_generate"
        state["code_delta"] = "error: loop_detected"

        delta = human_interrupt_node(state)
        assert delta["current_node"] == "plan_generate"
        assert "human_interrupt" not in str(delta)


# ═══════════════════════════════════════════════════════════════════════════════
# Time-travel rollback — historical checkpoint recovery
# ═══════════════════════════════════════════════════════════════════════════════


class TestTimeTravel:
    """Tests for rollback and time_travel_resume checkpoint recovery."""

    def test_rollback_success(self) -> None:
        """Save 3 checkpoints, rollback to the 2nd, verify exact match."""
        from src.control_plane.checkpoint import DistributedCheckpointManager

        thread_id: str = "test-timetravel-001"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()

        for i, node in enumerate(["intent_parse", "plan_generate", "error_detect"]):
            state: AgentState = {
                "messages": [f"cp-{i+1}"],
                "current_node": node,
                "code_delta": f"delta_{i+1}",
                "execution_logs": [f"log-{i+1}"],
                "retry_count": i,
            }
            manager.save(thread_id, state)

        all_keys = manager._client.keys(f"legacy:{thread_id}:*")
        sorted_keys = sorted(all_keys)
        target_key: str = (
            sorted_keys[1].decode()
            if isinstance(sorted_keys[1], bytes)
            else sorted_keys[1]
        )
        _prefix = f"legacy:{thread_id}:"
        checkpoint_id: str = target_key[len(_prefix):]
        rolled_back: AgentState | None = manager.rollback(thread_id, checkpoint_id)

        assert rolled_back is not None, "rollback must return a saved state"
        assert rolled_back["current_node"] == "plan_generate"

    def test_rollback_nonexistent_returns_none(self) -> None:
        """rollback with a non-existent checkpoint_id returns None."""
        from src.control_plane.checkpoint import DistributedCheckpointManager

        thread_id: str = "test-timetravel-002"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()

        manager.save(thread_id, {
            "messages": ["only-one"],
            "current_node": "start_node",
            "code_delta": "",
            "execution_logs": [],
            "retry_count": 0,
        })

        result: AgentState | None = manager.rollback(thread_id, "9999999999999")
        assert result is None, (
            "rollback must return None for non-existent checkpoint_id"
        )

    def test_time_travel_resume_creates_new_timeline(self) -> None:
        """Rollback + re-invoke creates a new checkpoint (new timeline branch)."""
        from src.control_plane.checkpoint import DistributedCheckpointManager
        from src.control_plane.graph import time_travel_resume

        thread_id: str = "test-timetravel-003"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()

        mid_state: AgentState = {
            "messages": ["[historical] mid-task"],
            "current_node": "intent_parse",
            "code_delta": "error_v1",
            "execution_logs": ["[historical] prior-log"],
            "retry_count": 1,
        }
        key: str = manager.save(thread_id, mid_state)
        _prefix = f"legacy:{thread_id}:"
        checkpoint_id: str = key[len(_prefix):]

        result: AgentState = time_travel_resume(thread_id, checkpoint_id)

        assert result["current_node"] in ("end", "human_interrupt"), (
            f"Expected end or human_interrupt, got {result['current_node']}"
        )
        assert "[historical] mid-task" in result["messages"], (
            "Historical messages must survive time-travel resume"
        )
        assert "[historical] prior-log" in result["execution_logs"]

        latest: AgentState | None = manager.load_latest(thread_id)
        assert latest is not None, "time_travel_resume must write a new checkpoint"


# ── Helper for checkpoint-based tests ─────────────────────────────────────────


def _sample_state() -> AgentState:
    """Return a representative AgentState for checkpoint test seeding."""
    return {
        "messages": ["sample-msg-a", "sample-msg-b"],
        "current_node": "plan_generate",
        "code_delta": "intent_parsed_v1",
        "execution_logs": ["sample-log-1"],
        "retry_count": 2,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Checkpoint Redlock — distributed locking tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestCheckpointRedlock:
    """Tests for Redlock-based distributed locking in checkpoint manager."""

    def test_save_with_lock_acquires_and_releases(self) -> None:
        """``save_with_lock`` acquires lock, saves state, releases lock."""
        from src.control_plane.checkpoint import DistributedCheckpointManager

        thread_id: str = "thread-redlock-001"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()
        state: AgentState = _sample_state()

        acquired: bool = manager.save_with_lock(thread_id, state)

        assert acquired is True, "save_with_lock must succeed under no contention"

        loaded: AgentState | None = manager.load_latest(thread_id)
        assert loaded is not None, "State must be readable after lock release"
        assert loaded["messages"] == state["messages"]
        assert loaded["current_node"] == state["current_node"]
        assert loaded["code_delta"] == state["code_delta"]

    def test_save_with_lock_returns_false_when_contended(self) -> None:
        """``save_with_lock`` returns False when another client holds the lock."""
        from src.control_plane.checkpoint import DistributedCheckpointManager

        thread_id: str = "thread-contended"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()
        lock_key: str = f"redlock:checkpoint:{thread_id}"

        manager._client.set(lock_key, "fake-owner", px=10000, nx=True)

        try:
            result: bool = manager.save_with_lock(thread_id, _sample_state())
            assert result is False, (
                "save_with_lock must return False when lock is held by another owner"
            )
        finally:
            manager._client.delete(lock_key)


# ═══════════════════════════════════════════════════════════════════════════════
# StateMachineEngine — class wrapper tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestStateMachineEngine:
    """Tests for the StateMachineEngine class wrapper."""

    def test_engine_compiles_without_checkpoint(self) -> None:
        """Engine compiles without a checkpoint manager and invokes correctly."""
        from src.control_plane.graph import StateMachineEngine

        engine: StateMachineEngine = StateMachineEngine()
        assert engine.compiled_graph is not None, (
            "compiled_graph must not be None after construction"
        )

        # Clean mock path completes to end_node (no tool = no HITL)
        result: AgentState = engine.invoke(_empty_state(), "test-engine-001")
        assert result["current_node"] == "end", (
            f"Clean path should complete to end_node, got {result['current_node']}"
        )

    def test_engine_invokes_with_checkpoint(self) -> None:
        """Engine with checkpoint manager pre-loads saved state."""
        from src.control_plane.checkpoint import DistributedCheckpointManager
        from src.control_plane.graph import StateMachineEngine

        thread_id: str = "test-engine-002"
        manager: DistributedCheckpointManager = DistributedCheckpointManager()

        mid_state: AgentState = {
            "messages": ["[checkpoint] engine-preload"],
            "current_node": "intent_parse",
            "code_delta": "error_v1",
            "execution_logs": ["[checkpoint] engine-log"],
            "retry_count": 1,
        }
        manager.save(thread_id, mid_state)

        engine: StateMachineEngine = StateMachineEngine(checkpoint_manager=manager)
        result: AgentState = engine.invoke(_empty_state(), thread_id)

        assert "[checkpoint] engine-preload" in result["messages"], (
            "Checkpoint state must be visible in invoke result"
        )
        assert "[checkpoint] engine-log" in result["execution_logs"]
        assert result["current_node"] in ("end", "human_interrupt"), (
            f"Expected end or human_interrupt, got {result['current_node']}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# ConfigSettings — config/settings.py correctness
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigSettings:
    """Tests for config/settings.py correctness."""

    def test_settings_load_defaults(self) -> None:
        """Config module loads with expected default values."""
        import importlib
        import config.settings as settings

        settings = importlib.reload(settings)

        assert isinstance(settings.REDIS_PASSWORD, str), (
            "REDIS_PASSWORD must be a string"
        )
        assert settings.REDIS_DB_CHECKPOINT == 0, (
            "DB 0 is A's exclusive checkpoint partition"
        )
        assert settings.REDIS_DB_SCHEMA == 1, (
            "DB 1 is B's exclusive schema partition"
        )
        assert settings.REDIS_DB_SECURITY == 2, (
            "DB 2 is C's exclusive security partition"
        )
        assert isinstance(settings.PRIMARY_LLM, str)
        assert isinstance(settings.FAST_LLM, str)
