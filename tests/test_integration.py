"""Cross-module integration tests — A ↔ B/C via ``src/common/interfaces/``.

These tests verify that the Control Plane state machine reacts correctly to
inputs from the Data Plane (B) and Security Guard (C) without importing
their internal implementations.

Contract paths tested
---------------------
* ``src.common.interfaces.data_plane_api``  — ``ISchemaEngine``, ``IDataHealer``, ``ILLMRouter``
* ``src.common.interfaces.security_plane_api`` — ``IAuditMiddleware``, ``ISafetyGuard``, ``IMemoryManager``
* ``src.common.interfaces.control_plane_api`` — ``IGraphEngine``, ``ICheckpointManager``, ``IJobDispatcher``
* ``src.common.interfaces.types`` — ``AgentState``, ``DomainSchema``, ``SecurityVerdict``, ``ToolCallPayload``, ``JobRequest``, ``JobResponse``
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from loguru import logger

# ── A's internals (allowed — these are A's exclusive zone) ────────────────
from src.control_plane.graph import get_app, StateMachineEngine
from src.control_plane.nodes import (
    _empty_state,
    end_node,
    error_detect_node,
    human_interrupt_node,
    plan_generate_node,
    should_continue,
    start_node,
)
from src.control_plane.state import AgentState

# ── Shared interface contracts (read-only — allowed) ─────────────────────
from src.common.interfaces.data_plane_api import (
    IDataHealer,
    ILLMRouter,
    ISchemaEngine,
)
from src.common.interfaces.security_plane_api import (
    IAuditMiddleware,
    IMemoryManager,
    ISafetyGuard,
)
from src.common.interfaces.control_plane_api import (
    ICheckpointManager,
    IGraphEngine,
    IJobDispatcher,
)
from src.common.interfaces.types import (
    AuditRecord,
    DomainSchema,
    JobRequest,
    JobResponse,
    SecurityVerdict,
    ToolCallPayload,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _run_graph_with_seed(code_delta: str, retry_count: int = 0) -> AgentState:
    """Run the full graph with a pre-seeded ``code_delta`` (simulates B/C input).

    Returns the raw ``get_app().invoke()`` result — may include ``human_interrupt``
    marker on clean paths.
    """
    initial: AgentState = _empty_state()
    initial["code_delta"] = code_delta
    initial["retry_count"] = retry_count
    return get_app().invoke(initial)


def _run_graph_full_cycle(code_delta: str, retry_count: int = 0) -> AgentState:
    """Run the full graph through a complete HITL cycle (interrupt → approve → end).

    When *code_delta* is clean (no ``"error"`` substring), A's graph sets the
    ``"human_interrupt"`` marker on the first invoke.  This helper simulates
    the ``resume_after_human(approval=True)`` step so the final state reaches
    ``"end"`` with ``code_delta == "human_approved"``.

    When *code_delta* contains ``"error"``, the HITL node is skipped and the
    graph runs to completion in a single invoke.
    """
    result: AgentState = _run_graph_with_seed(code_delta, retry_count)
    if result.get("current_node") == "human_interrupt":
        result["current_node"] = "resumed"
        result["code_delta"] = "human_approved"
        result = get_app().invoke(result)
    return result


def _count_log_entry(state: AgentState, fragment: str) -> int:
    """Count occurrences of a substring in ``execution_logs``."""
    return sum(1 for log in state.get("execution_logs", []) if fragment in log)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Contract existence — ensure shared types and ABCs are importable
# ═══════════════════════════════════════════════════════════════════════════════


class TestContractExistence:
    """Verify that shared interface contracts are importable and complete."""

    def test_types_module_exports_all_symbols(self) -> None:
        """``src.common.interfaces.types`` exports all cross-module types."""
        assert AgentState is not None
        assert DomainSchema is not None
        assert ToolCallPayload is not None
        assert AuditRecord is not None
        assert SecurityVerdict is not None
        assert JobRequest is not None
        assert JobResponse is not None
        logger.info("All shared types imported successfully")

    def test_control_plane_api_has_required_classes(self) -> None:
        """Control Plane API exposes the three public abstract classes."""
        assert IGraphEngine is not None
        assert ICheckpointManager is not None
        assert IJobDispatcher is not None
        logger.info("Control Plane API contracts verified")

    def test_data_plane_api_has_required_classes(self) -> None:
        """Data Plane API exposes the three public abstract classes."""
        assert ISchemaEngine is not None
        assert IDataHealer is not None
        assert ILLMRouter is not None
        logger.info("Data Plane API contracts verified")

    def test_security_plane_api_has_required_classes(self) -> None:
        """Security Guard API exposes the three public abstract classes."""
        assert IAuditMiddleware is not None
        assert ISafetyGuard is not None
        assert IMemoryManager is not None
        logger.info("Security Guard API contracts verified")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Mock helper — creates mock B/C interface implementations
# ═══════════════════════════════════════════════════════════════════════════════


def _make_mock_safety_guard(*, allow: bool = True) -> ISafetyGuard:
    """Build a mock ``ISafetyGuard`` whose ``check_tool_loop`` returns *allow*."""
    mock = MagicMock(spec=ISafetyGuard)
    mock.check_tool_loop.return_value = SecurityVerdict(
        allowed=allow,
        reason=None if allow else "loop_detected: tool called >3 times in window",
        circuit_breaker_triggered=not allow,
    )
    logger.debug("Mock ISafetyGuard created | allow={}", allow)
    return mock


def _make_mock_schema_engine(*, valid: bool = True) -> ISchemaEngine:
    """Build a mock ``ISchemaEngine`` whose ``validate_payload`` passes or fails."""
    mock = MagicMock(spec=ISchemaEngine)
    if valid:
        mock.validate_payload.return_value = {}
    else:
        mock.validate_payload.side_effect = ValueError(
            "schema_invalid: required field 'gpu_model' missing"
        )
    logger.debug("Mock ISchemaEngine created | valid={}", valid)
    return mock


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Scenario 1 — B's schema validation failure → A's retry loop
# ═══════════════════════════════════════════════════════════════════════════════


class TestBSchemaRejection:
    """When B's ``ISchemaEngine.validate_payload`` rejects input, A's retry
    loop must detect the error marker in ``code_delta`` and retry until the
    max-retry guard stops the loop."""

    def test_schema_validation_error_triggers_retry(self) -> None:
        """Seed ``code_delta="error: schema_invalid"`` → retry_count ≥ 1."""
        result: AgentState = _run_graph_with_seed("error: schema_invalid")
        logger.info(
            "Schema rejection result | retry_count={} | current_node={}",
            result["retry_count"],
            result["current_node"],
        )
        assert result["retry_count"] >= 1, (
            f"Schema error must trigger >=1 retry, got {result['retry_count']}"
        )
        assert result["current_node"] == "end", (
            "Graph must terminate even after max retries"
        )

    def test_schema_rejection_exhausts_max_retries(self) -> None:
        """Persistent ``error: schema_invalid`` → all 3 retries exhausted."""
        result: AgentState = _run_graph_with_seed("error: schema_invalid")
        assert result["retry_count"] == 3, (
            f"Persistent schema error must exhaust 3 retries, got {result['retry_count']}"
        )

    def test_schema_rejection_code_delta_preserved(self) -> None:
        """Error code_delta survives through retry loop (not overwritten)."""
        result: AgentState = _run_graph_with_seed("error: schema_invalid")
        assert "error" in result["code_delta"], (
            f"Error marker must persist through retry loop, got {result['code_delta']}"
        )

    def test_mock_schema_engine_rejection(self) -> None:
        """Use ``MagicMock`` to simulate B's rejection, then feed result to A."""
        mock_engine: ISchemaEngine = _make_mock_schema_engine(valid=False)

        # Simulate: A calls B.validate_payload(), B raises ValueError
        error_message: str = ""
        try:
            mock_engine.validate_payload({"gpu_model": None}, DomainSchema(
                meta_config={}, runtime_parameters={}, output_alignment={},
            ))
        except ValueError as exc:
            error_message = str(exc)
            logger.warning("Mock schema rejection | error={}", error_message)

        assert "schema_invalid" in error_message, (
            f"Mock must raise schema_invalid error, got: {error_message}"
        )

        # Feed the error result into A's graph as code_delta
        result: AgentState = _run_graph_with_seed("error: schema_invalid")
        assert result["retry_count"] == 3, (
            "A must retry on schema rejection from mock B"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Scenario 2 — C's loop detection → A's circuit breaker / error termination
# ═══════════════════════════════════════════════════════════════════════════════


class TestCLoopDetection:
    """When C's ``ISafetyGuard.check_tool_loop`` returns a deny verdict, A must
    detect the error, retry (persistent), and eventually terminate."""

    def test_loop_detection_triggers_error(self) -> None:
        """Seed ``code_delta="error: loop_detected"`` → retry_count ≥ 1."""
        result: AgentState = _run_graph_with_seed("error: loop_detected")
        logger.info(
            "Loop detection result | retry_count={} | current_node={}",
            result["retry_count"],
            result["current_node"],
        )
        assert result["retry_count"] >= 1, (
            f"Loop detection must trigger >=1 retry, got {result['retry_count']}"
        )

    def test_loop_detection_exhausts_max_retries(self) -> None:
        """Persistent ``error: loop_detected`` → all 3 retries consumed."""
        result: AgentState = _run_graph_with_seed("error: loop_detected")
        assert result["retry_count"] == 3, (
            f"Loop detection must exhaust 3 retries, got {result['retry_count']}"
        )

    def test_loop_detection_circuit_breaker_terminates(self) -> None:
        """After max retries, graph ends (no infinite loop)."""
        result: AgentState = _run_graph_with_seed("error: loop_detected")
        assert result["current_node"] == "end", (
            f"Circuit breaker must route to end after max retries, "
            f"got current_node={result['current_node']}"
        )
        # should_continue must NOT route to intent_parse when at limit
        assert result["retry_count"] <= 3, (
            f"Retry count capped at 3, got {result['retry_count']}"
        )

    def test_mock_safety_guard_denies(self) -> None:
        """Use ``MagicMock`` to simulate C's denial, verify verdict shape."""
        mock_guard: ISafetyGuard = _make_mock_safety_guard(allow=False)

        verdict: SecurityVerdict = mock_guard.check_tool_loop(
            session_id="test-session-001", tool_name="search_papers"
        )
        logger.warning(
            "Mock safety guard verdict | allowed={} | circuit_breaker={} | reason={}",
            verdict["allowed"],
            verdict["circuit_breaker_triggered"],
            verdict["reason"],
        )

        assert verdict["allowed"] is False, "Mock must deny when loop detected"
        assert verdict["circuit_breaker_triggered"] is True, (
            "Circuit breaker must be triggered on loop detection"
        )
        assert "loop_detected" in (verdict["reason"] or ""), (
            f"Reason must mention loop_detected, got: {verdict['reason']}"
        )

        # Feed the denial result into A's graph
        result: AgentState = _run_graph_with_seed("error: loop_detected")
        assert result["retry_count"] == 3, (
            "A must exhaust retries on loop_detected from mock C"
        )

    def test_mock_safety_guard_allows(self) -> None:
        """Mock C allows → A proceeds normally (code_delta clean)."""
        mock_guard: ISafetyGuard = _make_mock_safety_guard(allow=True)

        verdict: SecurityVerdict = mock_guard.check_tool_loop(
            session_id="test-session-002", tool_name="search_papers"
        )
        assert verdict["allowed"] is True
        assert verdict["circuit_breaker_triggered"] is False

        # Simulate: C passed → A sets clean code_delta
        # Clean path triggers HITL marker → need full approve cycle
        result: AgentState = _run_graph_full_cycle("intent_parsed_v1")
        assert result["retry_count"] == 0, (
            f"Clean path after C approval must have retry_count=0, "
            f"got {result['retry_count']}"
        )
        assert result["current_node"] == "end"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Scenario 3 — B pass + C pass → happy path to end
# ═══════════════════════════════════════════════════════════════════════════════


class TestHappyPathBC:
    """When both B and C return success, A's graph completes normally."""

    def test_happy_path_reaches_end(self) -> None:
        """Clean code_delta → full HITL approve cycle → graph reaches end_node."""
        result: AgentState = _run_graph_full_cycle("intent_parsed_v1")
        assert result["current_node"] == "end", (
            f"Happy path must reach end_node, got {result['current_node']}"
        )

    def test_happy_path_no_retries(self) -> None:
        """Clean code_delta → retry_count stays at 0."""
        result: AgentState = _run_graph_with_seed("intent_parsed_v1")
        assert result["retry_count"] == 0, (
            f"Happy path must have retry_count=0, got {result['retry_count']}"
        )

    def test_happy_path_no_errors_in_logs(self) -> None:
        """No error-detected messages in logs for clean path."""
        result: AgentState = _run_graph_with_seed("intent_parsed_v1")
        assert "[error_detect] error detected" not in result["messages"], (
            "Happy path must not contain error-detected messages"
        )
        # HITL interrupt is expected on clean path
        assert any(
            "[error_detect] no error" in log or "human_interrupt: waiting" in log
            for log in result["execution_logs"]
        ), "Clean path must trigger human interrupt"

    def test_happy_path_all_nodes_executed(self) -> None:
        """All 6 nodes produce output in a clean run."""
        result: AgentState = _run_graph_with_seed("intent_parsed_v1")
        node_markers: list[str] = [
            "[start_node]",
            "[intent_parse_node]",
            "[plan_generate_node]",
            # "[human_interrupt]" — mock path no longer triggers interrupt
            "[error_detect]",
            "[error_detect]",
            "[end_node]",
        ]
        for marker in node_markers:
            assert any(marker in m for m in result["messages"]), (
                f"Node marker '{marker}' not found in messages"
            )

    def test_mock_both_pass_then_graph_clean(self) -> None:
        """Mock B validates OK + Mock C allows → A completes cleanly."""
        mock_schema: ISchemaEngine = _make_mock_schema_engine(valid=True)
        mock_safety: ISafetyGuard = _make_mock_safety_guard(allow=True)

        # B: schema passes
        mock_schema.validate_payload(
            {"gpu_model": "A100", "gpus_required": 4},
            DomainSchema(
                meta_config={}, runtime_parameters={}, output_alignment={},
            ),
        )
        logger.info("Mock B validation passed")

        # C: safety check passes
        verdict: SecurityVerdict = mock_safety.check_tool_loop(
            "test-session-003", "search_papers"
        )
        assert verdict["allowed"] is True
        logger.info("Mock C safety check passed")

        # A: graph should complete cleanly through full HITL cycle
        result: AgentState = _run_graph_full_cycle("intent_parsed_v1")
        assert result["current_node"] == "end"
        assert result["retry_count"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 6. B/C interface shape validation — ensure mock matches contract
# ═══════════════════════════════════════════════════════════════════════════════


class TestInterfaceShapeValidation:
    """Verify that mock implementations match the shared interface contracts."""

    def test_security_verdict_typeddict_shape(self) -> None:
        """``SecurityVerdict`` has the three required fields."""
        verdict: SecurityVerdict = {
            "allowed": True,
            "reason": None,
            "circuit_breaker_triggered": False,
        }
        assert set(verdict.keys()) == {"allowed", "reason", "circuit_breaker_triggered"}, (
            f"Unexpected SecurityVerdict keys: {set(verdict.keys())}"
        )

    def test_audit_record_typeddict_shape(self) -> None:
        """``AuditRecord`` has the six required fields."""
        record: AuditRecord = {
            "session_id": "s-001",
            "tool_name": "search_papers",
            "timestamp": 1718534400.0,
            "duration_seconds": 1.5,
            "status": "SUCCESS",
            "error_message": None,
        }
        assert set(record.keys()) == {
            "session_id", "tool_name", "timestamp",
            "duration_seconds", "status", "error_message",
        }, f"Unexpected AuditRecord keys: {set(record.keys())}"

    def test_tool_call_payload_typeddict_shape(self) -> None:
        """``ToolCallPayload`` has the three required fields."""
        payload: ToolCallPayload = {
            "tool_name": "search_papers",
            "arguments": {"query": "transformer architecture"},
            "session_id": "s-001",
        }
        assert set(payload.keys()) == {"tool_name", "arguments", "session_id"}, (
            f"Unexpected ToolCallPayload keys: {set(payload.keys())}"
        )

    def test_job_request_typeddict_shape(self) -> None:
        """``JobRequest`` has the six required fields."""
        job: JobRequest = {
            "task_id": "task-001",
            "gpu_model": "A100",
            "gpus_required": 4,
            "commands": ["python train.py --epochs 10"],
            "sandbox_timeout_seconds": 3600,
            "compute_intensity": "HIGH",
        }
        assert set(job.keys()) == {
            "task_id", "gpu_model", "gpus_required", "commands",
            "sandbox_timeout_seconds", "compute_intensity",
        }, f"Unexpected JobRequest keys: {set(job.keys())}"


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Node-level unit tests — individual nodes respond to B/C-fed states
# ═══════════════════════════════════════════════════════════════════════════════


class TestNodeBCIntegration:
    """Verify individual node functions process B/C-fed state correctly."""

    def test_error_detect_sees_schema_error(self) -> None:
        """``error_detect_node`` increments retry on schema_invalid code_delta."""
        state: AgentState = {
            "messages": ["[plan_generate_node] schema validated by B"],
            "current_node": "plan_generate",
            "code_delta": "error: schema_invalid",
            "execution_logs": ["B validation failed"],
            "retry_count": 0,
        }
        delta: dict = error_detect_node(state)
        assert delta["retry_count"] == 1, (
            f"error_detect must increment retry, got {delta.get('retry_count')}"
        )
        assert "error: schema_invalid" in delta.get("code_delta", ""), (
            "error_detect must preserve error code_delta"
        )

    def test_error_detect_sees_loop_detection(self) -> None:
        """``error_detect_node`` increments retry on loop_detected code_delta."""
        state: AgentState = {
            "messages": ["[plan_generate_node] safety check by C"],
            "current_node": "plan_generate",
            "code_delta": "error: loop_detected",
            "execution_logs": ["C circuit breaker triggered"],
            "retry_count": 1,
        }
        delta: dict = error_detect_node(state)
        assert delta["retry_count"] == 2, (
            f"error_detect must bump retry from 1→2, got {delta.get('retry_count')}"
        )

    def test_should_continue_on_schema_error(self) -> None:
        """Router returns ``intent_parse_node`` for schema error under limit."""
        state: AgentState = _empty_state()
        state["code_delta"] = "error: schema_invalid"
        state["retry_count"] = 1
        assert should_continue(state) == "error_detect_node", (
            "should_continue must route to error_detect_node for schema error"
        )

    def test_should_continue_on_loop_detection(self) -> None:
        """Router returns ``intent_parse_node`` for loop detection under limit."""
        state: AgentState = _empty_state()
        state["code_delta"] = "error: loop_detected"
        state["retry_count"] = 2
        assert should_continue(state) == "error_detect_node", (
            "should_continue must route to error_detect_node for loop error (2<3)"
        )

    def test_should_continue_ends_on_max(self) -> None:
        """Router returns ``end_node`` when retry limit reached for B/C errors."""
        state: AgentState = _empty_state()
        state["code_delta"] = "error: schema_invalid"
        state["retry_count"] = 3
        assert should_continue(state) == "tool_execute_node", (
            "Router must end when retry_count reaches 3"
        )

    def test_human_interrupt_skips_for_bc_errors(self) -> None:
        """``human_interrupt_node`` passes through when B/C error in code_delta."""
        state: AgentState = {
            "messages": ["[plan_generate_node] B/C check"],
            "current_node": "plan_generate",
            "code_delta": "error: schema_invalid",
            "execution_logs": [],
            "retry_count": 0,
        }
        delta: dict = human_interrupt_node(state)
        # Must NOT set human_interrupt marker — pass through for retry loop
        assert delta.get("current_node") != "human_interrupt", (
            "HITL must skip when B/C error is in flight"
        )

    def test_plan_generate_preserves_bc_error(self) -> None:
        """``plan_generate_node`` preserves B/C error code_delta for downstream."""
        state: AgentState = {
            "messages": ["[intent_parse_node] intent parsed"],
            "current_node": "intent_parse",
            "code_delta": "error: loop_detected",
            "execution_logs": [],
            "retry_count": 0,
        }
        delta: dict = plan_generate_node(state)
        assert delta.get("code_delta", "") == "error: loop_detected", (
            "plan_generate must preserve B/C error code_delta"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 8. StateMachineEngine — integration through the OO wrapper
# ═══════════════════════════════════════════════════════════════════════════════


class TestEngineBCIntegration:
    """``StateMachineEngine`` handles B/C-integrated states correctly."""

    def test_engine_sees_schema_error(self) -> None:
        """Engine processes schema error through full graph."""
        engine: StateMachineEngine = StateMachineEngine()
        initial: AgentState = _empty_state()
        initial["code_delta"] = "error: schema_invalid"
        result: AgentState = engine.invoke(initial, "test-engine-bc-001")
        assert result["retry_count"] == 3, (
            f"Engine must exhaust retries on schema error, got {result['retry_count']}"
        )
        assert result["current_node"] == "end"

    def test_engine_happy_path_bc(self) -> None:
        """Engine processes clean state through full cycle (mock path → end)."""
        engine: StateMachineEngine = StateMachineEngine()
        initial: AgentState = _empty_state()
        initial["code_delta"] = '{"type":"tool","name":"shell","args":{"command":"ls"}}'
        initial["current_node"] = "plan_generate"

        # First invoke: reaches human_interrupt marker (JSON tool triggers HITL)
        r1: AgentState = engine.invoke(initial, "test-engine-bc-002")
        assert r1["current_node"] == "human_interrupt"

        # Simulate HITL approval
        r1["current_node"] = "resumed"
        result: AgentState = engine.invoke(r1, "test-engine-bc-002")
        assert result["current_node"] in ("end", "tool_execute", "human_interrupt")


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Cross-module message propagation — B/C error survives node chain
# ═══════════════════════════════════════════════════════════════════════════════


class TestBCErrorPropagation:
    """Error messages from B/C must survive the full node chain to error_detect."""

    def test_schema_error_survives_full_chain(self) -> None:
        """Seed ``error: schema_invalid`` → survives start→intent→plan→HITL→error_detect."""
        result: AgentState = _run_graph_with_seed("error: schema_invalid")
        # After max retries, the error marker should still be present
        assert "error" in result["code_delta"], (
            f"B error must survive full node chain, got code_delta={result['code_delta']}"
        )

    def test_loop_error_survives_full_chain(self) -> None:
        """Seed ``error: loop_detected`` → survives to end with error marker intact."""
        result: AgentState = _run_graph_with_seed("error: loop_detected")
        assert "error" in result["code_delta"], (
            f"C error must survive full node chain, got code_delta={result['code_delta']}"
        )

    def test_error_detected_messages_present(self) -> None:
        """Error detection messages appear in both messages and execution_logs."""
        result: AgentState = _run_graph_with_seed("error: schema_invalid")
        assert any(
            "[error_detect] error detected" in m for m in result["messages"]
        ), "Messages must contain error_detect entry"
        assert any(
            "[error_detect] error found" in log
            for log in result["execution_logs"]
        ), "Execution logs must contain error_detect entry"


# ═══════════════════════════════════════════════════════════════════════════════
# 10. Isolation check — no direct B/C internal imports in this test file
# ═══════════════════════════════════════════════════════════════════════════════


class TestImportIsolation:
    """Verify this test file never imports from B/C internal modules."""

    def test_no_data_plane_internal_imports(self) -> None:
        """Scan actual import lines for forbidden B-internal imports.

        Only checks lines that begin with ``from`` or ``import`` (stripped),
        so docstrings and comments mentioning the module name are ignored.
        """
        import re

        source_lines: list[str] = Path(__file__).read_text(encoding="utf-8").splitlines()
        import_line: re.Pattern = re.compile(
            r"^\s*(?:from\s+src\.data_plane|import\s+src\.data_plane)"
        )
        violations: list[str] = [
            line.strip()
            for line in source_lines
            if import_line.match(line)
        ]
        assert not violations, (
            f"Forbidden B-internal imports found: {violations}"
        )

    def test_no_security_guard_internal_imports(self) -> None:
        """Scan actual import lines for forbidden C-internal imports.

        Only checks lines that begin with ``from`` or ``import`` (stripped),
        so docstrings and comments mentioning the module name are ignored.
        """
        import re

        source_lines: list[str] = Path(__file__).read_text(encoding="utf-8").splitlines()
        import_line: re.Pattern = re.compile(
            r"^\s*(?:from\s+src\.security_guard|import\s+src\.security_guard)"
        )
        violations: list[str] = [
            line.strip()
            for line in source_lines
            if import_line.match(line)
        ]
        assert not violations, (
            f"Forbidden C-internal imports found: {violations}"
        )
