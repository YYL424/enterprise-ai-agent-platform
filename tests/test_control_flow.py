"""Pytest tests for the linear LangGraph control plane state machine."""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import pytest

from src.control_plane.graph import (
    app,
    end_node,
    intent_parse_node,
    plan_generate_node,
    start_node,
)
from src.control_plane.state import AgentState


def _empty_state() -> AgentState:
    return {
        "messages": [],
        "current_node": "",
        "code_delta": "",
        "execution_logs": [],
        "retry_count": 0,
    }


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
        pre_state["messages"] = ["user: 训练一个BERT模型"]
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


class TestFullGraphInvocation:
    def test_all_fields_present(self) -> None:
        result = app.invoke(_empty_state())
        expected = {"messages", "current_node", "code_delta", "execution_logs", "retry_count"}
        assert set(result.keys()) == expected

    def test_final_current_node_is_end(self) -> None:
        result = app.invoke(_empty_state())
        assert result["current_node"] == "end"

    def test_messages_grew(self) -> None:
        result = app.invoke(_empty_state())
        assert len(result["messages"]) >= 4

    def test_execution_logs_populated(self) -> None:
        result = app.invoke(_empty_state())
        assert len(result["execution_logs"]) >= 4

    def test_retry_count_zero(self) -> None:
        result = app.invoke(_empty_state())
        assert result["retry_count"] == 0
