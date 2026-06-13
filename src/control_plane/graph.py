"""Linear LangGraph state machine: start → intent_parse → plan_generate → end."""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from typing import Any

from langgraph.graph import END, StateGraph
from loguru import logger

from src.control_plane.state import AgentState


def start_node(state: AgentState) -> dict:
    """First node — initialises the pipeline with a start marker."""
    return {
        "messages": ["[start_node] pipeline initialised"],
        "current_node": "start_node",
        "code_delta": "",
        "execution_logs": ["start_node completed"],
        "retry_count": 0,
    }


def intent_parse_node(state: AgentState) -> dict:
    """Parse user intent from accumulated messages."""
    parsed = f"[intent] parsed from {len(state['messages'])} message(s)"
    return {
        "messages": ["[intent_parse_node] " + parsed],
        "current_node": "intent_parse",
        "code_delta": "intent_parsed_v1",
        "execution_logs": ["intent_parse_node completed"],
        "retry_count": state.get("retry_count", 0),
    }


def plan_generate_node(state: AgentState) -> dict:
    """Generate an execution plan based on parsed intent."""
    plan = f"[plan] generated plan for intent: {state.get('code_delta', 'unknown')}"
    return {
        "messages": ["[plan_generate_node] " + plan],
        "current_node": "plan_generate",
        "code_delta": state.get("code_delta", ""),
        "execution_logs": ["plan_generate_node completed"],
        "retry_count": state.get("retry_count", 0),
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


def build_graph() -> Any:
    """Build and return the compiled linear state machine."""
    graph = StateGraph(AgentState)

    graph.add_node("start_node", start_node)
    graph.add_node("intent_parse_node", intent_parse_node)
    graph.add_node("plan_generate_node", plan_generate_node)
    graph.add_node("end_node", end_node)

    graph.add_edge("start_node", "intent_parse_node")
    graph.add_edge("intent_parse_node", "plan_generate_node")
    graph.add_edge("plan_generate_node", "end_node")
    graph.add_edge("end_node", END)

    graph.set_entry_point("start_node")
    return graph.compile()


app = build_graph()


if __name__ == "__main__":
    initial_state: AgentState = {
        "messages": [],
        "current_node": "",
        "code_delta": "",
        "execution_logs": [],
        "retry_count": 0,
    }

    result = app.invoke(initial_state)
    logger.info("Final state:")
    for key, value in result.items():
        logger.info("  {}: {}", key, value)
