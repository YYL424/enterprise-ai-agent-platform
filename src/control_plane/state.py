"""AgentState TypedDict definition for the control plane state machine."""

import operator
from typing import Annotated, TypedDict


class AgentState(TypedDict):
    """Shared state flowing through the LangGraph state machine.

    Fields annotated with ``operator.add`` automatically append on
    parallel/duplicate writes rather than overwriting.
    """

    messages: Annotated[list[str], operator.add]
    current_node: str
    code_delta: str
    execution_logs: Annotated[list[str], operator.add]
    retry_count: int
