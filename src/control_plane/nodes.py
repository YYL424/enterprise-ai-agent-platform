"""Node functions for the LangGraph control plane state machine.

Day 7++ — Native Checkpointer + interrupt()
--------------------------------------------
With ``RedisCheckpointSaver`` passed to ``graph.compile()``, LangGraph
automatically persists state after every node.  This eliminates the
hand-rolled marker-passthrough scheme:

* ``human_interrupt_node`` calls ``interrupt()`` (LangGraph native) to
  pause the graph when a tool requires human approval.
* ``Command(resume=...)`` resumes execution from the interrupt point.
* All upstream passthrough guards are deleted — LangGraph resumes from
  the last checkpoint, not from START.
* State merge is handled by LangGraph's reducer annotations.
"""

from __future__ import annotations

import json
import re
from typing import Any

from loguru import logger
from langgraph.types import interrupt

from src.control_plane.state import AgentState


# ── Constants ────────────────────────────────────────────────────────────────────

_MAX_RETRIES: int = 3

_SYSTEM_PROMPT = """You are an Enterprise AI Agent. Complete user tasks using tools.

## Response Format (JSON ONLY)
- Tool call: {{"tool": "<name>", "args": {{...}}}}
- Final answer: {{"answer": "<response>"}}
- Clarification: {{"question": "<question>"}}

## Available Tools
{tool_defs}

## Rules
1. ONE JSON per response. No other text.
2. Observe each tool output before deciding next step.
3. {{"answer": "..."}} when done.
4. 若连续 2 次工具调用返回空结果或无有效数据，停止尝试，直接返回 {{"answer": "无法获取实时信息，可能原因：网站结构复杂或访问受限。"}}"""

_TOOL_DEFS = """## shell
执行 shell 命令。Args: {{"command": "<string>"}}

## read_file
读取文件内容。Args: {{"path": "<relative_path>"}}

## write_file
写入文件。Args: {{"path": "<relative_path>", "content": "<string>"}}

## list_dir
列出目录内容。Args: {{"path": "<relative_path>"}}

## grep
搜索文件内容（正则）。Args: {{"pattern": "<regex>", "path": "<relative_path>"}}

## fetch_web
抓取网页并清洗 HTML，返回纯文本。Args: {{"url": "<full_url>"}}"""


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


def _build_system_prompt() -> str:
    """Build the system prompt with tool definitions."""
    return _SYSTEM_PROMPT.format(tool_defs=_TOOL_DEFS)


def _get_llm():
    """Return LLMRouter from registry, or None if unavailable."""
    try:
        from src.common.registry import registry
        return registry.get("llm_router")
    except Exception:
        return None


def _get_tool_table() -> dict[str, Any]:
    """Return the tool table from registry, or an empty dict if unavailable."""
    try:
        from src.common.registry import registry
        return registry.get("tool_table")
    except Exception:
        return {}


def _get_data_healer():
    """Return DataHealer from registry, or None if unavailable."""
    try:
        from src.common.registry import registry
        return registry.get("data_healer")
    except Exception:
        return None


def _parse_llm_response(raw: str) -> dict[str, Any] | None:
    """Parse LLM response, attempting repair via DataHealer on failure."""
    cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip()
    cleaned = re.sub(r"```\s*$", "", cleaned).strip()

    try:
        return json.loads(cleaned)  # type: ignore[no-any-return]
    except json.JSONDecodeError:
        pass

    healer = _get_data_healer()
    if healer is not None:
        try:
            repaired = healer.heal_truncated_json(cleaned)
            if repaired is not None:
                return json.loads(repaired)  # type: ignore[no-any-return]
        except Exception:
            pass

    return None


def _infer_decision_type(parsed: dict[str, Any]) -> str:
    """Infer decision type from a parsed JSON dict.

    Returns one of ``"tool"``, ``"answer"``, ``"question"``,
    ``"tool_result"``, or ``""`` (unknown).
    """
    dt = parsed.get("type", "")
    if dt:
        return dt
    # Legacy format fallback
    if "tool" in parsed and "args" in parsed:
        return "tool"
    if "answer" in parsed:
        return "answer"
    if "question" in parsed:
        return "question"
    if "tool_result" in parsed or "output_summary" in parsed:
        return "tool_result"
    return ""


# ── Node functions ─────────────────────────────────────────────────────────────


def start_node(state: AgentState) -> dict:
    """First node — initialises the pipeline."""
    return {
        "messages": ["[start_node] pipeline initialised"],
        "current_node": "start_node",
        "code_delta": state.get("code_delta") or "",
        "execution_logs": ["start_node completed"],
        "retry_count": state.get("retry_count") or 0,
    }


def intent_parse_node(state: AgentState) -> dict:
    """Parse user intent, with retry-aware error handling.

    *Retry-loop behaviour*:

    - ``code_delta == "error_v1"`` **and** ``retry_count > 0``: repaired
      in one pass → ``"intent_parsed_v1"``.
    - Any *other* ``code_delta`` containing ``"error"``: persistent,
      preserved so ``error_detect_node`` can re-trigger.
    - Otherwise: ``"intent_parsed_v1"`` (standard path).
    """
    current_cd: str = state.get("code_delta") or ""
    retry_count: int = state.get("retry_count") or 0

    if retry_count > 0 and "error" in current_cd:
        if current_cd == "error_v1":
            code_delta: str = "intent_parsed_v1"
            logger.info(
                "Recoverable error fixed on retry | code_delta={} → {}",
                current_cd, code_delta,
            )
        else:
            code_delta = current_cd
            logger.warning(
                "Persistent error preserved | code_delta={} | retry_count={}",
                current_cd, retry_count,
            )
    elif "error" not in current_cd and current_cd and not current_cd.startswith("{"):
        code_delta = "intent_parsed_v1"
    elif "error" not in current_cd and not current_cd:
        code_delta = "intent_parsed_v1"
    else:
        code_delta = current_cd

    msg_count = len(state.get("messages", []))
    return {
        "messages": [f"[intent_parse_node] [intent] parsed from {msg_count} message(s)"],
        "current_node": "intent_parse",
        "code_delta": code_delta,
        "execution_logs": ["intent_parse_node completed"],
        "retry_count": retry_count,
    }


def plan_generate_node(state: AgentState) -> dict:
    """Call LLM to generate the next decision (tool / answer / question).

    Encodes the decision into ``code_delta`` as a JSON string.
    Falls back to mock behaviour when LLM is unavailable.

    When ``code_delta`` contains a ``tool_result`` (loop-back from tool
    execution), the LLM is called with the tool output in context so it
    can decide the next step.

    .. rubric:: B/C 集成预留点

    * **TODO(B)**: ``validate_schema(plan_payload, schema_name)``
    * **TODO(C)**: ``audit_tool_invocation(session_id, tool_name, path_hash)``
    """
    current_cd: str = state.get("code_delta") or ""
    retry_count: int = state.get("retry_count") or 0

    # ── JSON passthrough logic ────────────────────────────────────────────
    if current_cd.startswith("{"):
        try:
            parsed_cd = json.loads(current_cd)
            cd_type = _infer_decision_type(parsed_cd)

            # tool_result → call LLM with tool output (don't passthrough)
            if cd_type == "tool_result":
                logger.info(
                    "plan_generate_node — tool_result detected, "
                    "will call LLM for next decision | tool={}",
                    parsed_cd.get("tool", "?"),
                )
                # Fall through to LLM call
            else:
                # Decision JSON (tool / answer / question) — passthrough
                return {
                    "messages": ["[plan_generate_node] preserving JSON payload"],
                    "current_node": "plan_generate",
                    "code_delta": current_cd,
                    "execution_logs": ["plan_generate_node (JSON passthrough)"],
                    "retry_count": retry_count,
                }
        except json.JSONDecodeError:
            pass  # Not valid JSON → continue to LLM call

    # ── Try LLM call ────────────────────────────────────────────────────
    llm = _get_llm()
    if llm is not None:
        try:
            system_prompt = _build_system_prompt()
            messages: list[dict[str, str]] = [
                {"role": "system", "content": system_prompt},
            ]

            msgs = state.get("messages", [])
            for i, msg in enumerate(msgs):
                role = "user" if i == 0 or i % 2 == 0 else "assistant"
                if msg.startswith("[start_node]") or msg.startswith("[intent_parse_node]"):
                    continue
                if msg.startswith("[plan_generate_node]") and "preserving" in msg:
                    continue
                messages.append({"role": role, "content": msg})

            if len(messages) <= 1 and msgs:
                task_msg = msgs[-1]
                if not task_msg.startswith("["):
                    messages.append({"role": "user", "content": task_msg})

            response = llm.chat_primary(messages, temperature=0.0)

            if response is not None:
                parsed = _parse_llm_response(response)
                if parsed is not None:
                    code_delta = json.dumps(parsed, ensure_ascii=False)
                    dt = _infer_decision_type(parsed)
                    logger.info(
                        "plan_generate_node LLM response | type={}", dt,
                    )
                    return {
                        "messages": ["[plan_generate_node] LLM decision encoded"],
                        "current_node": "plan_generate",
                        "code_delta": code_delta,
                        "execution_logs": ["plan_generate_node (LLM)"],
                        "retry_count": retry_count,
                    }

            logger.warning("plan_generate_node — LLM returned None or unparseable")
            return {
                "current_node": "plan_generate",
                "code_delta": "error: llm_no_response",
                "retry_count": retry_count + 1,
                "execution_logs": ["plan_generate_node: LLM call failed"],
            }

        except Exception as exc:
            logger.error("plan_generate_node — LLM exception | {}", exc)
            return {
                "current_node": "plan_generate",
                "code_delta": "error: llm_exception",
                "retry_count": retry_count + 1,
                "execution_logs": [f"plan_generate_node: LLM exception: {exc}"],
            }

    # ── Fallback: classic mock behaviour ─────────────────────────────────
    plan: str = (
        f"[plan] generated plan for intent: "
        f"{state.get('code_delta') or 'unknown'}"
    )
    return {
        "messages": ["[plan_generate_node] " + plan],
        "current_node": "plan_generate",
        "code_delta": state.get("code_delta") or "",
        "execution_logs": ["plan_generate_node completed"],
        "retry_count": retry_count,
    }


def human_interrupt_node(state: AgentState) -> dict:
    """Human-in-the-loop via LangGraph native ``interrupt()``.

    - If ``code_delta`` contains ``"error"``: pass through (retry in flight).
    - If ``code_delta`` is a JSON tool call: call ``interrupt()`` to pause
      the graph.  On resume (via ``Command(resume=...)``), the returned
      boolean determines whether to approve the tool.
    - If ``code_delta`` is an answer / question: pass through (no tool to
      approve).
    - Otherwise: pass through (mock / legacy path).

    The graph is paused *at this node* — downstream nodes do not execute
    until ``Command(resume=...)`` is issued.
    """
    cd: str = state.get("code_delta") or ""

    # Error in flight — let retry loop handle it
    if "error" in cd:
        logger.info(
            "human_interrupt_node — error in code_delta, skipping | code_delta={}",
            cd,
        )
        return {"current_node": state.get("current_node") or ""}

    # JSON-encoded decision
    if cd.startswith("{"):
        try:
            parsed: dict[str, Any] = json.loads(cd)
            decision_type = _infer_decision_type(parsed)

            if decision_type == "tool":
                tool_name: str = parsed.get("name", parsed.get("tool", "?"))
                tool_args: dict[str, Any] = parsed.get("args", {})

                logger.info(
                    "human_interrupt_node — tool call detected, "
                    "requesting approval | tool={}",
                    tool_name,
                )

                # Native LangGraph interrupt — pauses graph execution.
                # On resume, returns True (approved) or False (rejected).
                approved: bool = interrupt({
                    "type": "human_approval",
                    "tool": tool_name,
                    "args": tool_args,
                })

                if approved:
                    logger.info(
                        "human_interrupt_node — approved | tool={}", tool_name,
                    )
                    return {
                        "current_node": "human_interrupt",
                        "code_delta": cd,  # preserve tool JSON for tool_execute_node
                        "messages": [
                            f"[human_interrupt] approved: {tool_name}"
                        ],
                        "execution_logs": ["human_interrupt: approved"],
                    }
                else:
                    logger.info(
                        "human_interrupt_node — rejected | tool={}", tool_name,
                    )
                    return {
                        "current_node": "human_interrupt",
                        "code_delta": "error: human_rejected",
                        "messages": [
                            f"[human_interrupt] rejected: {tool_name}"
                        ],
                        "execution_logs": ["human_interrupt: rejected"],
                    }

            elif decision_type in ("answer", "question"):
                logger.info(
                    "human_interrupt_node — {!r} decision, passing through",
                    decision_type,
                )
                return {"current_node": state.get("current_node") or ""}
        except json.JSONDecodeError:
            pass

    # Fallback: no tool to approve — pass through
    logger.info(
        "human_interrupt_node — pass-through | code_delta={:.80s}",
        cd,
    )
    return {"current_node": state.get("current_node") or ""}


def tool_execute_node(state: AgentState) -> dict:
    """Execute a tool call encoded in ``code_delta``.

    Parses ``code_delta`` for a JSON tool instruction, executes the
    named tool via the registry tool table, and appends results to
    ``messages`` and ``execution_logs``.

    Passes through if ``code_delta`` does not contain a tool instruction.
    """
    cd: str = state.get("code_delta") or ""

    if not cd or "error" in cd:
        return {"current_node": state.get("current_node") or ""}

    parsed: dict[str, Any] | None = None
    if cd.startswith("{"):
        try:
            parsed = json.loads(cd)
        except json.JSONDecodeError:
            pass

    if parsed is None:
        return {"current_node": state.get("current_node") or ""}

    decision_type = _infer_decision_type(parsed)
    if decision_type != "tool":
        return {"current_node": state.get("current_node") or ""}

    tool_name: str = parsed.get("name", parsed.get("tool", ""))
    tool_args: dict[str, Any] = parsed.get("args", {})

    if not tool_name:
        logger.warning("tool_execute_node — tool name is empty")
        return {
            "code_delta": "error: empty_tool_name",
            "retry_count": (state.get("retry_count") or 0) + 1,
            "current_node": "tool_execute",
        }

    tool_table = _get_tool_table()
    tool_fn = tool_table.get(tool_name)

    if tool_fn is None:
        output = f"Unknown tool: {tool_name}"
        ok = False
        logger.warning("tool_execute_node — unknown tool | tool={}", tool_name)
    else:
        session_id = state.get("session_id", None) or "default"
        try:
            ok, output = tool_fn(session_id=session_id, **tool_args)
        except Exception as exc:
            ok, output = False, str(exc)
            logger.error(
                "tool_execute_node — tool execution failed | tool={} | error={}",
                tool_name, exc,
            )

    status = "success" if ok else "failed"
    logger.info(
        "tool_execute_node — executed | tool={} | status={} | output_len={}",
        tool_name, status, len(str(output)),
    )

    tool_call_line = json.dumps(
        {"tool": tool_name, "args": tool_args}, ensure_ascii=False,
    )
    result_line = (
        f"Tool '{tool_name}' result ({status}):\n{str(output)[:2000]}"
    )

    return {
        "messages": [tool_call_line, result_line],
        "execution_logs": [f"tool:{tool_name}:{status}:{str(output)[:200]}"],
        "code_delta": json.dumps({
            "type": "tool_result",
            "tool": tool_name,
            "success": ok,
            "output_summary": str(output)[:500],
        }, ensure_ascii=False),
        "current_node": "tool_execute",
    }


def error_detect_node(state: AgentState) -> dict:
    """Inspect ``code_delta`` for error markers and increment retry count.

    Preserves the incoming ``code_delta`` value on error so that
    ``should_continue`` — which runs *after* the node return has been
    applied — still sees the error signal.

    Detects errors in both legacy string format and JSON-encoded
    ``tool_result`` with ``success: false``.
    """
    current_cd: str = state.get("code_delta") or ""
    has_error: bool = "error" in current_cd

    # Check for failed tool_result in JSON
    if current_cd.startswith("{"):
        try:
            parsed = json.loads(current_cd)
            if parsed.get("type") == "tool_result" and not parsed.get("success", True):
                has_error = True
            if parsed.get("type") in ("answer", "question"):
                has_error = False
        except json.JSONDecodeError:
            pass

    if has_error:
        new_retry: int = (state.get("retry_count") or 0) + 1
        logger.warning(
            "Error detected | code_delta={:.120s} | retry_count {}→{}",
            current_cd,
            state.get("retry_count") or 0,
            new_retry,
        )
        return {
            "retry_count": new_retry,
            "current_node": "error_detect",
            "code_delta": current_cd,
            "execution_logs": ["[error_detect] error found, retry_count incremented"],
            "messages": ["[error_detect] error detected"],
        }

    logger.info("No error detected | code_delta={:.120s}", current_cd)
    return {
        "current_node": "error_detect",
        "execution_logs": ["[error_detect] no error"],
        "messages": ["[error_detect] clean"],
    }


def end_node(state: AgentState) -> dict:
    """Final node — marks the pipeline as finished.

    Extracts answer text from JSON-encoded ``code_delta`` if present.
    """
    cd: str = state.get("code_delta") or ""
    extra_messages: list[str] = []

    if cd.startswith("{"):
        try:
            parsed = json.loads(cd)
            dt = _infer_decision_type(parsed)
            if dt == "answer":
                answer_text = parsed.get("text", parsed.get("answer", ""))
                if answer_text:
                    extra_messages.append(f"[end_node] final answer: {answer_text}")
            elif dt == "question":
                question_text = parsed.get("text", parsed.get("question", ""))
                if question_text:
                    extra_messages.append(
                        f"[end_node] clarification needed: {question_text}"
                    )
        except json.JSONDecodeError:
            pass

    return {
        "messages": ["[end_node] pipeline finished"] + extra_messages,
        "current_node": "end",
        "code_delta": state.get("code_delta") or "",
        "execution_logs": ["end_node completed"],
        "retry_count": state.get("retry_count") or 0,
    }


# ── Routers ─────────────────────────────────────────────────────────────────────


def should_continue(state: AgentState) -> str:
    """Conditional router attached to ``error_detect_node``.

    Returns the *name* of the next node to execute::

        "error" in code_delta  AND  retry_count < 3  →  "intent_parse_node"
        code_delta contains answer / question          →  "end_node"
        code_delta contains tool_result (success)      →  "plan_generate_node"
        code_delta contains tool_result (failure)
            AND under retry limit                      →  "intent_parse_node"
        code_delta contains tool_result (failure)
            AND max retries exhausted                  →  "end_node"
        otherwise (mock / non-JSON)                    →  "end_node"
    """
    code_delta: str = state.get("code_delta") or ""
    retry_count: int = state.get("retry_count") or 0

    # ── String error + under limit → retry ───────────────────────────────
    if "error" in code_delta and retry_count < _MAX_RETRIES:
        logger.info(
            "should_continue → intent_parse_node | error with retries left"
        )
        return "intent_parse_node"

    # ── Error + max retries exhausted → terminate ────────────────────────
    if "error" in code_delta and retry_count >= _MAX_RETRIES:
        logger.info(
            "should_continue → end_node | max retries exhausted | "
            "retry_count={}", retry_count,
        )
        return "end_node"

    # ── JSON-encoded decision ────────────────────────────────────────────
    if code_delta.startswith("{"):
        try:
            parsed: dict[str, Any] = json.loads(code_delta)
            dt = _infer_decision_type(parsed)

            if dt == "tool_result":
                if not parsed.get("success", True) and retry_count < _MAX_RETRIES:
                    logger.info(
                        "should_continue → intent_parse_node | "
                        "failed tool_result, retrying"
                    )
                    return "intent_parse_node"
                elif not parsed.get("success", True):
                    logger.info(
                        "should_continue → end_node | "
                        "failed tool_result, max retries exhausted"
                    )
                    return "end_node"
                else:
                    logger.info(
                        "should_continue → plan_generate_node | "
                        "tool_result (success), looping for next step"
                    )
                    return "plan_generate_node"

            if dt in ("answer", "question"):
                logger.info(
                    "should_continue → end_node | terminal type={}", dt,
                )
                return "end_node"

            logger.info(
                "should_continue → end_node | type={}",
                dt or "(no type, no known keys)",
            )
            return "end_node"
        except json.JSONDecodeError:
            pass

    # ── Default: non-JSON, non-error → terminate ─────────────────────────
    logger.info("should_continue → end_node | non-JSON, no error — terminating")
    return "end_node"
