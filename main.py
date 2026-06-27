# main.py — Enterprise AI Agent Platform
"""
串联 A / B / C 三平面全部模块，让 Agent 完成小任务。

用法:
    python main.py "列出 src/ 下的 Python 文件"          # 自动执行（Graph 状态机）
    python main.py --interactive "任务描述"               # 交互式 HITL（终端暂停等 y/n）
    python main.py --time-travel                          # 时间旅行回滚
    python main.py --redlock                              # Redlock 分布式锁
    python main.py --checkpoint                           # 手动 Checkpoint
    python main.py --all                                  # 全部 A 侧验证
    python main.py                                        # REPL 交互模式（SimpleAgent）
"""

import argparse
import os
import sys
import re
import json
import uuid
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import readline  # noqa: F401
except ImportError:
    pass

_project_root = Path(__file__).resolve().parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def _load_dotenv() -> None:
    env_path = _project_root / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if key not in os.environ:
                os.environ[key] = value


_load_dotenv()

from loguru import logger
from langsmith import traceable
from langgraph.types import Command

# ── 全部通过 __init__.py 公共门面导入 ──────────────────────────────────────────

from src.common.registry import ServiceRegistry, registry

from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB_SECURITY

from src.control_plane import (
    AgentState,
    StateMachineEngine,
    get_app,
    _empty_state,
    DistributedCheckpointManager,
    run_with_checkpoint,
    time_travel_resume,
)

from src.data_plane import (
    LLMRouter,
    DataHealer,
    SchemaEngine,
    DataValidator,
    DataPlaneMCPClient,
    MCPGateway,
)

from src.security_guard import (
    DualLayerMemoryGovernor,
    SlidingWindowRateLimiter,
    PathHashLoopDetector,
)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ServiceRegistry — 注册全部依赖
# ═══════════════════════════════════════════════════════════════════════════════

def _register_all_services() -> None:
    """向全局 registry 注册 A/B/C 全部服务。"""
    _register_core_graph_services()
    _register_bc_services()


def _register_core_graph_services() -> None:
    """注册 Graph 模式核心服务：checkpoint + LLM + 工具表 + engine。"""
    registry.register(
        "checkpoint_manager", DistributedCheckpointManager, singleton=True,
    )

    def _make_llm() -> Optional[LLMRouter]:
        if not os.getenv("PLATFORM_PRIMARY_LLM_API_KEY"):
            logger.warning("LLMRouter 跳过 — 无 API Key, 节点将走 mock")
            return None
        router = LLMRouter()
        logger.info("LLMRouter 就绪 — {}", router.primary_model_name)
        return router

    registry.register("llm_router", _make_llm, singleton=True)
    registry.register("data_healer", DataHealer, singleton=True)
    registry.register("tool_table", lambda: dict(_TOOL_TABLE), singleton=True)
    logger.info("tool_table 已注册 ({} 个工具)", len(_TOOL_TABLE))

    def _make_engine() -> StateMachineEngine:
        mgr = registry.get(
            "checkpoint_manager", expected_type=DistributedCheckpointManager,
        )
        engine = StateMachineEngine(checkpoint_manager=mgr)
        logger.info("StateMachineEngine 就绪")
        return engine

    registry.register("state_machine_engine", _make_engine, singleton=True)


def _register_bc_services() -> None:
    """注册 B/C 侧服务：Schema 校验、MCP、记忆、限流、循环检测。"""
    registry.register("schema_engine", SchemaEngine, singleton=True)
    registry.register("validator", DataValidator, singleton=True)

    # B: MCP 客户端 + 网关
    registry.register("mcp_client", DataPlaneMCPClient, singleton=True)
    registry.register("mcp_gateway", MCPGateway, singleton=True)

    registry.register(
        "memory_governor",
        lambda: DualLayerMemoryGovernor(max_window_size=3),
        singleton=True,
    )

    # C: SlidingWindowRateLimiter (依赖 Redis)
    def _make_rate_limiter() -> Optional[SlidingWindowRateLimiter]:
        try:
            import redis as _redis
            cli = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
                db=REDIS_DB_SECURITY, socket_connect_timeout=2,
                decode_responses=True,
            )
            cli.ping()
            limiter = SlidingWindowRateLimiter(cli, window_seconds=180, max_calls=15)
            logger.info("SlidingWindowRateLimiter 就绪")
            return limiter
        except Exception as exc:
            logger.warning("RateLimiter 跳过 — {}", exc)
            return None

    registry.register("rate_limiter", _make_rate_limiter, singleton=True)

    # C: PathHashLoopDetector (依赖 Redis)
    def _make_loop_detector() -> Optional[PathHashLoopDetector]:
        try:
            import redis as _redis
            cli = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
                db=REDIS_DB_SECURITY, socket_connect_timeout=2,
                decode_responses=True,
            )
            cli.ping()
            detector = PathHashLoopDetector(cli)
            logger.info("PathHashLoopDetector 就绪")
            return detector
        except Exception as exc:
            logger.warning("LoopDetector 跳过 — {}", exc)
            return None

    registry.register("loop_detector", _make_loop_detector, singleton=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. 工具（裸函数，被 @audit_tool_call AOP 切面包裹）
# ═══════════════════════════════════════════════════════════════════════════════

_SHELL_TIMEOUT = 30
_workspace = _project_root


def _do_shell(*, session_id: str = "", command: str = "") -> Tuple[bool, str]:
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=_SHELL_TIMEOUT, cwd=str(_workspace),
        )
        out = result.stdout.strip() or result.stderr.strip() or "(no output)"
        return result.returncode == 0, out[:2000]
    except subprocess.TimeoutExpired:
        return False, f"Timeout ({_SHELL_TIMEOUT}s)"
    except Exception as exc:
        return False, f"Shell error: {exc}"


def _do_read_file(*, session_id: str = "", path: str = "") -> Tuple[bool, str]:
    target = _workspace / path
    if not target.exists():
        return False, f"File not found: {path}"
    if target.is_dir():
        return False, f"Is a directory: {path}"
    try:
        return True, target.read_text(encoding="utf-8")[:3000]
    except UnicodeDecodeError:
        try:
            return True, target.read_text(encoding="gbk")[:3000]
        except Exception as exc:
            return False, f"Encoding error: {exc}"
    except Exception as exc:
        return False, f"Read error: {exc}"


def _do_write_file(*, session_id: str = "", path: str = "", content: str = "") -> Tuple[bool, str]:
    target = _workspace / path
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return True, f"Written {len(content)} chars to {path}"
    except Exception as exc:
        return False, f"Write error: {exc}"


def _do_list_dir(*, session_id: str = "", path: str = ".") -> Tuple[bool, str]:
    target = _workspace / path
    if not target.exists():
        return False, f"Not found: {path}"
    if not target.is_dir():
        return False, f"Not a directory: {path}"
    try:
        entries = []
        for entry in sorted(target.iterdir()):
            tag = "[D]" if entry.is_dir() else "[F]"
            entries.append(f"  {tag} {entry.name}")
        return True, "\n".join(entries[:60]) if entries else "(empty)"
    except Exception as exc:
        return False, f"List error: {exc}"


def _do_grep(*, session_id: str = "", pattern: str = "", path: str = ".") -> Tuple[bool, str]:
    target = _workspace / path
    if not target.exists():
        return False, f"Not found: {path}"
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        return False, f"Invalid regex: {exc}"

    results: List[str] = []
    files = list(target.rglob("*")) if target.is_dir() else [target]
    for fp in files:
        if not fp.is_file() or fp.suffix in {".pyc", ".pyo", ".exe", ".dll", ".pyd"}:
            continue
        try:
            for lineno, line in enumerate(
                fp.read_text(encoding="utf-8", errors="replace").splitlines(), 1
            ):
                if compiled.search(line):
                    rel = fp.relative_to(_workspace)
                    results.append(f"{rel}:{lineno}: {line.strip()[:120]}")
                    if len(results) >= 50:
                        break
        except Exception:
            continue
        if len(results) >= 50:
            break
    return True, "\n".join(results) if results else f"No matches for '{pattern}'"


def _do_fetch_web(*, session_id: str = "", url: str = "") -> Tuple[bool, str]:
    """B: DataPlaneMCPClient → web_mcp_server → 抓取网页 + 清洗 HTML。"""
    try:
        client: DataPlaneMCPClient = registry.get("mcp_client", expected_type=DataPlaneMCPClient)
    except Exception:
        return False, "MCP client 未就绪"
    try:
        text = client.fetch_and_clean_web(url)
        return True, text
    except Exception as exc:
        return False, f"Fetch error: {exc}"


# ── 工具表 ────────────────────────────────────────────────────────────────────

_TOOL_TABLE: Dict[str, Any] = {
    "shell":      _do_shell,
    "read_file":  _do_read_file,
    "write_file": _do_write_file,
    "list_dir":   _do_list_dir,
    "grep":       _do_grep,
    "fetch_web":  _do_fetch_web,
}

_TOOL_DEFS = """## shell
执行 shell 命令。Args: {"command": "<string>"}

## read_file
读取文件内容。Args: {"path": "<relative_path>"}

## write_file
写入文件。Args: {"path": "<relative_path>", "content": "<string>"}

## list_dir
列出目录内容。Args: {"path": "<relative_path>"}

## grep
搜索文件内容（正则）。Args: {"pattern": "<regex>", "path": "<relative_path>"}

## fetch_web
抓取网页并清洗 HTML，返回纯文本。Args: {"url": "<full_url>"}"""


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SimpleAgent — 串联所有模块
# ═══════════════════════════════════════════════════════════════════════════════

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


class SimpleAgent:
    """Thin wrapper around the Graph state machine for REPL / single-task use.

    All LLM calling, tool execution, error handling, and HITL logic lives
    inside the Graph nodes.  SimpleAgent just constructs the initial state,
    invokes the compiled graph, and extracts the answer.
    """

    def __init__(self, max_steps: int = 10, verbose: bool = False) -> None:
        self._max_steps = max_steps
        self.verbose = verbose

    # ── 主循环（基于 Graph 状态机）────────────────────────────────────
    @traceable(name="Agent_Task_Execution", run_type="chain")
    def run(self, task: str, session_id: Optional[str] = None) -> str:
        """Execute a task via the Graph state machine.

        Constructs the initial state, invokes the compiled graph, and
        auto-resumes any HITL interrupts (non-interactive mode).
        """
        if session_id is None:
            session_id = str(uuid.uuid4())[:8]

        self._log(f"\n{'='*60}")
        self._log(f"Session: {session_id}  |  Task: {task}")
        self._log(f"{'='*60}")

        initial_state = _empty_state()
        initial_state["messages"] = [task]
        config: dict[str, Any] = {"configurable": {"thread_id": session_id}}

        graph = get_app()

        # Invoke graph — runs until interrupt() or completion
        result: AgentState = graph.invoke(initial_state, config)

        # Auto-resume any HITL pauses with approval
        step = 1
        while True:
            state_snapshot = graph.get_state(config)
            # LangGraph: interrupt() pauses by setting snapshot.next, not .interrupted
            if not state_snapshot or not state_snapshot.next:
                break

            step += 1
            self._log(f"  [Step {step}] auto-resuming HITL pause (approval=True)")
            result = graph.invoke(Command(resume=True), config)

        # Extract final answer
        final_answer = self._extract_answer(result)

        self._log(f"\n{'='*60}")
        self._log(f"结果: {final_answer[:500]}")
        self._log(f"{'='*60}\n")

        return final_answer

    @staticmethod
    def _extract_answer(result: AgentState) -> str:
        """Extract the final answer from a completed AgentState."""
        cd: str = result.get("code_delta") or ""

        if cd.startswith("{"):
            try:
                parsed = json.loads(cd)
                if "answer" in parsed:
                    return str(parsed["answer"])
                if "text" in parsed:
                    return str(parsed["text"])
            except json.JSONDecodeError:
                pass

        msgs = result.get("messages", [])
        for msg in reversed(msgs):
            if isinstance(msg, str) and not msg.startswith("["):
                return msg

        return f"Graph completed (current_node={result.get('current_node', '?')})"

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Graph 状态机模式
# ═══════════════════════════════════════════════════════════════════════════════

def _print_banner(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def _print_status() -> None:
    llm_ok = os.getenv("PLATFORM_PRIMARY_LLM_API_KEY") is not None
    redis_ok = False
    try:
        import redis as _redis
        cli = _redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
            socket_connect_timeout=1,
        )
        cli.ping()
        redis_ok = True
    except Exception:
        pass
    ckpt_ok = False
    try:
        registry.get("checkpoint_manager")
        ckpt_ok = True
    except Exception:
        pass
    healer_ok = False
    try:
        registry.get("data_healer")
        healer_ok = True
    except Exception:
        pass
    memory_ok = False
    try:
        registry.get("memory_governor")
        memory_ok = True
    except Exception:
        pass
    mcp_ok = False
    try:
        registry.get("mcp_client", expected_type=DataPlaneMCPClient)
        mcp_ok = True
    except Exception:
        pass
    print(f"  LLM:         {'✓' if llm_ok else '✗'}")
    print(f"  Redis:       {'✓' if redis_ok else '✗'}")
    print(f"  Checkpoint:  {'✓' if ckpt_ok else '✗'}")
    print(f"  Healer:      {'✓' if healer_ok else '✗'}")
    print(f"  Memory:      {'✓' if memory_ok else '✗'}")
    print(f"  MCP:         {'✓' if mcp_ok else '✗'}")


def run_graph_mode(
    task: str,
    interactive: bool = False,
    verbose: bool = False,
    max_steps: int = 10,
) -> None:
    """Graph 状态机 + HITL 条件审批（原生 interrupt / Command(resume=...)）。

    Args:
        task: 用户任务描述
        interactive: True 时遇到 human_interrupt 暂停等 y/n；
                     False 时自动 approval=True
        verbose: 打印详细消息
        max_steps: 最大步数（预留给未来扩展）
    """
    _print_banner(
        "交互式 HITL 模式（终端等待 y/n 审批）" if interactive
        else "Graph 状态机模式"
    )

    initial_state = _empty_state()
    initial_state["messages"] = [task]
    session_id = str(uuid.uuid4())
    print(f"  Task:    {task}")
    print(f"  Session: {session_id}")
    if verbose:
        print(f"  max_steps: {max_steps}")

    graph = get_app()
    config: dict[str, Any] = {"configurable": {"thread_id": session_id}}

    # 首次 invoke — runs until interrupt() or completion
    result: AgentState = graph.invoke(initial_state, config)
    step = 0

    while True:
        step += 1
        print(f"\n{'─' * 60}")
        print(f"  [Step {step}]")
        print(f"  current_node : {result.get('current_node', '?')}")
        print(f"  code_delta   : {result.get('code_delta', '')}")
        print(f"  retry_count  : {result.get('retry_count', 0)}")
        print(f"  messages     : {len(result.get('messages', []))} 条")

        if verbose:
            for i, msg in enumerate(result.get("messages", [])):
                print(f"    [{i}] {str(msg)[:200]}")

        # Check if graph paused at human_interrupt_node
        state_snapshot = graph.get_state(config)
        is_paused = (
            state_snapshot is not None
            and state_snapshot.next
            and "human_interrupt_node" in state_snapshot.next
        )
        if not is_paused:
            print(f"\n  ✅ 流程完成（current_node={result.get('current_node')}）")
            break

        # ── HITL 审批 ──────────────────────────────────────────────────
        print(f"\n  ⚠️  Graph paused — 需要人工审批")
        interrupt_val = getattr(state_snapshot, "interrupt_value", None)
        if interrupt_val and isinstance(interrupt_val, dict):
            tool_name = interrupt_val.get("tool", "?")
            print(f"  📋 工具: {tool_name}")

        if interactive:
            while True:
                try:
                    choice = input("\n  审批通过？[y/n]: ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\n  ⚠️  用户中断输入，默认视为拒绝。")
                    choice = "n"
                    break
                if choice in ("y", "yes"):
                    approval = True
                    break
                if choice in ("n", "no"):
                    approval = False
                    break
                print("  ❌ 无效输入，请输入 y 或 n")
            action = "审批通过 ✓" if approval else "审批拒绝 ✗"
            print(f"\n  [resume] {action}")
        else:
            print(f"\n  [resume] 自动审批通过")
            approval = True

        result = graph.invoke(Command(resume=approval), config)

    # ── 最终状态 ───────────────────────────────────────────────────────
    print(f"\n  最终状态:")
    print(f"  current_node : {result.get('current_node', '?')}")
    print(f"  code_delta   : {result.get('code_delta', '')}")
    print(f"  retry_count  : {result.get('retry_count', 0)}")
    print(f"  messages     : {len(result.get('messages', []))} 条")

    if "error" in (result.get("code_delta") or ""):
        print(f"  ⚠️  流程结束但 code_delta 含 error 标记。")

    if verbose:
        for i, msg in enumerate(result.get("messages", [])):
            print(f"    [{i}] {str(msg)[:200]}")


def run_time_travel_mode(verbose: bool = False) -> None:
    """时间旅行回滚 + time_travel_resume 验证."""
    _print_banner("时间旅行回滚模式")

    mgr = DistributedCheckpointManager()
    session_id = f"tt-demo-{uuid.uuid4().hex[:6]}"
    print(f"  Session: {session_id}")

    for i in range(1, 4):
        st: AgentState = _empty_state()
        st["messages"] = [f"[snapshot-{i}]"]
        st["current_node"] = f"node_{i}"
        st["code_delta"] = f"delta_{i}"
        st["retry_count"] = i
        key = mgr.save(session_id, st)
        cid = key[len(f"legacy:{session_id}:"):]
        print(f"  [{i}] saved → checkpoint_id={cid} | current_node={st['current_node']}")

    all_keys = mgr._client.keys(f"legacy:{session_id}:*")
    sorted_keys = sorted(all_keys)
    target_cid = sorted_keys[1].decode() if isinstance(sorted_keys[1], bytes) else sorted_keys[1]
    target_cid = target_cid[len(f"legacy:{session_id}:"):]
    print(f"\n  rollback → checkpoint_id={target_cid}")
    historical = mgr.rollback(session_id, target_cid)
    if historical is not None:
        print(f"  ✅ rollback 成功: current_node={historical['current_node']}, code_delta={historical['code_delta']}")
    else:
        print(f"  ❌ rollback 返回 None")
        return

    print(f"\n  time_travel_resume(session_id={session_id!r}, checkpoint_id={target_cid!r})")
    try:
        tt_result = time_travel_resume(session_id, target_cid, manager=mgr)
        print(f"  ✅ 时间旅行完成: current_node={tt_result.get('current_node', '?')}, "
              f"code_delta={tt_result.get('code_delta', '')}")
        print(f"  messages: {len(tt_result.get('messages', []))} 条")
        if verbose:
            for i, msg in enumerate(tt_result.get("messages", [])):
                print(f"    [{i}] {str(msg)[:200]}")
    except ValueError as exc:
        print(f"  ❌ time_travel_resume 失败: {exc}")


def run_redlock_mode() -> None:
    """Redlock 分布式锁 + save_with_lock 验证."""
    _print_banner("Redlock 分布式锁模式")

    mgr = DistributedCheckpointManager()
    session_id = f"redlock-demo-{uuid.uuid4().hex[:6]}"
    print(f"  Session: {session_id}")

    st: AgentState = _empty_state()
    st["messages"] = ["redlock-test"]
    st["current_node"] = "redlock_demo"
    st["code_delta"] = "locked_write"

    print(f"\n  调用 save_with_lock({session_id!r}, state)")
    ok = mgr.save_with_lock(session_id, st)
    print(f"  ✅ save_with_lock → {ok}")
    assert ok, "save_with_lock must succeed under no contention"

    loaded = mgr.load_latest(session_id)
    if loaded:
        print(f"  ✅ load_latest: current_node={loaded['current_node']}, code_delta={loaded['code_delta']}")
    else:
        print(f"  ❌ load_latest 返回 None")

    print(f"\n  模拟竞争：手动占锁")
    lock_key = f"redlock:checkpoint:{session_id}"
    mgr._client.set(lock_key, "fake-owner", px=10000, nx=True)
    contended = mgr.save_with_lock(session_id, st)
    mgr._client.delete(lock_key)
    if contended:
        print(f"  ⚠️ 竞争下 save_with_lock 返回 True（预期 False）")
    else:
        print(f"  ✅ 竞争下 save_with_lock 返回 False（正确拒绝）")


def run_checkpoint_mode(verbose: bool = False) -> None:
    """直接调用 run_with_checkpoint（不走 StateMachineEngine 封装）."""
    _print_banner("手动 Checkpoint 模式")

    session_id = f"cp-demo-{uuid.uuid4().hex[:6]}"
    print(f"  Session: {session_id}")

    mgr = DistributedCheckpointManager()
    mid: AgentState = _empty_state()
    mid["messages"] = ["[pre-seeded]"]
    mid["current_node"] = "intent_parse"
    mid["code_delta"] = "error_v1"
    mid["retry_count"] = 1
    key = mgr.save(session_id, mid)
    print(f"  预埋 checkpoint: {key}")

    initial = _empty_state()
    result = run_with_checkpoint(session_id, initial, manager=mgr)
    print(f"  current_node : {result.get('current_node', '?')}")
    print(f"  code_delta   : {result.get('code_delta', '')}")
    print(f"  retry_count  : {result.get('retry_count', 0)}")
    print(f"  messages     : {len(result.get('messages', []))} 条")
    has_pre_seeded = any("[pre-seeded]" in m for m in result.get("messages", []))
    print(f"  {'✅' if has_pre_seeded else '❌'} 预埋消息保留")
    if verbose and has_pre_seeded:
        for i, msg in enumerate(result.get("messages", [])):
            print(f"    [{i}] {str(msg)[:200]}")


def run_all_demos(task: str, verbose: bool = False) -> None:
    """顺序执行所有 A 侧接口验证."""
    run_graph_mode(task, verbose=verbose)
    run_time_travel_mode(verbose=verbose)
    run_redlock_mode()
    run_checkpoint_mode(verbose=verbose)
    _print_banner("全部 A 侧接口验证完成")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. CLI — 入口编排层
# ═══════════════════════════════════════════════════════════════════════════════

# ── 模式路由表（命令行标志 → 执行函数）─────────────────────────────────────

_MODE_REGISTRY: dict[str, Callable] = {
    "time_travel": lambda args: run_time_travel_mode(verbose=args.verbose),
    "redlock":     lambda args: run_redlock_mode(),
    "checkpoint":  lambda args: run_checkpoint_mode(verbose=args.verbose),
    "all":         lambda args: run_all_demos(
        args.task or "分析项目代码结构并生成执行计划", verbose=args.verbose,
    ),
}

# ── 基础设施验证标志（这些模式下只注册最小服务集）─────────────────────────

_INFRA_FLAGS = {"time_travel", "redlock", "checkpoint"}


def _parse_cli() -> argparse.Namespace:
    """解析命令行参数，返回 Namespace 对象。"""
    parser = argparse.ArgumentParser(
        description="Enterprise AI Agent Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='示例:\n  python main.py "列出 src/ 下的 .py 文件"\n'
               "  python main.py --interactive '列出...'\n"
               "  python main.py",
    )
    parser.add_argument("task", nargs="?", default=None)
    parser.add_argument(
        "--time-travel", action="store_true", default=False,
        help="时间旅行回滚 + time_travel_resume 验证",
    )
    parser.add_argument(
        "--redlock", action="store_true", default=False,
        help="Redlock 分布式锁 + save_with_lock 验证",
    )
    parser.add_argument(
        "--checkpoint", action="store_true", default=False,
        help="直接调用 run_with_checkpoint 验证",
    )
    parser.add_argument(
        "--interactive", action="store_true", default=False,
        help="交互式审批 — 遇到 human_interrupt 时暂停等待用户 y/n",
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="顺序执行全部 A 侧接口验证",
    )
    parser.add_argument("--verbose", action="store_true", default=False)

    return parser.parse_args()


def _bootstrap_services(args: argparse.Namespace) -> None:
    """按需注册服务。

    - 基础设施验证模式（--time-travel / --redlock / --checkpoint）: 仅 checkpoint。
    - ``--all`` / 有 task / REPL: 全量服务（Graph + B/C）。
    """
    activated = {flag for flag in _INFRA_FLAGS if getattr(args, flag, False)}

    if activated and not args.all:
        # 基础设施验证模式 — 最小注册
        logger.info("基础设施验证模式 {} — 仅注册 checkpoint", sorted(activated))
        registry.register(
            "checkpoint_manager", DistributedCheckpointManager, singleton=True,
        )

        def _make_engine() -> StateMachineEngine:
            mgr = registry.get(
                "checkpoint_manager", expected_type=DistributedCheckpointManager,
            )
            engine = StateMachineEngine(checkpoint_manager=mgr)
            logger.info("StateMachineEngine 就绪")
            return engine

        registry.register("state_machine_engine", _make_engine, singleton=True)
    else:
        # 全量注册：Graph + B/C 全部服务
        logger.info("全量服务注册 — Graph + B/C")
        _register_all_services()


def _active_mode(args: argparse.Namespace) -> str | None:
    """返回当前激活的验证模式名，无匹配时返回 None。"""
    for flag in _MODE_REGISTRY:
        if getattr(args, flag, False):
            return flag
    return None


def _dispatch_mode(args: argparse.Namespace) -> None:
    """根据命令行参数分发到对应模式。

    优先匹配验证模式（--time-travel / --redlock / --checkpoint / --all），
    无匹配时默认走 Graph 状态机。
    """
    mode = _active_mode(args)

    if mode is not None:
        handler = _MODE_REGISTRY[mode]
        handler(args)
        return

    # 默认：所有任务走 Graph 状态机
    run_graph_mode(
        args.task or "分析项目代码结构并生成执行计划",
        interactive=args.interactive,
        verbose=args.verbose,
    )


def _run_interactive(agent: SimpleAgent) -> None:
    """交互 REPL 循环 — 单任务异常不退出。"""
    _print_banner("Enterprise AI Agent Platform — 交互模式")
    _print_status()
    print("\n  输入任务（quit 退出）\n")

    while True:
        try:
            user_input = input("🧠 > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  退出。")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            print("  退出。")
            break

        try:
            agent.run(user_input)
        except Exception as exc:
            logger.error("Agent 运行异常: {}", exc)
            print(f"  ⚠️ 任务失败: {exc}\n")


def main() -> int:
    """平台入口 — 解析参数 → 注册服务 → 自动路由。

    自动路由规则：
        - ``--time-travel`` / ``--redlock`` / ``--checkpoint`` / ``--all`` → 验证模式
        - 有 task 参数 → Graph 状态机
        - 无 task 参数且无验证 flag → REPL 交互模式（SimpleAgent）

    Returns:
        0 正常退出，1 致命异常。
    """
    args = _parse_cli()

    # 判断是否有验证模式被激活
    has_mode_flag = _active_mode(args) is not None

    try:
        # 无验证模式 + 无任务 + 非交互 → REPL 模式
        # （--interactive 单独出现意味用户想交互审批 + 默认任务，不是 REPL）
        if args.task is None and not has_mode_flag and not args.interactive:
            # 全量注册 B/C 服务供 SimpleAgent 使用
            logger.info("REPL 模式 — 全量服务注册")
            _register_all_services()

            agent = SimpleAgent(verbose=True)
            _run_interactive(agent)
            return 0

        _bootstrap_services(args)
        _dispatch_mode(args)
        return 0
    except Exception as exc:
        logger.error("Fatal: {}", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
