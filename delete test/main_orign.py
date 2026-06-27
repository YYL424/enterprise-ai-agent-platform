# main.py — Enterprise AI Agent Platform
"""
串联 A / B / C 三平面全部模块，让 Agent 完成小任务。

用法:
    python main.py "列出 src/ 下的 Python 文件"    # 单任务
    python main.py --graph                          # Graph + 完整 HITL
    python main.py --time-travel                    # 时间旅行回滚
    python main.py --redlock                        # Redlock 分布式锁
    python main.py --checkpoint                     # 手动 Checkpoint
    python main.py --all                            # 全部 A 侧验证
    python main.py                                  # 交互模式
"""

import argparse
import os
import sys
import re
import json
import time
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

# ── 全部通过 __init__.py 公共门面导入 ──────────────────────────────────────────

from src.common.registry import ServiceRegistry, registry
from src.common.interfaces import DomainSchema

from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB_SECURITY

from src.control_plane import (
    AgentState,
    CheckpointRedlock,
    StateMachineEngine,
    build_graph,
    get_app,
    _empty_state,
    DistributedCheckpointManager,
    resume_after_human,
    run_with_checkpoint,
    time_travel_resume,
)

from src.data_plane import (
    LLMRouter,
    LLMCallError,
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
    audit_tool_call,
    SecurityInterceptionException,
)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ServiceRegistry — 注册全部依赖
# ═══════════════════════════════════════════════════════════════════════════════

def _register_all_services() -> None:
    """向全局 registry 注册 A/B/C 全部服务。"""

    def _make_llm() -> Optional[LLMRouter]:
        if not os.getenv("PLATFORM_PRIMARY_LLM_API_KEY"):
            logger.warning("LLMRouter 跳过")
            return None
        router = LLMRouter()
        logger.info("LLMRouter 就绪 — {}", router.primary_model_name)
        return router

    registry.register("llm_router", _make_llm, singleton=True)
    registry.register("schema_engine", SchemaEngine, singleton=True)
    registry.register("data_healer", DataHealer, singleton=True)
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

    # A: Checkpoint
    registry.register("checkpoint_manager", DistributedCheckpointManager, singleton=True)

    # A: StateMachineEngine
    def _make_engine() -> StateMachineEngine:
        mgr = registry.get("checkpoint_manager", expected_type=DistributedCheckpointManager)
        engine = StateMachineEngine(checkpoint_manager=mgr)
        logger.info("StateMachineEngine 就绪")
        return engine

    registry.register("state_machine_engine", _make_engine, singleton=True)


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

    def __init__(self, max_steps: int = 10, verbose: bool = False) -> None:
        self._max_steps = max_steps
        self.verbose = verbose

        # 全部依赖从 ServiceRegistry 解析
        self._llm: Optional[LLMRouter] = self._get("llm_router")
        self._healer: DataHealer = self._get("data_healer", DataHealer)
        self._schema_engine: Optional[SchemaEngine] = self._get("schema_engine")
        self._validator: DataValidator = self._get("validator", DataValidator)
        self._memory: DualLayerMemoryGovernor = self._get(
            "memory_governor", DualLayerMemoryGovernor,
            lambda: DualLayerMemoryGovernor(max_window_size=3),
        )
        self._checkpoint: Optional[DistributedCheckpointManager] = self._get("checkpoint_manager")
        self._engine: Optional[StateMachineEngine] = self._get("state_machine_engine")

        # C: 用 @audit_tool_call 切面包裹每个工具
        rate_limiter = self._get("rate_limiter")
        loop_detector = self._get("loop_detector")
        self._audited_tools: Dict[str, Any] = {}
        for name, fn in _TOOL_TABLE.items():
            try:
                self._audited_tools[name] = audit_tool_call(
                    rate_limiter=rate_limiter,
                    path_analyzer=loop_detector,
                )(fn)
            except Exception:
                self._audited_tools[name] = fn

        # C: 记忆状态
        self._active_window: List[Dict[str, Any]] = []
        self._global_summary: List[str] = []
        self._tool_call_count: int = 0

    @staticmethod
    def _get(name: str, typ: type = object, fallback: Any = None) -> Any:
        try:
            return registry.get(name, expected_type=typ)
        except Exception:
            if fallback is not None:
                return fallback() if callable(fallback) else fallback
            return None

    # ── 主循环 ────────────────────────────────────────────────────────────
    @traceable(name="Agent_Task_Execution", run_type="chain")
    def run(self, task: str, session_id: Optional[str] = None) -> str:
        if session_id is None:
            session_id = str(uuid.uuid4())[:8]

        self._log(f"\n{'='*60}")
        self._log(f"Session: {session_id}  |  Task: {task}")
        self._log(f"{'='*60}")

        system_prompt = _SYSTEM_PROMPT.format(tool_defs=_TOOL_DEFS)
        # LLMRouter 双轨均走 OpenAI 兼容协议 → system 角色独立发送
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"## Task\n{task}"},
        ]

        final_answer = ""
        for step in range(1, self._max_steps + 1):
            self._log(f"\n── Step {step}/{self._max_steps} ──")

            # B: LLMRouter.chat_primary()
            response = self._call_llm(messages)
            if response is None:
                final_answer = "LLM 调用失败。"
                break

            # B: DataHealer + SchemaEngine 校验链
            parsed = self._validate_and_parse(response)
            if parsed is None:
                messages.append({"role": "assistant", "content": response})
                messages.append({
                    "role": "user",
                    "content": "Invalid format. JSON only: "
                               '{"tool": "...", "args": {...}} or {"answer": "..."}',
                })
                if step < self._max_steps:
                    self._log("⏳ Step 冷却 10.0s")
                    time.sleep(10.0)
                continue

            if "answer" in parsed:
                final_answer = parsed["answer"]
                # 长内容自动写入文件，避免终端截断
                if len(final_answer) > 800:
                    output_dir = _project_root / "output"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    output_path = output_dir / f"{session_id}_answer.md"
                    try:
                        output_path.write_text(final_answer, encoding="utf-8")
                        self._log(f"📄 完整内容已保存至 {output_path}")
                        final_answer = (
                            f"[完整内容已保存至 {output_path}]\n\n"
                            f"{final_answer[:300]}..."
                        )
                    except Exception as exc:
                        self._log(f"⚠️ 写入文件失败: {exc}")
                self._log(f"✅ 完成: {final_answer[:200]}")
                break

            if "question" in parsed:
                final_answer = f"[需要澄清] {parsed['question']}"
                self._log(f"❓ {final_answer}")
                break

            if "tool" in parsed:
                self._tool_call_count += 1
                if self._tool_call_count > 6:
                    final_answer = "工具调用次数过多（>6），任务中止以防止循环。"
                    self._log(f"⚠️ {final_answer}")
                    break

                tool_name = parsed["tool"]
                tool_args = parsed.get("args", {})
                self._log(f"🔧 {tool_name}({json.dumps(tool_args, ensure_ascii=False)})")

                # C: @audit_tool_call 切面 → SlidingWindowRateLimiter + PathHashLoopDetector
                #     前置安检 → 执行工具 → 后置审计（全在装饰器内完成）
                tool_fn = self._audited_tools.get(tool_name)
                if tool_fn is None:
                    output = f"Unknown tool: {tool_name}"
                    ok = False
                else:
                    try:
                        ok, output = tool_fn(
                            session_id=session_id, **tool_args,
                        )
                    except SecurityInterceptionException as exc:
                        self._log(f"🛡️ 安全拦截: {exc}")
                        final_answer = (
                            f"🛡️ 任务被安全系统暂停。原因：{exc}。\n"
                            f"建议：简化任务描述，或尝试不涉及网页抓取的查询方式。"
                        )
                        break

                status = "✓" if ok else "✗"
                self._log(f"  {status} {str(output)[:300]}")

                # C: DualLayerMemoryGovernor — 记忆管理
                turn_record: Dict[str, Any] = {
                    "tool_name": tool_name,
                    "args": tool_args,
                    "output": str(output)[:500],
                    "error": None if ok else str(output)[:200],
                }
                try:
                    self._active_window = self._memory.process_memory_tick(
                        self._active_window, self._global_summary, turn_record,
                    )
                except Exception:
                    pass

                messages.append({
                    "role": "assistant",
                    "content": json.dumps(parsed, ensure_ascii=False),
                })
                messages.append({
                    "role": "user",
                    "content": f"Tool '{tool_name}' result "
                               f"({'success' if ok else 'failed'}):\n{output}",
                })
                # fall through to cooldown

            # ── 统一 Step 间冷却 ──────────────────────────────────────
            if step < self._max_steps:
                cooldown: float = 10.0
                self._log(f"⏳ Step 冷却 {cooldown:.1f}s")
                time.sleep(cooldown)

        else:
            final_answer = f"超过最大步数 ({self._max_steps})。"

        self._log(f"\n{'='*60}")
        self._log(f"结果: {final_answer[:500]}")
        self._log(f"{'='*60}\n")

        # A: DistributedCheckpointManager — 持久化会话
        if self._checkpoint is not None:
            try:
                state: AgentState = _empty_state()
                state["messages"] = [task, final_answer]
                state["code_delta"] = "agent_completed"
                ckpt_id = self._checkpoint.save(session_id, state)
                self._log(f"💾 Checkpoint: {ckpt_id}")
            except Exception as exc:
                self._log(f"⚠️ Checkpoint: {exc}")

        return final_answer

    # ── B: LLM ────────────────────────────────────────────────────────────

    def _call_llm(self, messages: List[Dict[str, str]]) -> Optional[str]:
        if self._llm is None:
            self._log("⚠️ LLM 未配置")
            return None
        try:
            return self._llm.chat_primary(messages, temperature=0.0)
        except LLMCallError as exc:
            self._log(f"❌ LLMRouter: {exc}")
            return None

    # ── B: 校验链 (DataHealer + SchemaEngine + DataValidator) ─────────────

    def _validate_and_parse(self, raw: str) -> Optional[Dict[str, Any]]:
        cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip()
        cleaned = re.sub(r"```\s*$", "", cleaned).strip()

        parsed = None
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            repaired = self._healer.heal_truncated_json(cleaned)
            if repaired is not None:
                try:
                    parsed = json.loads(repaired)
                    self._log("🔧 DataHealer 修复成功")
                except json.JSONDecodeError:
                    pass

        if parsed is None:
            self._log(f"⚠️ 无法解析: {raw[:200]}...")
            return None

        # SchemaEngine + DataValidator: 用合约校验载荷
        if self._schema_engine is not None and isinstance(parsed, dict):
            contracts_dir = _project_root / "contracts" / "domains"
            if contracts_dir.is_dir():
                for sf in contracts_dir.glob("*.json"):
                    try:
                        schema: DomainSchema = self._schema_engine.load_schema(sf.stem)
                        self._validator.audit_and_execute(
                            parsed, self._schema_engine, schema,
                        )
                    except Exception:
                        pass

        return parsed

    # ── 日志 ──────────────────────────────────────────────────────────────

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
    mcp_ok = False
    try:
        registry.get("mcp_client", expected_type=DataPlaneMCPClient)
        mcp_ok = True
    except Exception:
        pass
    print(f"  LLM:         {'✓' if llm_ok else '✗'}")
    print(f"  Redis:       {'✓' if redis_ok else '✗'}")
    print(f"  Checkpoint:  {'✓' if ckpt_ok else '✗'}")
    print(f"  Healer:      ✓")
    print(f"  Memory:      ✓")
    print(f"  MCP:         {'✓' if mcp_ok else '✗'}")


def run_graph_mode(task: str, verbose: bool = False) -> None:
    """Graph 状态机 + 完整 HITL 审批/拒绝双路径."""
    _print_banner("Graph 状态机模式（含 HITL 完整链路）")

    initial_state = _empty_state()
    initial_state["messages"] = [task]
    session_id = str(uuid.uuid4())
    print(f"  Task: {task}")
    print(f"  Session: {session_id}")

    # ── 第一步：invoke → human_interrupt 标记 ──────────────────────────
    graph = get_app()
    config = {"configurable": {"thread_id": session_id}}
    result: AgentState = graph.invoke(initial_state, config)

    print(f"\n  [Step 1] invoke 完成")
    print(f"  current_node : {result.get('current_node', '?')}")
    print(f"  code_delta   : {result.get('code_delta', '')}")
    print(f"  retry_count  : {result.get('retry_count', 0)}")
    print(f"  messages     : {len(result.get('messages', []))} 条")
    if verbose:
        for i, msg in enumerate(result.get("messages", [])):
            print(f"    [{i}] {str(msg)[:200]}")

    if result.get("current_node") != "human_interrupt":
        print(f"\n  ✅ 无中断，直接完成。")
        return

    # ── 第二步：HITL 审批 → resume_after_human(approval=True) ─────────
    print(f"\n  ⚠️ 检测到 human_interrupt 标记")
    print(f"  [Step 2] 模拟 HITL 审批 → resume_after_human(approval=True)")

    mgr = registry.get("checkpoint_manager", expected_type=DistributedCheckpointManager)
    mgr.save(session_id, result)
    approved: AgentState = resume_after_human(session_id, approval=True, manager=mgr)

    print(f"  current_node : {approved.get('current_node', '?')}")
    print(f"  code_delta   : {approved.get('code_delta', '')}")
    print(f"  retry_count  : {approved.get('retry_count', 0)}")
    if approved.get("current_node") == "end":
        print(f"  ✅ HITL 审批通过 → end_node")
    else:
        print(f"  ⚠️ 未到 end_node: {approved.get('current_node')}")

    # ── 第三步：HITL 拒绝路径验证 ────────────────────────────────────
    print(f"\n  [Step 3] 模拟 HITL 拒绝 → resume_after_human(approval=False)")
    initial2 = _empty_state()
    initial2["messages"] = ["reject-test"]
    r1 = graph.invoke(initial2, config)
    if r1.get("current_node") == "human_interrupt":
        mgr.save(session_id, r1)
        rejected: AgentState = resume_after_human(session_id, approval=False, manager=mgr)
        print(f"  current_node : {rejected.get('current_node', '?')}")
        print(f"  code_delta   : {rejected.get('code_delta', '')}")
        print(f"  retry_count  : {rejected.get('retry_count', 0)}")
        if rejected.get("retry_count", 0) >= 1:
            print(f"  ✅ 拒绝触发重试循环（retry_count={rejected.get('retry_count', 0)}）")
        else:
            print(f"  ⚠️ 拒绝未触发预期的重试")
    else:
        print(f"  ⚠️ 未触发 human_interrupt: {r1.get('current_node')}")


def run_time_travel_mode(verbose: bool = False) -> None:
    """时间旅行回滚 + time_travel_resume 验证."""
    _print_banner("时间旅行回滚模式")

    mgr = DistributedCheckpointManager()
    session_id = f"tt-demo-{uuid.uuid4().hex[:6]}"
    print(f"  Session: {session_id}")

    # ── 保存 3 个快照 ──────────────────────────────────────────────
    for i in range(1, 4):
        st: AgentState = _empty_state()
        st["messages"] = [f"[snapshot-{i}]"]
        st["current_node"] = f"node_{i}"
        st["code_delta"] = f"delta_{i}"
        st["retry_count"] = i
        key = mgr.save(session_id, st)
        cid = key[len(f"checkpoint:{session_id}:"):]
        print(f"  [{i}] saved → checkpoint_id={cid} | current_node={st['current_node']}")

    # ── rollback 到第 2 个 ──────────────────────────────────────────
    all_keys = mgr._client.keys(f"checkpoint:{session_id}:*")
    sorted_keys = sorted(all_keys)
    target_cid = sorted_keys[1].decode() if isinstance(sorted_keys[1], bytes) else sorted_keys[1]
    target_cid = target_cid[len(f"checkpoint:{session_id}:"):]
    print(f"\n  rollback → checkpoint_id={target_cid}")
    historical = mgr.rollback(session_id, target_cid)
    if historical is not None:
        print(f"  ✅ rollback 成功: current_node={historical['current_node']}, code_delta={historical['code_delta']}")
    else:
        print(f"  ❌ rollback 返回 None")
        return

    # ── time_travel_resume ───────────────────────────────────────────
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

    # ── save_with_lock ──────────────────────────────────────────────
    print(f"\n  调用 save_with_lock({session_id!r}, state)")
    ok = mgr.save_with_lock(session_id, st)
    print(f"  ✅ save_with_lock → {ok}")
    assert ok, "save_with_lock must succeed under no contention"

    # ── 验证持久化 ─────────────────────────────────────────────────
    loaded = mgr.load_latest(session_id)
    if loaded:
        print(f"  ✅ load_latest: current_node={loaded['current_node']}, code_delta={loaded['code_delta']}")
    else:
        print(f"  ❌ load_latest 返回 None")

    # ── 竞争验证：手动占锁 → save_with_lock 应返回 False ──────────
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

    # ── 预埋一个 checkpoint ─────────────────────────────────────────
    mgr = DistributedCheckpointManager()
    mid: AgentState = _empty_state()
    mid["messages"] = ["[pre-seeded]"]
    mid["current_node"] = "intent_parse"
    mid["code_delta"] = "error_v1"
    mid["retry_count"] = 1
    key = mgr.save(session_id, mid)
    print(f"  预埋 checkpoint: {key}")

    # ── run_with_checkpoint 应读取预埋并继续 ───────────────────────
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
    "graph":       lambda args: run_graph_mode(
        args.task or "分析项目代码结构并生成执行计划", verbose=args.verbose,
    ),
    "time_travel": lambda args: run_time_travel_mode(verbose=args.verbose),
    "redlock":     lambda args: run_redlock_mode(),
    "checkpoint":  lambda args: run_checkpoint_mode(verbose=args.verbose),
    "all":         lambda args: run_all_demos(
        args.task or "分析项目代码结构并生成执行计划", verbose=args.verbose,
    ),
}

# ── 哪些模式只需要 A 侧服务（不需要 LLM）──────────────────────────────────

_A_ONLY_FLAGS = {"graph", "time_travel", "redlock", "checkpoint", "all"}


def _parse_cli() -> argparse.Namespace:
    """解析命令行参数，返回 Namespace 对象。"""
    parser = argparse.ArgumentParser(
        description="Enterprise AI Agent Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='示例:\n  python main.py "列出 src/ 下的 .py 文件"\n'
               "  python main.py --graph\n"
               "  python main.py",
    )
    parser.add_argument("task", nargs="?", default=None)
    parser.add_argument(
        "--graph", action="store_true", default=False,
        help="Graph 状态机 + 完整 HITL 审批/拒绝双路径",
    )
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
        "--all", action="store_true", default=False,
        help="顺序执行全部 A 侧接口验证",
    )
    parser.add_argument("--max-steps", type=int, default=10)
    parser.add_argument("--verbose", action="store_true", default=False)

    return parser.parse_args()


def _bootstrap_services(args: argparse.Namespace) -> None:
    """按需注册服务。

    当用户指定 A 侧验证模式（--graph/--time-travel/--redlock/--checkpoint/--all）
    时，仅注册 checkpoint_manager，跳过 LLMRouter 等需要外部 API Key 的服务。
    否则走全量注册。
    """
    activated = {flag for flag in _A_ONLY_FLAGS if getattr(args, flag, False)}

    if activated:
        # A 侧验证模式 — 最少注册
        logger.info("A 侧验证模式 {} — 仅注册 checkpoint 服务", sorted(activated))
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
        # 完整 Agent 模式 — 全量注册
        _register_all_services()


def _active_mode(args: argparse.Namespace) -> str | None:
    """返回当前激活的模式名，无匹配时返回 None。"""
    for flag in _A_ONLY_FLAGS:
        if getattr(args, flag, False):
            return flag
    return None


def _dispatch_mode(args: argparse.Namespace) -> None:
    """根据命令行参数分发到对应模式。

    优先匹配 A 侧验证模式（通过 ``_MODE_REGISTRY`` 查找），
    无匹配时进入 Agent 模式（单任务或交互）。
    """
    mode = _active_mode(args)

    if mode is not None:
        handler = _MODE_REGISTRY[mode]
        handler(args)
        return

    # ── Agent 模式 ──────────────────────────────────────────────────────
    agent = SimpleAgent(max_steps=args.max_steps, verbose=True)

    if args.task:
        result = agent.run(args.task)
        print(f"\n{'='*60}")
        print(f"  {result}")
        print(f"{'='*60}")
        return

    _run_interactive(agent)


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
    """平台入口 — 解析参数 → 注册服务 → 分发模式。

    Returns:
        0 正常退出，1 致命异常。
    """
    args = _parse_cli()

    try:
        _bootstrap_services(args)
        _dispatch_mode(args)
        return 0
    except Exception as exc:
        logger.error("Fatal: {}", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
