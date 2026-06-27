"""成员C（Security Guard）对外暴露的接口契约。

成员A和B通过此接口与C交互，禁止绕过安全层直接调用工具。
"""

from __future__ import annotations
from typing import Any, Callable, Dict, Optional
from src.common.interfaces.types import AuditRecord, SecurityVerdict, ToolCallPayload


class IAuditMiddleware:
    """AOP审计中间件接口（成员C实现，A/B挂载）"""
    
    def audit_tool_call(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """装饰器：包装工具函数，执行前置/后置审计。"""
        raise NotImplementedError
    
    def get_audit_log(self, session_id: str, limit: int = 100) -> list[AuditRecord]:
        """查询指定会话的审计日志。"""
        raise NotImplementedError


class ISafetyGuard:
    """安全护栏接口（成员C实现，A/B在工具调用前检查）"""
    
    def check_tool_loop(self, session_id: str, tool_name: str) -> SecurityVerdict:
        """检查连续调用熔断，防止单工具死循环。"""
        raise NotImplementedError
    
    def check_alternating_loop(self, session_id: str, tool_sequence: list[str]) -> SecurityVerdict:
        """检查交替型死循环（A→B→A→B），防止华尔兹步频欺骗。"""
        raise NotImplementedError
    
    def check_sliding_window(self, session_id: str) -> SecurityVerdict:
        """检查滑动时间窗口内的总调用频次，防止爆发型幻觉。"""
        raise NotImplementedError


class IMemoryManager:
    """记忆管理接口（成员C实现，A/B在状态更新时调用）"""

    def trim_active_window(self, messages: list[str], max_rounds: int = 3) -> list[str]:
        """裁剪Active Window，只保留最近N轮详细上下文。"""
        raise NotImplementedError

    def generate_summary(self, historical_logs: list[str]) -> str:
        """调用FAST_LLM生成历史摘要，注入长期记忆区。"""
        raise NotImplementedError


# ── C 侧预留钩子（A/B 直接调用，C 后续实现）──────────────────────────────────


def audit_tool_invocation(
    session_id: str,
    tool_name: str,
    path_hash: str = "",
) -> dict:
    """C 的 AOP 审计入口 — 在工具调用前执行安全检查。

    由成员 A 的控制平面在每次 Skill/Tool 调用前调用，由成员 C 的
    AOP 环绕拦截器实现。 返回的 dict 包含放行/阻断决策及理由。

    Args:
        session_id: 当前会话标识（对应 LangGraph thread_id）。
        tool_name: 即将调用的工具名称（如 ``"compile_code"``）。
        path_hash: 可选，当前有向执行路径的 MD5 指纹（用于交替型死循环检测）。

    Returns:
        ``{"allowed": True, "reason": "..."}`` 表示放行；
        ``{"allowed": False, "reason": "..."}`` 表示阻断（触发熔断/安全越权）。

    Raises:
        NotImplementedError: C 尚未实现此钩子。
    """
    raise NotImplementedError("C 待实现 — AOP 审计中间件尚未挂载")
