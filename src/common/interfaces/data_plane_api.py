"""成员B（Data Plane）对外暴露的接口契约。

成员A和C通过此接口与B交互，禁止直接操作B的Schema文件。
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Type
from src.common.interfaces.types import DomainSchema, ToolCallPayload


class ISchemaEngine:
    """Schema解析引擎接口（成员B实现，A/C调用）"""
    
    def load_schema(self, domain_name: str) -> DomainSchema:
        """加载指定领域的Schema，返回结构化契约。"""
        raise NotImplementedError
    
    def validate_payload(self, payload: Dict[str, Any], schema: DomainSchema) -> Dict[str, Any]:
        """根据Schema校验载荷，返回校验后的数据或抛出ValidationError。"""
        raise NotImplementedError
    
    def hot_inject(self, schema_path: str) -> None:
        """热注入新的Schema文件，无需重启系统。"""
        raise NotImplementedError


class IDataHealer:
    """数据自愈引擎接口（成员B实现，A/C在异常时调用）"""
    
    def heal_truncated_json(self, raw_text: str) -> Optional[str]:
        """第一级：Regex贪婪剥离 + 第二级：栈扫描补齐。"""
        raise NotImplementedError
    
    def heal_missing_fields(self, partial_data: Dict[str, Any], error_path: List[str]) -> Dict[str, Any]:
        """第三级：基于ValidationError路径的靶向修复。"""
        raise NotImplementedError


class ILLMRouter:
    """大模型网关路由接口（成员B实现，A/C调用）"""

    def chat_primary(self, messages: List[Dict[str, str]], temperature: float = 0.0) -> str:
        """调用主力大模型（Claude-3.5-Sonnet），用于核心推理。"""
        raise NotImplementedError

    def chat_fast(self, messages: List[Dict[str, str]], temperature: float = 0.3) -> str:
        """调用快速大模型（Qwen-2.5-7B），用于摘要和修复。"""
        raise NotImplementedError


# ── B 侧预留钩子（A/C 直接调用，B 后续实现）──────────────────────────────────


def validate_schema(payload: dict, schema_name: str = "default") -> dict:
    """B 的 Schema 校验入口。

    根据 *schema_name* 对应的 JSON Schema 对 *payload* 执行运行时强类型校验。
    校验通过返回原始 payload（可能附带类型强制转换）；校验失败抛出
    ``ValidationError``（由 B 的 Pydantic 校验层生成）。

    Args:
        payload: 待校验的字典载荷（通常来自大模型 Function Calling 输出）。
        schema_name: 目标领域 Schema 名称，对应 ``contracts/domains/`` 下的
            ``<schema_name>.json`` 文件。

    Returns:
        校验通过后的 payload dict。

    Raises:
        NotImplementedError: B 尚未实现此钩子。
        ValidationError: B 实现后，当 payload 不符合 Schema 时抛出。
    """
    raise NotImplementedError("B 待实现 — Schema 校验引擎尚未挂载")
