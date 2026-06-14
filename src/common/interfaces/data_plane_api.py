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
