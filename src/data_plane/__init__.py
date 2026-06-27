# src/data_plane/__init__.py
"""
Data Plane — member B's exclusive module.
提供 Schema 动态编译、数据自愈、运行时审计、双轨制 LLM 路由以及 MCP 客户端通信接口。
"""

# 导出核心接口与实现类
from .engine import SchemaEngine
from .healer import DataHealer
from .llm_router import LLMRouter, LLMCallError
from .validators import DataValidator
from .mcp_client_proxy import DataPlaneMCPClient
from .mcp_gateway import MCPGateway

__all__ = [
    "SchemaEngine",
    "DataHealer",
    "LLMRouter",
    "LLMCallError",
    "DataValidator",
    "DataPlaneMCPClient",
    "MCPGateway",
]