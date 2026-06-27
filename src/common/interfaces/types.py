"""跨模块共享类型契约（Control Plane / Data Plane / Security Guard 统一接口）

三人铁律：
- 任何成员修改此文件，必须在团队群聊通知，并重新推送 GitHub
- 修改后所有成员必须执行 git pull 同步
- 禁止私自修改他人模块依赖的类型字段
"""

from __future__ import annotations

import operator
from typing import Annotated, List, TypedDict, Dict, Any, Optional
from enum import Enum


# ==============================================================================
# 1. 全局状态定义（成员A主导，B/C只读）
# ==============================================================================

class AgentState(TypedDict):
    """LangGraph 状态机全局状态定义。
    
    成员A负责维护，成员B/C读取但不修改定义。
    列表字段使用 operator.add 确保并发安全。
    """
    messages: Annotated[List[str], operator.add]
    current_node: str
    code_delta: str
    execution_logs: Annotated[List[str], operator.add]
    retry_count: int


# ==============================================================================
# 2. 数据契约类型（成员B主导，A/C只读）
# ==============================================================================

class DomainSchema(TypedDict):
    """动态注入的领域 Schema 契约。
    
    成员B负责加载和校验，A/C通过接口获取。
    """
    meta_config: Dict[str, Any]
    runtime_parameters: Dict[str, Any]
    output_alignment: Dict[str, Any]


class ToolCallPayload(TypedDict):
    """大模型 Tool Calling 的载荷格式。
    
    成员B校验后，传递给成员A的状态机或成员C的审计层。
    """
    tool_name: str
    arguments: Dict[str, Any]
    session_id: str


# ==============================================================================
# 3. 安全与审计类型（成员C主导，A/B只读）
# ==============================================================================

class AuditRecord(TypedDict):
    """AOP 审计日志记录格式。
    
    成员C生成，A/B可通过接口查询。
    """
    session_id: str
    tool_name: str
    timestamp: float
    duration_seconds: float
    status: str  # "SUCCESS" | "RUNTIME_EXCEPTION" | "CRITICAL_ERROR"
    error_message: Optional[str]


class SecurityVerdict(TypedDict):
    """安全护栏的裁决结果。
    
    成员C返回给A/B，决定是否放行。
    """
    allowed: bool
    reason: Optional[str]
    circuit_breaker_triggered: bool


# ==============================================================================
# 4. 跨模块作业类型（Module 1 → Module 2 接口）
# ==============================================================================

class JobRequest(TypedDict):
    """向中层 K8s 调度中心下发的作业请求。
    
    成员A生成，成员B校验字段类型，成员C审计安全性。
    """
    task_id: str
    gpu_model: str
    gpus_required: int
    commands: List[str]
    sandbox_timeout_seconds: int
    compute_intensity: str  # "HIGH" | "MEDIUM" | "LOW"


class JobResponse(TypedDict):
    """中层 K8s 返回的作业状态。
    
    成员A接收并驱动状态机流转。
    """
    status: str  # "RUNNING" | "SUCCEEDED" | "FAILED" | "TIMEOUT"
    pod_name: Optional[str]
    reason: Optional[str]
