"""成员A（Control Plane）对外暴露的接口契约。

成员B和C通过此接口与A交互，禁止直接调用A的内部实现。
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Callable
from src.common.interfaces.types import AgentState, JobRequest, JobResponse


class IGraphEngine:
    """状态机引擎接口（成员A实现，B/C调用）"""
    
    def invoke(self, initial_state: AgentState) -> AgentState:
        """运行一次完整的状态机流转，返回最终状态。"""
        raise NotImplementedError
    
    def get_current_node(self, thread_id: str) -> str:
        """获取指定线程当前所在的节点名称。"""
        raise NotImplementedError
    
    def rollback_to_checkpoint(self, thread_id: str, checkpoint_id: str) -> AgentState:
        """回退到指定Checkpoint，返回恢复后的状态。"""
        raise NotImplementedError


class ICheckpointManager:
    """Checkpoint持久化接口（成员A实现，B/C可查询）"""
    
    def save(self, thread_id: str, state: AgentState) -> str:
        """保存状态快照，返回checkpoint_id。"""
        raise NotImplementedError
    
    def load_latest(self, thread_id: str) -> Optional[AgentState]:
        """加载指定线程的最新状态。"""
        raise NotImplementedError
    
    def list_checkpoints(self, thread_id: str) -> List[str]:
        """列出指定线程的所有Checkpoint ID。"""
        raise NotImplementedError


class IJobDispatcher:
    """作业分发接口（成员A实现，向Module 2下发）"""
    
    def dispatch(self, job: JobRequest) -> JobResponse:
        """将作业下发到中层K8s调度中心，返回响应。"""
        raise NotImplementedError
