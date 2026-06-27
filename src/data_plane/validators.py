# src/data_plane/validators.py
from typing import Any, Dict
from pydantic import ValidationError
from loguru import logger

class DataValidator:
    """运行时强类型校验器与边界审计屏障 (已引入协议感知模式)"""
    
    @staticmethod
    def audit_and_execute(payload: Dict[str, Any], schema_engine: Any, schema_obj: Any) -> Dict[str, Any]:
        """
        核心修正：增加协议感知 Bypass 逻辑
        校验器现在会先判断 payload 是否为基础 ReAct 协议格式，避免误伤
        """
        # 1. 协议感知 Bypass：如果 payload 包含基础控制键，视为无需进行领域模型校验的通用消息
        # 常见基础结构: {"answer": "..."}, {"tool": "..."}, {"question": "..."}
        basic_keys = {"answer", "tool", "question", "tool_calls"}
        if any(key in payload for key in basic_keys):
            logger.debug("[Data_Plane] 协议感知模式触发：检测到基础控制流，跳过领域 Schema 强制校验")
            return payload
        
        # 2. 领域数据校验：仅当 payload 看起来像是业务负载时，才触发强类型校验
        try:
            return schema_engine.validate_payload(payload, schema_obj)
        except ValidationError as e:
            logger.error(f"[Data_Plane] 严重契约违背: {e.errors()}")
            raise e