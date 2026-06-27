# src/data_plane/llm_router.py
import os
import time
from typing import List, Dict, Any, Optional
from loguru import logger
from openai import OpenAI
from src.common.interfaces.data_plane_api import ILLMRouter
from langsmith import traceable

# src/data_plane/llm_router.py
# ... 前面 import 保持不变 ...

#  引入你（成员 F）写的高阶防御装饰器
from src.compute_engine.runtime.api_interceptor import resilient_api_call

class LLMRouter(ILLMRouter):
    # ... __init__ 保持不变 ...

    @traceable(name="LLM_Call_Primary", run_type="llm")
    @resilient_api_call  #  只需要加上这一行魔法！
    def chat_primary(self, messages: List[Dict[str, Any]], temperature: float = 0.0) -> str:
        """调用主力推理模型（所有重试、限流、灾备均已交由底层网关 AOP 接管）"""
        
        if not self.primary_client:
            raise LLMCallError("Primary client is not initialized.")

        logger.info(f"[LLMRouter] 路由至主力模型: {self.primary_model_name}")
        safe_messages = self._normalize_messages(messages)
        
        #  删掉了所有 for 循环、try-except 和 time.sleep！
        # 直接发包，最纯粹的业务逻辑
        response = self.primary_client.chat.completions.create(
            model=self.primary_model_name,
            messages=safe_messages,
            temperature=temperature
        )
        
        content = response.choices[0].message.content
        if content is None:
            raise LLMCallError("Primary LLM returned empty content.")
            
        logger.info(f"[LLMRouter] 主力模型通信成功，长度: {len(content)}")
        return content

    @traceable(name="LLM_Call_Fast", run_type="llm")
    @resilient_api_call  #  副轨模型也套上保护伞
    def chat_fast(self, messages: List[Dict[str, Any]], temperature: float = 0.3) -> str:
        # ... 同样删掉内部的 try-except，只保留最核心的 .create() 发包代码 ...