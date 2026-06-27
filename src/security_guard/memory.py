# src/security_guard/memory.py
import time
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class DualLayerMemoryGovernor:
    """
    企业级双层记忆管理器 (主动滑动窗口 + 后台异步轻量化摘要)
    目标：解决长文本幻觉，斩断 Token 成本二次方雪崩。
    """
    def __init__(self, max_window_size: int = 3):
        """
        :param max_window_size: 核心高保真窗口保留的最大轮数，默认 3 轮
        """
        self.max_window_size = max_window_size
        
        # 【架构核心】独立的后台线程池
        # 记忆压缩绝不能阻塞成员 A 的主状态机流转，必须异步执行
        self.async_pool = ThreadPoolExecutor(
            max_workers=4, 
            thread_name_prefix="Memory_Summarizer"
        )

    def _generate_milestone_summary_task(self, evicted_turn: dict, global_summary: list):
        """
        被后台线程调用的实际压缩任务（真实 LLM 接入版）
        """
        try:
            logger.info("⏳ [后台摘要] 触发 FAST_LLM 进行老旧记忆压缩...")
            
            # 1. 动态获取数据平面的大模型路由
            # 这里通过你们团队公共的 registry 来获取，避免物理强耦合
            from src.common.registry import registry
            llm_router = registry.get("llm_router")
            
            if not llm_router:
                raise RuntimeError("LLMRouter 未就绪，无法进行语义压缩")

            # 2. 提取需要压缩的原始长文本
            tool_name = evicted_turn.get("tool_name", "unknown")
            raw_output = evicted_turn.get("output", "")
            raw_error = evicted_turn.get("error", "")
            
            # 3. 组装提炼 Prompt
            prompt = (
                f"你是一个记忆压缩助手。Agent刚才执行了工具 [{tool_name}]。\n"
                f"执行输出：\n{raw_output[:2000]}\n"  # 限制下输入长度防爆炸
                f"报错信息：{raw_error}\n\n"
                f"请用不超过30个字的一句话，客观总结这次工具执行的核心结果（发现了什么，或失败原因是什么），作为历史里程碑记录。"
            )

            # 4. 真实调用大模型 (使用低温度保证客观事实)
            milestone_content = llm_router.chat_primary(
                messages=[{"role": "user", "content": prompt}], 
                temperature=0.1
            )
            
            milestone = f"[{tool_name} 结果]: {milestone_content.strip()}"
            
            # 5. 压入长记忆池
            global_summary.append(milestone)
            
            logger.info(f"✨ [后台摘要] 真实语义压缩完成！新增里程碑: {milestone}")
            
        except Exception as e:
            # 异步线程里的报错不能影响主进程
            logger.error(f"❌ [后台摘要] 记忆压缩失败，降级为默认拼接: {e}")
            # Fail-Open: 如果 API 挂了，降级回退到你的字符串拼接方案
            fallback_milestone = f"历史里程碑 -> 尝试了 {evicted_turn.get('tool_name')}"
            global_summary.append(fallback_milestone)

    def process_memory_tick(self, active_window: list, global_summary: list, new_turn: dict) -> list:
        """
        在每一轮大模型推理结束后，由成员 A 的图状态机调用此接口。
        
        :param active_window: 当前的短期高保真窗口 (传引用)
        :param global_summary: 当前的长期摘要池 (传引用)
        :param new_turn: 本轮刚产生的新交互日志 (含入参、执行结果、报错文本等)
        :return: 更新后的 active_window
        """
        # 1. 将最新的一轮高保真记录压入短期窗口
        active_window.append(new_turn)

        # 2. 检查是否溢出 (滑动窗口机制)
        if len(active_window) > self.max_window_size:
            # 踢出最旧的一轮 (FIFO 先进先出)
            evicted_turn = active_window.pop(0)
            
            logger.debug(f"🗑️ [记忆滑动] 窗口超载 (> {self.max_window_size} 轮)，剔除最旧日志并移交后台。")

            # 3. 【核心设计】将弹出的几万字老日志丢进线程池异步压缩，主进程瞬间返回
            self.async_pool.submit(
                self._generate_milestone_summary_task, 
                evicted_turn, 
                global_summary
            )

        return active_window