# src/security_guard/aspect.py
import time
import logging
from functools import wraps
from langsmith import traceable

# 配置基础日志，方便控制台观测
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义安全层专属的根异常
class SecurityInterceptionException(Exception):
    """当触发任何安全护栏（如熔断、死循环、越权拦截）时抛出。
    成员 A 的状态机会专门 catch 这个异常来挂起任务。"""
    pass

def audit_tool_call(rate_limiter=None, path_analyzer=None):
    """
    企业级全链路 AOP 安全审计切面 (高阶装饰器)
    
    :param rate_limiter: 注入的滑动窗口限流器实例 (如 window.py 中的类)
    :param path_analyzer: 注入的有向拓扑死循环检测器实例 (如 path_hash.py 中的类)
    """
    def decorator(func):
        # 【架构强约束】：@traceable 必须在最外层包裹
        # 这样才能在 LangSmith 上精准录制到被安全策略强杀的 Error 红色链路
        @traceable(name=f"Tool_Execution_{func.__name__}")
        @wraps(func)
        def wrapper(*args, **kwargs):
            tool_name = func.__name__
            
            # 【租户隔离】：企业级生产环境必须有 session_id，不能搞全局大锅饭
            # 假设上层调用工具时，会在 kwargs 里传入 session_id，如果没传则给个兜底
            session_id = kwargs.get("session_id", "anonymous_session")

            logging.info(f"🛡️ [AOP 前置审计] 拦截到工具调用请求 | 目标: {tool_name} | 会话: {session_id}")
            t0 = time.perf_counter()

            # ==========================================
            # 1. 前置安全审查 (Pre-execution Audit)
            # ==========================================
            # 指挥调度：让专业的模块干专业的事
            if rate_limiter:
                # 如果超出阈值，rate_limiter 内部会抛出 SecurityInterceptionException
                rate_limiter.check_and_record(session_id, tool_name)

            if path_analyzer:
                # 如果判定为隐蔽交替死循环，内部同样抛出异常强行斩断
                path_analyzer.check_and_record(session_id, tool_name)

            # ==========================================
            # 2. 物理隔离放行 (Execution)
            # ==========================================
            try:
                # 如果前面的安检全部通过，才真正执行业务代码
                result = func(*args, **kwargs)
            except Exception as e:
                # 区分【安全拦截】与【业务本身报错】
                logging.error(f"❌ [业务运行异常] 工具 {tool_name} 内部执行崩溃: {str(e)}")
                raise e

            # ==========================================
            # 3. 后置审计 (Post-execution Audit)
            # ==========================================
            latency_ms = (time.perf_counter() - t0) * 1000
            logging.info(f"✅ [AOP 后置审计] 工具 {tool_name} 执行完毕 | 耗时: {latency_ms:.2f}ms")

            return result

        return wrapper
    return decorator