# src/security_guard/window.py
import time
import uuid
import logging
from redis import Redis
from .aspect import SecurityInterceptionException

logger = logging.getLogger(__name__)

class SlidingWindowRateLimiter:
    """
    企业级基于 Redis Zset 的滑动时间窗口限流器
    """
    def __init__(self, redis_client: Redis, window_seconds: int = 180, max_calls: int = 5):
        """
        初始化安检仪
        :param redis_client: 注入的 Redis 客户端连接 (强制要求连到 DB 2)
        :param window_seconds: 滑动窗口的时间跨度（秒），默认 3 分钟
        :param max_calls: 窗口期内允许的最大连续调用次数，默认 5 次
        """
        self.redis_client = redis_client
        self.window_seconds = window_seconds
        self.max_calls = max_calls

    def check_and_record(self, session_id: str, tool_name: str) -> bool:
        """
        执行核心安检逻辑：清理过期数据 -> 打卡 -> 统计 -> 拦截决策
        如果触发熔断，直接抛出异常斩断控制流。
        """
        # 1. 构建租户隔离的 Redis Key
        # 格式例如: rate_limit:user_001:compile_code
        redis_key = f"rate_limit:{session_id}:{tool_name}"
        
        # 2. 获取高精度时间戳
        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (self.window_seconds * 1000)
        
        # 3. 【核心防御】生成绝对唯一的打卡记录 (时间戳 + UUID)
        # 防止大模型在同一毫秒内并发调用导致 Redis 哈希键碰撞覆盖
        unique_member = f"{current_time_ms}:{uuid.uuid4().hex}"

        try:
            # 4. 【性能优化】开启 Redis Pipeline 流水线
            # 把 3 条命令打包成 1 个网络包发给 Redis，将长尾延迟压制在 5ms 以内
            pipeline = self.redis_client.pipeline()
            
            # 动作 A：橡皮擦。清理时间窗口之前的陈旧数据
            pipeline.zremrangebyscore(redis_key, 0, window_start_ms)
            # 动作 B：盖章。压入本次调用的唯一记录
            pipeline.zadd(redis_key, {unique_member: current_time_ms})
            # 动作 C：盘点。统计当前窗口内还剩多少条有效记录
            pipeline.zcard(redis_key)
            
            # 5. 一次性执行流水线并获取结果
            results = pipeline.execute()
            current_count = results[2] # zcard 的结果在列表的第 3 个位置
            
            logger.debug(f"[滑动窗口] {tool_name} (会话:{session_id}) 窗口内调用量: {current_count}/{self.max_calls}")

            # 6. 【执法决策】如果超标，立刻拔网线！
            if current_count > self.max_calls:
                logger.warning(f"🚫 [熔断警报] 拦截！会话 {session_id} 疯狂调用 {tool_name} 达 {current_count} 次！")
                
                # 抛出我们在 aspect.py 中定义的专属异常
                raise SecurityInterceptionException(
                    f"Tool Loop Detected: '{tool_name}' has been called {current_count} times "
                    f"within {self.window_seconds} seconds. Execution blocked."
                )
                
            return True

        except SecurityInterceptionException:
            # 遇到安全阻断异常，直接向上抛出，不被下面的宽容逻辑吃掉
            raise
        except Exception as e:
            # 【高可用妥协原则 (Fail Open)】
            # 如果是 Redis 服务器自己崩了（比如断网），我们不能让整个 Agent 平台瘫痪。
            # 此时选择打印错误，但放行大模型的调用。
            logger.error(f"⚠️ [中间件异常] Redis 滑动窗口执行失败，临时放行: {e}")
            return True