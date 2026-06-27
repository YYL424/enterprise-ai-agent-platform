# src/security_guard/path_hash.py
import time
import uuid
import hashlib
import logging
from redis import Redis
from .aspect import SecurityInterceptionException

logger = logging.getLogger(__name__)

class PathHashLoopDetector:
    """
    企业级有向拓扑交替死循环检测器 (基于 Path-Hash)
    """
    def __init__(self, redis_client: Redis, history_length: int = 6, 
                 window_seconds: int = 180, max_path_repeats: int = 3):
        """
        初始化拓扑雷达
        :param redis_client: Redis 客户端连接
        :param history_length: 追踪最近多少步的执行轨迹，默认 6 步
        :param window_seconds: 观察滑动窗口跨度，默认 3 分钟
        :param max_path_repeats: 相同路径指纹在窗口期内允许出现的最大次数
        """
        self.redis_client = redis_client
        self.history_length = history_length
        self.window_seconds = window_seconds
        self.max_path_repeats = max_path_repeats

    def _calculate_path_hash(self, path_list: list) -> str:
        """
        核心算法：将路径序列转换为 MD5 拓扑指纹
        输入示例: ['compile', 'search', 'compile', 'search']
        """
        # 必须凑够一定的步数才开始算作一个有效路径（比如至少3步）
        if len(path_list) < 3:
            return "too_short_to_hash"
            
        path_str = " ➜ ".join(path_list)
        # 计算 MD5，把长字符串压缩成极短的唯一指纹
        return hashlib.md5(path_str.encode('utf-8')).hexdigest()

    def check_and_record(self, session_id: str, tool_name: str) -> bool:
        """
        执行交替死循环探测：记录尾迹 -> 算指纹 -> 滑动窗口审查
        """
        list_key = f"path_history:{session_id}"
        
        try:
            # ==========================================
            # 阶段一：维护会话的“最近 N 步飞行尾迹”
            # ==========================================
            pipe = self.redis_client.pipeline()
            # 1. 压入当前工具
            pipe.rpush(list_key, tool_name)
            # 2. 截断队列，永远只保留最近 N 步 (从 -history_length 保留到 -1)
            pipe.ltrim(list_key, -self.history_length, -1)
            # 3. 拿出当前全部轨迹
            pipe.lrange(list_key, 0, -1)
            
            list_results = pipe.execute()
            current_path = list_results[2] # 拿到 lrange 的返回列表
            
            # ==========================================
            # 阶段二：计算拓扑指纹
            # ==========================================
            path_hash = self._calculate_path_hash(current_path)
            
            if path_hash == "too_short_to_hash":
                return True # 步数太少，还没形成规律，安全放行
                
            logger.debug(f"[轨迹追踪] 当前路径: {' ➜ '.join(current_path)} | 指纹: {path_hash[:8]}")

            # ==========================================
            # 阶段三：对该“指纹”执行滑动窗口限流
            # ==========================================
            hash_window_key = f"path_window:{session_id}:{path_hash}"
            
            current_time_ms = int(time.time() * 1000)
            window_start_ms = current_time_ms - (self.window_seconds * 1000)
            unique_member = f"{current_time_ms}:{uuid.uuid4().hex}"

            # 再次开启流水线，对这个特定指纹进行频次盘点
            pipe = self.redis_client.pipeline()
            pipe.zremrangebyscore(hash_window_key, 0, window_start_ms)
            pipe.zadd(hash_window_key, {unique_member: current_time_ms})
            pipe.zcard(hash_window_key)
            
            zset_results = pipe.execute()
            repeat_count = zset_results[2]

            # ==========================================
            # 阶段四：终极审判
            # ==========================================
            if repeat_count > self.max_path_repeats:
                logger.error(f"☠️ [交替死循环捕获] 会话 {session_id} 陷入逻辑死锁！路径特征: {path_hash[:8]}")
                raise SecurityInterceptionException(
                    f"Alternating Loop Detected: The execution path pattern has repeated "
                    f"{repeat_count} times. Execution violently blocked."
                )

            return True

        except SecurityInterceptionException:
            raise
        except Exception as e:
            logger.error(f"⚠️ [中间件异常] Path-Hash 检测失败，临时放行: {e}")
            return True