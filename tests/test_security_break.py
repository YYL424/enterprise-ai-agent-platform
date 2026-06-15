import time
import redis
import os
from dotenv import load_dotenv
from functools import wraps

# 1. 极简版 Redis 测试 (你需要替换为你的 DB 2)
load_dotenv() # 加载 .env 文件

redis_client = redis.Redis(
    host='localhost', 
    port=6379, 
    db=2, 
    password=os.getenv('REDIS_PASSWORD'), 
    decode_responses=True
)

# 2. 你的 MVD 核心产出：AOP 拦截器骨架
def tool_guard(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tool_name = func.__name__
        print(f"[安全审计] 拦截到工具调用意图: {tool_name}")
        
        # 写入 Redis 测试
        current_time = int(time.time() * 1000)
        redis_client.zadd(f"window:{tool_name}", {str(current_time): current_time})
        count = redis_client.zcard(f"window:{tool_name}")
        print(f"[滑动窗口] {tool_name} 当前调用次数: {count}")
        
        if count > 5:
            print("[熔断器] 触发熔断！阻止执行。")
            return "CircuitBreakerTriggered"
            
        result = func(*args, **kwargs)
        print(f"[安全审计] 工具执行完毕，放行。")
        return result
    return wrapper

# 3. 模拟成员 B 写的工具
@tool_guard
def get_weather(city: str) -> str:
    """Get weather for a given city."""
    time.sleep(0.1) # 模拟网络延迟
    return f"It's always sunny in {city}!"

# 4. 执行测试
if __name__ == "__main__":
    print("=== 开始 Member C 基础基建测试 ===")
    print(get_weather("San Francisco"))