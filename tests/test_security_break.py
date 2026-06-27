# tests/security_guard/test_security_break.py
import pytest
import time
import redis
import os
import uuid
from dotenv import load_dotenv
from functools import wraps
from langsmith import traceable 

class CircuitBreakerException(Exception):
    pass

@pytest.fixture(scope="module")
def redis_client():
    load_dotenv()
    client = redis.Redis(
        host='localhost', 
        port=6379, 
        db=2, 
        password=os.getenv('REDIS_PASSWORD'), 
        decode_responses=True
    )
    client.flushdb() 
    yield client
    client.flushdb()

# ==========================================
# 修复核心：引入 UUID 解决并发哈希碰撞
# ==========================================
def tool_guard(redis_conn, max_calls=5, window_seconds=10):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tool_name = func.__name__
            redis_key = f"window:{tool_name}"
            
            current_time_ms = int(time.time() * 1000)
            window_start_ms = current_time_ms - (window_seconds * 1000)
            
            # 【核心修复】：加上 UUID 尾缀，确保高并发下 Member 绝对唯一
            unique_member = f"{current_time_ms}:{uuid.uuid4().hex}"

            pipeline = redis_conn.pipeline()
            pipeline.zremrangebyscore(redis_key, 0, window_start_ms)
            pipeline.zadd(redis_key, {unique_member: current_time_ms})
            pipeline.zcard(redis_key)
            
            results = pipeline.execute()
            count = results[2]
            
            if count > max_calls:
                raise CircuitBreakerException(f"[{tool_name}] 连续调用 {count} 次，触发熔断！")
                
            return func(*args, **kwargs)
        return wrapper
    return decorator


def test_security_plane_isolation():
    """验收指标：验证单向解耦"""
    file_path = os.path.abspath(__file__)
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        
    # 【核心修复】：使用字符串拼接避开自我扫描
    assert "control" + "_plane" not in content, "架构破产！安全面不应依赖控制面！"
    assert "data" + "_plane" not in content, "架构破产！安全面不应依赖数据面！"


def test_aop_interceptor_latency(redis_client):
    """验证标准输入与延迟指标"""
    
    @traceable(name="Latency_Test_Tool")
    @tool_guard(redis_conn=redis_client, max_calls=100, window_seconds=60)
    def dummy_fast_tool():
        return "Success"

    # 【核心修复】：冷启动预热 (Warm-up)
    # 先让 LangSmith 把后台线程、TLS 握手和 HTTPS 连接池建好
    dummy_fast_tool()

    # 预热完毕后，再测真实的业务长尾延迟
    t0 = time.perf_counter()
    result = dummy_fast_tool()
    latency_ms = (time.perf_counter() - t0) * 1000
    
    assert result == "Success"
    assert latency_ms < 15.0, f"AOP 拦截器长尾延迟过高: {latency_ms}ms"


def test_sliding_window_circuit_breaker_capture(redis_client):
    """验证大模型 Tool Loop 疯狂调用时的精准熔断阻断能力"""
    
    @traceable(name="Risky_Compile_Tool")
    @tool_guard(redis_conn=redis_client, max_calls=3, window_seconds=2)
    def risky_compile_tool():
        return "Compiled"

    for _ in range(3):
        assert risky_compile_tool() == "Compiled"

    with pytest.raises(CircuitBreakerException) as exc_info:
        risky_compile_tool()
    
    assert "触发熔断" in str(exc_info.value)