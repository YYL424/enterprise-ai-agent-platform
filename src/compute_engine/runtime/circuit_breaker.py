"""
Enterprise AI Platform - Module 2 (Runtime Plane)
Path: src/compute_engine/runtime/circuit_breaker.py
Author: 成员 F
Description: 提供线程安全的三态熔断器有限状态机、指数退避算法与令牌桶限流器。
"""

import time
import random
import threading
from enum import Enum
from loguru import logger
import yaml
from pathlib import Path


class CircuitState(Enum):
    CLOSED = "CLOSED"       # 闭合状态：正常放行所有请求
    OPEN = "OPEN"           # 开启状态：触发熔断，拒绝所有请求
    HALF_OPEN = "HALF_OPEN" # 半开状态：冷却结束，放行极少量探针请求


class TokenBucket:
    """令牌桶算法实现，用于控制 API 瞬时并发与 QPS 限流"""
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill_time = time.time()
        self._lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            # 计算自上次补充以来生成的令牌数
            elapsed = now - self.last_refill_time
            new_tokens = elapsed * self.refill_rate
            
            if new_tokens > 0:
                self.tokens = min(self.capacity, self.tokens + new_tokens)
                self.last_refill_time = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


class CircuitBreaker:
    """三态有限状态机熔断器与指数退避引擎"""
    def __init__(self, config_path: str):
        # 动态加载 YAML 配置
        self._load_config(config_path)
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self._lock = threading.Lock()

    def _load_config(self, config_path: str):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f).get("api_gateway", {})
                self.failure_threshold = config.get("max_failure_threshold", 3)
                self.cooldown_period = config.get("circuit_breaker_cooldown_period", 60)
                self.base_backoff = config.get("exponential_backoff_base_time", 2)
                self.max_retries = config.get("max_retry_limit", 5)
                
                tb_config = config.get("token_bucket", {})
                self.rate_limiter = TokenBucket(
                    capacity=tb_config.get("capacity", 60),
                    refill_rate=tb_config.get("refill_rate_per_sec", 10)
                )
        except Exception as e:
            logger.error(f"Failed to load gateway_policy.yaml: {e}. Using defaults.")
            raise

    def check_request_allowed(self) -> bool:
        """在发起请求前调用，检查熔断器状态与令牌桶"""
        with self._lock:
            if self.state == CircuitState.OPEN:
                # 检查是否度过冷却期
                if time.time() - self.last_failure_time > self.cooldown_period:
                    logger.warning("熔断器冷却期结束，进入 HALF_OPEN 状态，准备发射探针...")
                    self.state = CircuitState.HALF_OPEN
                    return True  # 放行探针请求
                return False     # 仍在冷却期，直接熔断

            elif self.state == CircuitState.HALF_OPEN:
                # 在半开状态下，仅允许串行探针，此处策略可简化为直接放行该请求
                return True
                
        # 如果是 CLOSED 状态，还需检查令牌桶是否溢出
        if not self.rate_limiter.consume(1):
            logger.warning("令牌桶限流触发，瞬时请求速率过高。")
            return False
            
        return True

    def record_success(self):
        """请求成功后回调，重置状态机"""
        with self._lock:
            if self.state != CircuitState.CLOSED:
                logger.success("探针请求成功，下游服务已自愈！熔断器重置为 CLOSED。")
                self.state = CircuitState.CLOSED
                self.failure_count = 0

    def record_failure(self):
        """请求失败（5xx/429/超时）后回调，步进错误计数"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            # 如果处于半开状态，或者失败次数达到阈值，立刻跳闸
            if self.state == CircuitState.HALF_OPEN or self.failure_count >= self.failure_threshold:
                if self.state != CircuitState.OPEN:
                    logger.error(f"API 连续失败 {self.failure_count} 次，熔断器跳闸进入 OPEN！冷却 {self.cooldown_period}s。")
                self.state = CircuitState.OPEN

    def get_exponential_backoff_time(self, retry_attempt: int) -> float:
        """获取带有随机抖动 (Jitter) 的指数退避睡眠时间"""
        # Delay = BaseTime * 2^retry_count ± RandomJitter
        delay = self.base_backoff * (2 ** (retry_attempt - 1))
        jitter = random.uniform(0, 0.3 * delay) # 30% 抖动防踩踏
        return delay + jitter