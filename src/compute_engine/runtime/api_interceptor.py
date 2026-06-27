"""
Enterprise AI Platform - Module 2 (Runtime Plane)
Path: src/compute_engine/runtime/api_interceptor.py
Author: 成员 F
Description: 异步无侵入式高阶装饰器，用于全局接管外部 API 请求的限流、重试与异构灾备切拨。
"""

import os
import yaml
import asyncio
import functools
from loguru import logger
from openai import APIError, RateLimitError, APIConnectionError, InternalServerError

# 引入上一轮我们写好的熔断器心脏
from .circuit_breaker import CircuitBreaker, CircuitState

# 1. 实例化全局单例网关控制台
_gateway_breaker = CircuitBreaker("config/module2/gateway_policy.yaml")

def _load_failover_config():
    """读取灾备路由配置字典"""
    try:
        with open("config/module2/gateway_policy.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f).get("api_gateway", {})
            return config.get("failover_routing", {})
    except Exception as e:
        logger.error(f"加载 failover 路由表失败: {e}")
        return {}

_failover_routing = _load_failover_config()


def resilient_api_call(func):
    """
    高阶异步装饰器：AOP 拦截器
    预期包裹的目标：LLMRouter 中的异步 chat_primary / chat_fast 等核心网络方法。
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        max_retries = _gateway_breaker.max_retries
        trigger_failover = False  # 是否触发异构灾备的开关

        # ==========================================
        # 阶段一：限流、熔断与指数退避重试 (Primary Loop)
        # ==========================================
        for attempt in range(1, max_retries + 1):
            
            # 1. 令牌桶限流与熔断器检查
            if not _gateway_breaker.check_request_allowed():
                if _gateway_breaker.state == CircuitState.OPEN:
                    logger.error("熔断器处于 OPEN 状态，拦截本地发往主节点的请求。")
                    trigger_failover = True
                    break  # 跳出重试循环，直接放弃主节点，去激活灾备节点
                else:
                    # 如果只是瞬时并发超出了令牌桶限制，阻塞极短时间等待令牌
                    await asyncio.sleep(0.2)
                    continue

            # 2. 尝试执行真实的 API 网络请求
            try:
                # 把控制权暂时交还给原始函数（比如成员 B 的 chat_primary）
                result = await func(self, *args, **kwargs)

                # 3. 请求成功，上报网关重置状态机
                _gateway_breaker.record_success()
                return result

            except (RateLimitError, APIConnectionError, InternalServerError) as e:
                # 捕获网络级与限流级报错（不拦截业务级的 Prompt 错误）
                logger.warning(f"主节点网络异常 (Attempt {attempt}/{max_retries}): {e}")
                _gateway_breaker.record_failure()

                if attempt < max_retries:
                    # 计算带有抖动（Jitter）的防踩踏睡眠时间
                    delay = _gateway_breaker.get_exponential_backoff_time(attempt)
                    logger.info(f"触发指数退避，静默等待 {delay:.2f} 秒后重试...")
                    await asyncio.sleep(delay)
                else:
                    logger.error("达到最大重试次数，主节点（Primary Endpoint）宣告阵亡！")
                    trigger_failover = True
                    break
            except Exception as e:
                # 发生非网络类的其他异常，直接抛出给 Module 1 状态机处理
                raise e

        # ==========================================
        # 阶段二：异构灾备切拨逻辑 (Failover Routing)
        # ==========================================
        if trigger_failover and "backup" in _failover_routing:
            logger.critical("[异构灾备启动] 网关正在物理隔离层重定向流量至【备用节点】...")
            backup_conf = _failover_routing["backup"]

            # 提取 YAML 里的灾备配置
            new_base_url = backup_conf.get("base_url")
            new_model = backup_conf.get("model_name")
            new_api_key = os.getenv(backup_conf.get("api_key_env_var", ""), "")

            # 【核心架构魔法】动态改写底层的 HTTP 客户端与请求参数，无需重启系统！
            # 假设被装饰的是 LLMRouter 实例，它拥有 self.client 属性
            if hasattr(self, "client"):
                self.client.base_url = new_base_url
                self.client.api_key = new_api_key
                logger.info(f"底层网络连接已重定向至: {new_base_url}")

            # 拦截并篡改 kwargs 里的请求模型名
            if "model" in kwargs:
                kwargs["model"] = new_model
                logger.info(f"思考模型已强制热替换为: {new_model}")

            # 灾备节点接管后的最后一次殊死一搏
            try:
                logger.success("流量劫持完成！正在使用异构灾备引擎执行任务...")
                return await func(self, *args, **kwargs)
            except Exception as e:
                logger.error(f"致命打击：灾备节点同样宣告失败，全网瘫痪: {e}")
                raise RuntimeError("DataPlane Gateway: 所有主备算力通道均已物理断开。") from e

        # 如果主节点挂了且没配灾备，只能刚性报错退出
        raise RuntimeError("DataPlane Gateway: 主节点请求失败，未开启/未匹配到灾备路由。")

    return wrapper