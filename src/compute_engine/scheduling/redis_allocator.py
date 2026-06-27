# src/compute_engine/scheduling/redis_allocator.py
import redis
from loguru import logger
from typing import Tuple

class RedisSlotAllocator:
    """
    分布式槽位原子分配引擎 (成员D独占)
    基于单单线程 Redis Lua 脚本，杜绝高并发任务抢占导致的 Race Condition
    """
    def __init__(self, redis_client: redis.Redis, total_slots: int = 4):
        self.redis = redis_client
        self.total_slots = total_slots
        self.slots_key = "compute:gpu:available_slots"
        self.active_jobs_key = "compute:gpu:active_jobs"
        
        # 预设 RTX 5090 初始化槽位水位线 (例：24GB 划分为 4 个 Slot，每个 Slot 占 6GB 显存契约)
        if not self.redis.exists(self.slots_key):
            self.redis.set(self.slots_key, self.total_slots)

        # 🌟 核心攻坚：编写无锁高并发原子自旋 Lua 脚本
        self._lua_allocate = """
        local slots_key = KEYS[1]
        local active_jobs_key = KEYS[2]
        local job_id = ARGV[1]
        local required_slots = tonumber(ARGV[2])

        local current_slots = tonumber(redis.call('get', slots_key) or '0')
        if current_slots >= required_slots then
            -- 资源充裕，执行无碰撞扣减
            redis.call('decrby', slots_key, required_slots)
            redis.call('hset', active_jobs_key, job_id, required_slots)
            return 1 -- 代表分配成功
        else
            return 0 -- 代表槽位不足，需要进队列排队
        end
        """
        self.allocate_script = self.redis.register_script(self._lua_allocate)

    def try_atomic_allocate(self, job_id: str, required_slots: int) -> bool:
        """
        尝试一键原子性锁定虚拟显存槽位
        """
        try:
            # 执行 Lua 脚本，穿透至 Redis 内核执行
            result = self.allocate_script(
                keys=[self.slots_key, self.active_jobs_key],
                args=[job_id, required_slots]
            )
            status = bool(result)
            if status:
                logger.info(f"[Allocator] 任务 [{job_id}] 成功锁定 {required_slots} 个显存 Slot")
            return status
        except Exception as e:
            logger.error(f"[Allocator] Lua 扣减链路出现网络抖动: {str(e)}")
            return False

    def release_slots(self, job_id: str) -> int:
        """
        释放特定任务占用的显存槽位并归还水位
        """
        try:
            # 查出这个任务之前占了多少个 Slot
            allocated_slots = self.redis.hget(self.active_jobs_key, job_id)
            if not allocated_slots:
                return 0

            slots_to_free = int(allocated_slots)
            
            # 管道流水线安全归还
            pipe = self.redis.pipeline()
            pipe.incrby(self.slots_key, slots_to_free)
            pipe.hdel(self.active_jobs_key, job_id)
            pipe.execute()

            logger.info(f"[Allocator] 任务 [{job_id}] 执行完毕/被抢占，安全归还 {slots_to_free} 个 Slot")
            return slots_to_free
        except Exception as e:
            logger.error(f"[Allocator] 归还资源水位失败: {str(e)}")
            return 0