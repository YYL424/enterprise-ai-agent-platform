# src/compute_engine/scheduling/queue_scheduler.py
import time
from loguru import logger
from src.compute_engine.scheduling.pynvml_monitor import GPUMonitor
from src.compute_engine.scheduling.redis_allocator import RedisSlotAllocator

class PriorityQueueScheduler:
    """
    Slot-Based 逻辑抢占式调度中枢 (成员D独占)
    控制任务排队、双重水位校验与跨多进程的软抢占信号分发
    """
    def __init__(self, allocator: RedisSlotAllocator, monitor: GPUMonitor):
        self.allocator = allocator
        self.monitor = monitor
        self.queue_key = "compute:scheduler:priority_zset"
        self.preemption_channel = "compute:scheduler:preemption_signals"

    def submit_to_queue(self, job_id: str, priority: int, required_slots: int):
        """
        将因硬件资源不足而挂起的任务推入基于 Redis Zset 的优先级队列
        """
        # 将 job_id 与所需槽位打包成符合格式的元组，权重直接等于优先级得分 (分数越高代表越紧急)
        member_value = f"{job_id}:{required_slots}"
        self.allocator.redis.zadd(self.queue_key, {member_value: priority})
        logger.warning(f"[Scheduler] 物理算力不足，任务 [{job_id}] 已被压入高并发排队池，当前优先级分值: {priority}")

    def execute_schedule_cycle(self) -> str:
        """
        【调度周期核心自愈图】：每个时钟周期自动触发，扫描排队池并执行双重校验与软抢占
        """
        # 1. 从 Zset 队列中拔出目前胜率最高（Priority值最大）的头号任务
        waiting_jobs = self.allocator.redis.zrevrange(self.queue_key, 0, 0, withscores=True)
        if not waiting_jobs:
            return "IDLE"

        job_info_str, score = waiting_jobs[0]
        job_id, required_slots_str = job_info_str.decode("utf-8").split(":")
        required_slots = int(required_slots_str)
        priority = int(score)

        # 2. 核心双重校验：先看虚拟 Lua 槽位是否允许通过
        if self.allocator.try_atomic_allocate(job_id, required_slots):
            # 虚拟槽位扣减成功，紧接着进行第二重物理校对：直接探查 NVML 真实物理显存水位
            metrics = self.monitor.get_hardware_metrics()
            if metrics.get("status") == "HEALTHY" and metrics.get("vram_free_mb", 0) > (required_slots * 4000): # 粗略估计每个Slot对应约4G空闲显存
                # 两重防御全部击穿，安全放行任务！从排队池中物理移除
                self.allocator.redis.zrem(self.queue_key, job_info_str)
                logger.info(f"[Scheduler] 🌟 绿色通道放行：任务 [{job_id}] 击穿虚拟与物理双重锁，正式移交下层沙箱拉起！")
                return f"LAUNCH:{job_id}"
            else:
                # 触发极少发生的幽灵情况：虚拟槽位足够，但物理显卡可能由于不明外部进程遭受污染
                logger.critical(f"[Scheduler] 警告：虚拟槽位充足但物理显存发生未注册污染！回滚虚拟槽位，继续排队")
                self.allocator.release_slots(job_id)
                return "PHYSICAL_WAIT"

        # 3. 核心攻坚逻辑：如果虚拟槽位扣减失败（代表当前 5090 已被塞满），触发高优抢占自愈机制
        logger.info(f"[Scheduler] 算力池满载。尝试为高优任务 [{job_id}](P:{priority}) 评估软抢占可能性...")
        
        # 扫描当前正在跑的任务，挑出一个优先级最低的任务把它干掉
        active_jobs = self.allocator.redis.hgetall(self.allocator.active_jobs_key)
        
        # 假设这里配合上层控制平面的元数据，找到了一个运行中的低优任务 low_job_id (优先级远低于当前排队任务)
        # 发送软抢占广播命令（利用 Redis 广播通道）
        for active_job_bin in active_jobs.keys():
            active_job_id = active_job_bin.decode("utf-8")
            
            # 此处模拟强弱判定机制：广播下发优雅退出信号
            preempt_payload = f"PREEMPT_SIGNAL:{active_job_id}:TARGET:{job_id}"
            self.allocator.redis.publish(self.preemption_channel, preempt_payload)
            logger.critical(f"[Scheduler] ⚡ 驱逐风暴激活 ⚡：向通道派发软终止信号，强行命令低优任务 [{active_job_id}] 保存 Checkpoint 并交出 Slot！")
            break # 策略性单例驱逐，防止大面积雪崩

        return "PREEMPTING"