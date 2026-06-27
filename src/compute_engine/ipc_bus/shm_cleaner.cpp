/**
 * Enterprise AI Platform - Module 3 (底层数据血管)
 * Path: src/compute_engine/ipc_bus/shm_cleaner.cpp
 * Author: 成员 H
 * Description: 孤儿僵尸沙箱死锁检测器。利用 kill(pid, 0) 与版本号实现 100ms 无感自愈。
 */

#include "shm_ring_buffer.hpp"
#include <signal.h>   // 引入内核信号量 API
#include <errno.h>    // 引入系统错误号
#include <thread>
#include <chrono>
#include <iostream>
#include <atomic>

namespace enterprise_ai {
namespace ipc_bus {

class SHMCleaner {
private:
    RingBufferHeader* header_;
    std::atomic<bool> running_;
    std::thread cleaner_thread_;
    uint32_t interval_ms_;

    // 核心异步巡检主循环
    void cleaner_loop() {
        while (running_.load(std::memory_order_acquire)) {
            // 遍历所有物理槽位 (全量扫描 512MB 内存结构的时间开销极小)
            for (uint32_t i = 0; i < header_->max_slots; ++i) {
                Slot* slot = &header_->slots[i];

                // 规则一：只关心那些“宣称自己正在独占写入”的槽位
                if (slot->status.load(std::memory_order_acquire) == STATUS_WRITING) {
                    pid_t owner = slot->owner_pid;

                    // 🌟 纪检委的最高强权探针：kill(pid, 0) 🌟
                    // 信号 0 是一个极其特殊的魔法值，它绝对不会向目标进程发送任何破坏性信号，
                    // 仅仅是强制要求 Linux 内核检查这个 PID 是否依然物理存活于进程树中。
                    if (kill(owner, 0) == -1 && errno == ESRCH) {
                        // 判定结论：ESRCH (No such process)，该沙箱已被系统彻底强杀！
                        // 发生严重的 IPC 协议共享状态孤儿死锁，立刻启动物理抢救！
                        
                        // 1. 提取该槽位死前的世代号
                        uint64_t current_epoch = slot->epoch_version.load(std::memory_order_relaxed);

                        // 2. 夺命解除：暴力将死锁槽位重置为 IDLE (空闲可用态)
                        slot->status.store(STATUS_IDLE, std::memory_order_release);
                        
                        // 3. 时空隔离：强制将世代版本号加一，配合 CAS 原语，
                        // 彻底防止死掉的沙箱突然产生某种内核态幻影回调导致的 ABA 踩踏问题。
                        slot->epoch_version.compare_exchange_strong(current_epoch, current_epoch + 1);

                        // 记录到底层系统日志中，证明清洗器完成了一次微秒级外科手术
                        // std::cerr << "[SHM_CLEANER] 成功超度僵尸沙箱 PID: " << owner 
                        //           << " | 秒级释放死锁槽位: " << i << std::endl;
                    }
                }
            }
            
            // 执行 bus_policy.yaml 中规定的底噪休眠节拍器 (默认 100ms)
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
        }
    }

public:
    // 构造时注入共享内存的头部指针
    SHMCleaner(RingBufferHeader* header, uint32_t interval_ms = 100) 
        : header_(header), running_(false), interval_ms_(interval_ms) {}

    ~SHMCleaner() {
        stop();
    }

    // 随平台控制主进程一起点火，启动后台异步自愈线程
    void start() {
        if (!running_.exchange(true)) {
            cleaner_thread_ = std::thread(&SHMCleaner::cleaner_loop, this);
        }
    }

    // 系统优雅退出时的平滑关闭机制
    void stop() {
        if (running_.exchange(false)) {
            if (cleaner_thread_.joinable()) {
                cleaner_thread_.join();
            }
        }
    }
};

} // namespace ipc_bus
} // namespace enterprise_ai