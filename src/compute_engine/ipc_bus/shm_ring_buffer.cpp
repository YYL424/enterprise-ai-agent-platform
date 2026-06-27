/**
 * Enterprise AI Platform - Module 3 (底层数据血管)
 * Path: src/compute_engine/ipc_bus/shm_ring_buffer.cpp
 * Author: 成员 H
 * Description: 基于 POSIX 共享内存与无锁 CAS 雕刻的数据推流总线
 */

#include "shm_ring_buffer.hpp"
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <chrono>

namespace enterprise_ai {
namespace ipc_bus {

class IPCBusClient {
private:
    RingBufferHeader* header_ = nullptr;
    int shm_fd_ = -1;
    size_t total_size_ = 0;

public:
    IPCBusClient() = default;

    ~IPCBusClient() {
        if (header_ != nullptr && header_ != MAP_FAILED) {
            munmap(header_, total_size_);
        }
        if (shm_fd_ != -1) {
            close(shm_fd_);
        }
    }

    // 初始化/挂载物理共享内存
    void attach_shm(const char* shm_name, uint32_t max_slots) {
        // 计算总物理内存大小：Header大小 + (最大槽位数 * 4096)
        total_size_ = sizeof(RingBufferHeader) + (max_slots * sizeof(Slot));

        // 打开 POSIX 共享内存 (O_RDWR 读写权限)
        shm_fd_ = shm_open(shm_name, O_RDWR, 0666);
        if (shm_fd_ == -1) {
            throw std::runtime_error("FATAL: Failed to open shared memory. Has it been initialized by main process?");
        }

        // 内存映射 (mmap) 到当前进程的用户态虚拟空间
        void* addr = mmap(nullptr, total_size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
        if (addr == MAP_FAILED) {
            throw std::runtime_error("FATAL: Failed to mmap shared memory.");
        }

        header_ = static_cast<RingBufferHeader*>(addr);
    }

    // 沙箱端写入日志/Token的高频接口 (绝对的 0 锁，纯原子指令)
    bool write_payload(const uint8_t* data, uint32_t length) {
        if (length > Slot::PAYLOAD_CAPACITY) {
            return false; // 超出单槽位物理红线，直接拒收
        }

        uint32_t current_tail;
        uint32_t next_tail;
        
        // ------------------------------------------------------------------
        // 核心步骤 1：原子空间预留 (Atomic Reservation) 与 CAS 争抢
        // ------------------------------------------------------------------
        while (true) {
            // 使用 relaxed 语义，因为仅仅是加载当前值，不涉及多变量同步
            current_tail = header_->tail.load(std::memory_order_relaxed);
            next_tail = (current_tail + 1) % header_->max_slots;

            // 碰撞检测：检查是否撞上读指针 (队列满)
            // 使用 acquire 语义，确保能看到消费者(主进程)最新提交的 head
            if (next_tail == header_->head.load(std::memory_order_acquire)) {
                // 触发物理背压阀门 (对应 YAML 中的 HARD_INTERCEPT 策略)
                // 执行极其轻量的用户态自旋让出 CPU (50微秒)，强制拖慢沙箱的输出速度
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue; // 醒来后重新争抢
            }

            // CAS 原子争抢槽位：只有当 tail 依然等于 current_tail 时，才将其更新为 next_tail
            if (header_->tail.compare_exchange_weak(current_tail, next_tail, 
                                                    std::memory_order_acquire, 
                                                    std::memory_order_relaxed)) {
                break; // 争抢成功！当前沙箱成功独占了 current_tail 这个槽位
            }
        }

        // ------------------------------------------------------------------
        // 核心步骤 2：零拷贝物理覆写与状态机流转
        // ------------------------------------------------------------------
        Slot* slot = &header_->slots[current_tail];

        // 锁定控制头：标记我正在写，并写入自身 PID 供清洗器探测
        slot->status.store(STATUS_WRITING, std::memory_order_relaxed);
        slot->owner_pid = getpid();
        
        // 零拷贝直写物理内存
        std::memcpy(slot->payload, data, length);
        slot->payload_length = length;
        
        // 世代号推演 (用于清洗器辨别死锁旧状态)
        slot->epoch_version.fetch_add(1, std::memory_order_relaxed);

        // 终极指令：释放内存屏障 (Release Barrier)
        // 确保前面的 memcpy 操作绝对不会被 CPU 乱序执行排在状态修改之后
        // 只有当 status 变为 STATUS_READY 后，消费者才能看见完整的数据！
        slot->status.store(STATUS_READY, std::memory_order_release);

        return true;
    }
};

} // namespace ipc_bus
} // namespace enterprise_ai