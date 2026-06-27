/**
 * Enterprise AI Platform - Module 3 (底层数据血管)
 * Path: src/compute_engine/ipc_bus/shm_ring_buffer.hpp
 * Author: 成员 H
 * Description: 无锁队列控制头（Head/Tail 指针）与物理槽位（Slot）映射结构体。
 *              基于 POSIX 共享内存，完全屏蔽内核互斥锁，利用 CAS 实现纳秒级 IPC。
 */

#ifndef SHM_RING_BUFFER_HPP
#define SHM_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <sys/types.h>

namespace enterprise_ai {
namespace ipc_bus {

// ============================================================================
// 1. 槽位状态机枚举 (底层采用 8 位整型，极致压缩内存)
// ============================================================================
enum SlotStatus : uint8_t {
    STATUS_IDLE    = 0, // 空闲状态：消费者已读完，等待沙箱写入
    STATUS_WRITING = 1, // 独占状态：沙箱正在写入，CAS 锁定中
    STATUS_READY   = 2  // 就绪状态：沙箱已写完，等待控制主进程消费
};

// ============================================================================
// 2. 物理槽位 (Slot) 结构体定义
// ============================================================================
// alignas(4096) 确保每个槽位完美对齐 Linux 的物理内存页 (Page Size)
// 这将彻底杜绝跨页读写引发的硬缺页中断 (Hard Page Fault)
struct alignas(4096) Slot {
    // ----------------- 控制头 (Control Header: 20 Bytes) -----------------
    std::atomic<uint64_t> epoch_version; // 8 字节 (Offset: 0)
    
    std::atomic<uint8_t>  status;        // 1 字节 (Offset: 8)
                                         // 编译器隐式 Padding 3 字节 (Offset: 9~11)
                                         
    int32_t               owner_pid;     // 4 字节 (Offset: 12) -> 替换 pid_t，尺寸绝对恒定
    
    uint32_t              payload_length;// 4 字节 (Offset: 16)
    
    // ----------------- 数据区 (Payload Area) -----------------
    // 显式定义数据区容量: 4096 - 20 (Header总占用) = 4076
    static constexpr size_t PAYLOAD_CAPACITY = 4076;
    
    uint8_t               payload[PAYLOAD_CAPACITY];
};

// 🌟 终极物理防御契约 (编译期强制拦截) 🌟
// 只要编译器试图在这里乱加 Padding，导致整体大小偏离 4096 字节 (4KB)，
// 编译时会直接爆出红字 FATAL 错误，绝对不把隐患带到运行时！
static_assert(sizeof(Slot) == 4096, 
              "FATAL: Slot structure size MUST be exactly 4096 bytes for Linux Page alignment!");
// ============================================================================
// 3. 无锁环形队列全局控制块 (Ring Buffer Control Block)
// ============================================================================
// alignas(64) 确保读写指针分布在不同的 CPU 缓存行 (Cache Line) 上
// 彻底解决多核并发下的 MESI 协议伪共享 (False Sharing) 灾难
struct RingBufferHeader {
    // 消费者指针 (控制主进程读取位置)
    alignas(64) std::atomic<uint32_t> head;
    
    // 生产者指针 (沙箱写入位置)
    alignas(64) std::atomic<uint32_t> tail;
    
    // 共享内存总容量元数据 (用于跨进程双向校验)
    uint32_t max_slots;
    uint32_t slot_size;
    
    // 占位填充，确保后面的 Slot 数组首地址也能对齐到 4096 边界
    uint8_t padding[4096 - 128 - 8]; 
    
    // 零长数组 (Flexible Array Member) 语法技巧
    // 实际的 Slots 物理内存将紧贴着 Header 在 /dev/shm 中连续展开
    Slot slots[0]; 
};

} // namespace ipc_bus
} // namespace enterprise_ai

#endif // SHM_RING_BUFFER_HPP