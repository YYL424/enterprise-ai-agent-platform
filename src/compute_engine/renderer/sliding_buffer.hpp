// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: sliding_buffer.hpp — 固定内存、多通道、单向滑动缓冲区头文件
//
// 职责: 渲染器热路径核心数据结构。初始化时一次性分配所有内存，
//       运行期仅指针移动与就地覆写，零动态分配。

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace enterprise_ai {
namespace renderer {

// ---------------------------------------------------------------------------
// EpochAnchor — 记录每个 (epoch, snapshot_id) 落入通道时的物理写入位置
// ---------------------------------------------------------------------------
struct EpochAnchor {
    uint64_t epoch;        // Graph epoch (from Module 1 control plane)
    uint64_t snapshot_id;  // Node snapshot identifier
    size_t   write_pos;    // Physical byte offset in channel buffer
};

// ---------------------------------------------------------------------------
// ChannelSlot — 单通道缓冲区描述符 (所有权在 SlidingBuffer)
// ---------------------------------------------------------------------------
struct ChannelSlot {
    uint8_t* buf;          // 64-byte aligned, allocated once via posix_memalign
    size_t   capacity;     // Total buffer size in bytes
    size_t   write_pos;    // Next byte offset to write into
    size_t   read_pos;     // debug/stat only, not used in hot path (pending_off drives reads)
    size_t   pending_off;  // Offset where pending (unread) data starts
    size_t   pending_len;  // Length of pending data available to read

    ChannelSlot() noexcept
        : buf(nullptr)
        , capacity(0)
        , write_pos(0)
        , read_pos(0)
        , pending_off(0)
        , pending_len(0) {}
};

// ---------------------------------------------------------------------------
// SlidingBuffer — 固定内存、多通道、单向滑动缓冲区
// ---------------------------------------------------------------------------
// 线程安全: 由 RendererEngine 单线程串行访问，内部不加锁。
class SlidingBuffer {
public:
    SlidingBuffer(size_t buffer_capacity_per_channel, size_t max_channels);
    ~SlidingBuffer() noexcept;

    // Non-copyable, non-movable (owns raw buffers)
    SlidingBuffer(const SlidingBuffer&) = delete;
    SlidingBuffer& operator=(const SlidingBuffer&) = delete;
    SlidingBuffer(SlidingBuffer&&) = delete;
    SlidingBuffer& operator=(SlidingBuffer&&) = delete;

    // ------------------------------------------------------------------
    // write — 向指定通道写入数据
    // ------------------------------------------------------------------
    // 若剩余空间不足，环形覆写最旧数据。
    // 写入后自动更新 EpochAnchor 表。
    // 返回实际写入字节数。
    size_t write(uint32_t channel_id,
                 const uint8_t* data,
                 size_t len,
                 uint64_t epoch,
                 uint64_t snapshot_id) noexcept;

    // ------------------------------------------------------------------
    // read_slice — 非阻塞读取
    // ------------------------------------------------------------------
    // 从指定通道读取最多 max_len 字节到 out 缓冲区。
    // out_len 设置为实际读取的字节数。
    // 返回值为实际读取的字节数（与 out_len 相同）。
    size_t read_slice(uint32_t channel_id,
                      size_t max_len,
                      uint8_t* out,
                      size_t& out_len) noexcept;

    // ------------------------------------------------------------------
    // rewind_to_epoch — 时间旅行回滚
    // ------------------------------------------------------------------
    // 将通道的 write_pos 回滚到目标 epoch/snapshot 记录的位置。
    // 回滚区域用 std::memset 清零（物理擦除）。
    // 同步清理该 epoch 之后的所有 Anchor 记录。
    // 返回 true 表示找到并回滚成功。
    bool rewind_to_epoch(uint32_t channel_id,
                         uint64_t epoch,
                         uint64_t snapshot_id) noexcept;

    // ------------------------------------------------------------------
    // get_epoch_anchor_count — 获取通道的 Anchor 数量
    // ------------------------------------------------------------------
    size_t get_epoch_anchor_count(uint32_t channel_id) const noexcept;

    // ------------------------------------------------------------------
    // Statistics getters
    // ------------------------------------------------------------------
    size_t total_bytes_written() const noexcept { return total_bytes_written_; }
    size_t total_bytes_read()    const noexcept { return total_bytes_read_; }
    size_t total_rewinds()       const noexcept { return total_rewinds_; }

private:
    // Locate the anchor index for (epoch, snapshot_id). Returns SIZE_MAX if not found.
    size_t find_anchor(uint32_t channel_id, uint64_t epoch, uint64_t snapshot_id) const noexcept;

    // ---- data members ----
    size_t buffer_capacity_per_channel_;
    size_t max_channels_;
    std::vector<ChannelSlot>           channels_;
    std::vector<std::vector<EpochAnchor>> epoch_anchors_;

    static constexpr size_t kMaxAnchors = 4096;

    size_t total_bytes_written_ = 0;
    size_t total_bytes_read_    = 0;
    size_t total_rewinds_       = 0;
};

}  // namespace renderer
}  // namespace enterprise_ai
