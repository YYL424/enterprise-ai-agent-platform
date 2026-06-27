// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: differential_layer.hpp — 差分图层引擎头文件
//
// 职责: 与 Module 1 Time Travel 对齐。将时间旅行请求转换为
//       SlidingBuffer 物理回滚 + EpollReactor 控制帧广播。

#pragma once

#include <cstddef>
#include <cstdint>

namespace enterprise_ai {
namespace renderer {

// Forward declarations
class SlidingBuffer;
class EpollReactor;

// ---------------------------------------------------------------------------
// DifferentialLayer — 差分图层引擎
// ---------------------------------------------------------------------------
class DifferentialLayer {
public:
    // Constructor injection: holds non-owning pointers to external components.
    DifferentialLayer(SlidingBuffer* buffer, EpollReactor* reactor) noexcept
        : sliding_buffer_(buffer)
        , epoll_reactor_(reactor) {}

    ~DifferentialLayer() noexcept = default;

    // Non-copyable, non-movable (holds pointers to external state)
    DifferentialLayer(const DifferentialLayer&) = delete;
    DifferentialLayer& operator=(const DifferentialLayer&) = delete;
    DifferentialLayer(DifferentialLayer&&) = delete;
    DifferentialLayer& operator=(DifferentialLayer&&) = delete;

    // ------------------------------------------------------------------
    // reset_to_epoch — 时间旅行回滚入口
    // ------------------------------------------------------------------
    // 1. 调用 sliding_buffer_->rewind_to_epoch(epoch, snapshot_id)
    // 2. 构造 8 字节 OPCODE_RESET_TO_EPOCH 帧
    // 3. 调用 epoll_reactor_->broadcast(reset_frame, 8)
    //
    // 返回 true 表示 epoch 存在且回滚成功。
    // 若 epoch 不存在，返回 false，不广播。
    bool reset_to_epoch(uint32_t channel_id,
                        uint64_t epoch,
                        uint64_t snapshot_id) noexcept;

    // ------------------------------------------------------------------
    // get_epoch_anchor_count — 供测试验证
    // ------------------------------------------------------------------
    size_t get_epoch_anchor_count(uint32_t channel_id) const noexcept;

    // ------------------------------------------------------------------
    // Statistics
    // ------------------------------------------------------------------
    size_t total_resets() const noexcept { return total_resets_; }

private:
    SlidingBuffer* sliding_buffer_;
    EpollReactor*  epoll_reactor_;
    size_t         total_resets_ = 0;
};

}  // namespace renderer
}  // namespace enterprise_ai
