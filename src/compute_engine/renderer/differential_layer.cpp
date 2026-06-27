// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: differential_layer.cpp — 差分图层引擎实现
//
// 职责: 将 Module 1 Time Travel 请求转换为 SlidingBuffer 物理回滚
//       + 8 字节 RESET 控制帧广播到所有客户端。

#include "differential_layer.hpp"
#include "sliding_buffer.hpp"
#include "epoll_reactor.hpp"
#include "msgpack_encoder.hpp"

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// reset_to_epoch — 时间旅行回滚入口
// ===========================================================================

bool DifferentialLayer::reset_to_epoch(uint32_t channel_id,
                                        uint64_t epoch,
                                        uint64_t snapshot_id) noexcept
{
    if (sliding_buffer_ == nullptr || epoll_reactor_ == nullptr) {
        return false;
    }

    // Step 1: Physically rewind the sliding buffer
    bool rewind_ok = sliding_buffer_->rewind_to_epoch(channel_id, epoch, snapshot_id);
    if (!rewind_ok) {
        // Epoch not found — do not broadcast
        return false;
    }

    // Step 2: Construct 8-byte OPCODE_RESET_TO_EPOCH control frame
    uint8_t reset_frame[8];
    MsgPackEncoder::encode_reset_frame(epoch, reset_frame);

    // Step 3: Broadcast reset frame to all connected clients
    epoll_reactor_->broadcast(reset_frame, 8);

    total_resets_++;
    return true;
}

// ===========================================================================
// get_epoch_anchor_count
// ===========================================================================

size_t DifferentialLayer::get_epoch_anchor_count(uint32_t channel_id) const noexcept {
    if (sliding_buffer_ == nullptr) {
        return 0;
    }
    return sliding_buffer_->get_epoch_anchor_count(channel_id);
}

}  // namespace renderer
}  // namespace enterprise_ai
