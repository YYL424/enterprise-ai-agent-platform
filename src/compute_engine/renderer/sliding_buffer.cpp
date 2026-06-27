// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: sliding_buffer.cpp — 固定内存、多通道、单向滑动缓冲区实现
//
// 运行期零动态内存分配。所有 buf 在构造函数内一次性分配。
// 热路径函数全部 noexcept。

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200112L  // Required for posix_memalign on Linux
#endif

#include "sliding_buffer.hpp"

#include <cstdlib>
#include <cstring>
#include <algorithm>

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// Constructor / Destructor
// ===========================================================================

SlidingBuffer::SlidingBuffer(size_t buffer_capacity_per_channel, size_t max_channels)
    : buffer_capacity_per_channel_(buffer_capacity_per_channel)
    , max_channels_(max_channels)
{
    channels_.reserve(max_channels_);
    epoch_anchors_.reserve(max_channels_);

    for (size_t i = 0; i < max_channels_; ++i) {
        ChannelSlot slot;
        slot.capacity = buffer_capacity_per_channel_;

        // posix_memalign: 64-byte aligned allocation
        int ret = posix_memalign(reinterpret_cast<void**>(&slot.buf),
                                 64,
                                 buffer_capacity_per_channel_);
        if (ret != 0 || slot.buf == nullptr) {
            // On allocation failure, set capacity to 0 so writes are no-ops
            slot.buf = nullptr;
            slot.capacity = 0;
        } else {
            // Zero-initialize the buffer
            std::memset(slot.buf, 0, buffer_capacity_per_channel_);
        }

        channels_.push_back(slot);

        // Pre-allocate anchor capacity (4096, no reallocation at runtime)
        std::vector<EpochAnchor> anchors;
        anchors.reserve(kMaxAnchors);
        epoch_anchors_.push_back(std::move(anchors));
    }
}

SlidingBuffer::~SlidingBuffer() noexcept {
    for (auto& channel : channels_) {
        if (channel.buf != nullptr) {
            std::free(channel.buf);
            channel.buf = nullptr;
        }
    }
}

// ===========================================================================
// write — 向指定通道写入数据 (热路径, noexcept)
// ===========================================================================

size_t SlidingBuffer::write(uint32_t channel_id,
                             const uint8_t* data,
                             size_t len,
                             uint64_t epoch,
                             uint64_t snapshot_id) noexcept
{
    if (channel_id >= channels_.size() || data == nullptr || len == 0) {
        return 0;
    }

    ChannelSlot& ch = channels_[channel_id];
    if (ch.buf == nullptr || ch.capacity == 0) {
        return 0;
    }

    const size_t cap = ch.capacity;

    // If data is larger than the entire buffer, only write the trailing portion
    if (len > cap) {
        data = data + (len - cap);
        len = cap;
    }

    // Check if write would overflow the ring buffer; if so, advance read_pos
    // to make room (oldest data is overwritten)
    size_t space_left = cap - ch.write_pos;
    if (len > space_left) {
        // Wrap case: write from write_pos to end, then from 0 to remaining
        size_t first_part = space_left;
        size_t second_part = len - first_part;

        std::memcpy(ch.buf + ch.write_pos, data, first_part);
        std::memcpy(ch.buf, data + first_part, second_part);

        ch.write_pos = second_part;
    } else {
        // No wrap: write directly at write_pos
        std::memcpy(ch.buf + ch.write_pos, data, len);
        ch.write_pos += len;
        if (ch.write_pos >= cap) {
            ch.write_pos = 0;  // wrap-around correction
        }
    }

    // Update pending tracking: advance read_pos if we overwrote unread data
    ch.pending_len = std::min(ch.pending_len + len, cap);
    ch.pending_off = (ch.write_pos >= ch.pending_len)
                         ? (ch.write_pos - ch.pending_len)
                         : (cap + ch.write_pos - ch.pending_len);

    // Update epoch anchor (track the write position after this write)
    auto& anchors = epoch_anchors_[channel_id];

    // Check if this epoch/snapshot already exists (update in place)
    bool found = false;
    for (auto& anchor : anchors) {
        if (anchor.epoch == epoch && anchor.snapshot_id == snapshot_id) {
            anchor.write_pos = ch.write_pos;
            found = true;
            break;
        }
    }

    if (!found) {
        // If we've hit the max, remove the oldest anchor
        if (anchors.size() >= kMaxAnchors) {
            anchors.erase(anchors.begin());
        }
        EpochAnchor anchor;
        anchor.epoch = epoch;
        anchor.snapshot_id = snapshot_id;
        anchor.write_pos = ch.write_pos;
        // channel_id is implicit: anchors are indexed by epoch_anchors_[channel_id]
        anchors.push_back(anchor);
    }

    total_bytes_written_ += len;
    return len;
}

// ===========================================================================
// read_slice — 非阻塞读取
// ===========================================================================

size_t SlidingBuffer::read_slice(uint32_t channel_id,
                                  size_t max_len,
                                  uint8_t* out,
                                  size_t& out_len) noexcept
{
    out_len = 0;

    if (channel_id >= channels_.size() || out == nullptr || max_len == 0) {
        return 0;
    }

    ChannelSlot& ch = channels_[channel_id];
    if (ch.buf == nullptr || ch.pending_len == 0) {
        return 0;
    }

    const size_t cap = ch.capacity;
    const size_t read_len = std::min(max_len, ch.pending_len);

    if (ch.pending_off + read_len <= cap) {
        // No wrap: direct copy
        std::memcpy(out, ch.buf + ch.pending_off, read_len);
    } else {
        // Wrap: two-part copy
        size_t first_part = cap - ch.pending_off;
        size_t second_part = read_len - first_part;
        std::memcpy(out, ch.buf + ch.pending_off, first_part);
        std::memcpy(out + first_part, ch.buf, second_part);
    }

    // Advance the read position
    ch.pending_off = (ch.pending_off + read_len) % cap;
    ch.pending_len -= read_len;
    ch.read_pos = ch.pending_off;

    out_len = read_len;
    total_bytes_read_ += read_len;
    return read_len;
}

// ===========================================================================
// rewind_to_epoch — 时间旅行回滚
// ===========================================================================

bool SlidingBuffer::rewind_to_epoch(uint32_t channel_id,
                                     uint64_t epoch,
                                     uint64_t snapshot_id) noexcept
{
    if (channel_id >= channels_.size()) {
        return false;
    }

    size_t anchor_idx = find_anchor(channel_id, epoch, snapshot_id);
    if (anchor_idx == SIZE_MAX) {
        return false;
    }

    ChannelSlot& ch = channels_[channel_id];
    if (ch.buf == nullptr) {
        return false;
    }

    auto& anchors = epoch_anchors_[channel_id];
    const EpochAnchor& target = anchors[anchor_idx];

    const size_t target_pos = target.write_pos;
    const size_t current_pos = ch.write_pos;
    const size_t cap = ch.capacity;

    // Zero out the memory region from target_pos to current_pos (physical erase)
    if (target_pos <= current_pos) {
        // Linear erase: target_pos → current_pos
        std::memset(ch.buf + target_pos, 0, current_pos - target_pos);
    } else {
        // Wrap-around erase: target_pos → end + 0 → current_pos
        std::memset(ch.buf + target_pos, 0, cap - target_pos);
        std::memset(ch.buf, 0, current_pos);
    }

    // Roll back write position
    ch.write_pos = target_pos;

    // Reset read state:
    //   pending_len = 0 — 回滚后无"未读"数据。
    //   回滚点之前的旧数据已被 consume（或视为历史快照，不重新暴露给 read_slice）。
    //   新写入从 target_pos 开始正向追加，形成新时间线分支。
    //   如需读取旧数据，调用方应在回滚前通过 read_slice 主动排空。
    ch.pending_off = target_pos;
    ch.pending_len = 0;
    ch.read_pos = target_pos;

    // Remove all anchors at and after the target index
    // Keep anchors up to and including the target
    anchors.resize(anchor_idx + 1);

    total_rewinds_++;
    return true;
}

// ===========================================================================
// get_epoch_anchor_count
// ===========================================================================

size_t SlidingBuffer::get_epoch_anchor_count(uint32_t channel_id) const noexcept {
    if (channel_id >= epoch_anchors_.size()) {
        return 0;
    }
    return epoch_anchors_[channel_id].size();
}

// ===========================================================================
// find_anchor (private)
// ===========================================================================

size_t SlidingBuffer::find_anchor(uint32_t channel_id,
                                   uint64_t epoch,
                                   uint64_t snapshot_id) const noexcept
{
    if (channel_id >= epoch_anchors_.size()) {
        return SIZE_MAX;
    }

    const auto& anchors = epoch_anchors_[channel_id];
    // Linear scan from newest to oldest (most common case: recent epoch)
    for (size_t i = anchors.size(); i > 0; --i) {
        size_t idx = i - 1;
        if (anchors[idx].epoch == epoch && anchors[idx].snapshot_id == snapshot_id) {
            return idx;
        }
    }
    return SIZE_MAX;
}

}  // namespace renderer
}  // namespace enterprise_ai
