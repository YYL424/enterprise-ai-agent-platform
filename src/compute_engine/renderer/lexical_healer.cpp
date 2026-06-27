// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: lexical_healer.cpp — UTF-8 词法自愈状态机实现
//
// 零异常、零动态分配、纯位运算。
// 无 try/catch、无 std::string、无堆分配。

#include "lexical_healer.hpp"

#include <cstring>
#include <algorithm>

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// Constructor
// ===========================================================================

LexicalHealer::LexicalHealer(size_t max_channels)
    : max_channels_(max_channels)
{
    pending_tails_.resize(max_channels_);
    // Default-constructed PendingTail has len=0 (no pending state)
}

// ===========================================================================
// heal — UTF-8 词法自愈核心
// ===========================================================================

bool LexicalHealer::heal(uint32_t channel_id,
                          const uint8_t* input,
                          size_t input_len,
                          uint8_t* output,
                          size_t& output_len,
                          PendingTail& out_pending) noexcept
{
    total_heal_calls_++;

    if (channel_id >= max_channels_ || output == nullptr) {
        output_len = 0;
        return false;
    }

    // Zero-input edge case: flush pending tail only
    if (input == nullptr || input_len == 0) {
        // If we have pending bytes, they represent a complete truncated tail
        // that could not be resolved. Copy them out as-is.
        PendingTail& pending = pending_tails_[channel_id];
        if (pending.len > 0) {
            std::memcpy(output, pending.bytes, pending.len);
            output_len = pending.len;
            out_pending = pending;
            pending.reset();
            total_truncations_++;
            return true;
        }
        output_len = 0;
        out_pending.reset();
        return true;
    }

    PendingTail& pending = pending_tails_[channel_id];
    size_t total_input = pending.len + input_len;

    // Step 1: Prepend pending bytes to output head
    if (pending.len > 0) {
        std::memcpy(output, pending.bytes, pending.len);
        std::memcpy(output + pending.len, input, input_len);
    } else {
        std::memcpy(output, input, input_len);
    }

    // Step 2: Find the last complete UTF-8 character boundary
    size_t trailing = find_last_complete_boundary(output, total_input);

    // Step 3: Save incomplete tail bytes to pending
    if (trailing > 0) {
        // trailing bytes are at the end of the merged buffer
        const uint8_t* tail_start = output + total_input - trailing;
        pending.len = static_cast<uint8_t>(trailing);
        std::memcpy(pending.bytes, tail_start, trailing);
        output_len = total_input - trailing;
        total_truncations_++;
    } else {
        // No truncation — clear pending
        pending.reset();
        output_len = total_input;
    }

    out_pending = pending;
    return true;
}

// ===========================================================================
// reset_channel
// ===========================================================================

void LexicalHealer::reset_channel(uint32_t channel_id) noexcept {
    if (channel_id < max_channels_) {
        pending_tails_[channel_id].reset();
    }
}

// ===========================================================================
// utf8_sequence_length (static)
// ===========================================================================

uint8_t LexicalHealer::utf8_sequence_length(uint8_t lead_byte) noexcept {
    // Continuation bytes (10xxxxxx) — not a lead byte
    if ((lead_byte & 0xC0) == 0x80) {
        return 0;
    }
    // 1-byte sequence (0xxxxxxx)
    if ((lead_byte & 0x80) == 0x00) {
        return 1;
    }
    // 2-byte sequence (110xxxxx)
    if ((lead_byte & 0xE0) == 0xC0) {
        return 2;
    }
    // 3-byte sequence (1110xxxx)
    if ((lead_byte & 0xF0) == 0xE0) {
        return 3;
    }
    // 4-byte sequence (11110xxx)
    if ((lead_byte & 0xF8) == 0xF0) {
        return 4;
    }
    // Invalid lead byte — treat as 1-byte
    return 1;
}

// ===========================================================================
// find_last_complete_boundary (static)
// ===========================================================================

size_t LexicalHealer::find_last_complete_boundary(const uint8_t* buf, size_t len) noexcept {
    if (len == 0) return 0;

    // Start from the last byte
    const uint8_t* p = buf + len - 1;
    size_t trailing = 0;

    // If the last byte is ASCII (0xxxxxxx), it's always complete
    if ((*p & 0x80) == 0x00) {
        return 0;
    }

    // Scan backward to find the lead byte of the last sequence
    while (p >= buf) {
        if ((*p & 0xC0) == 0x80) {
            // Continuation byte — count it and go back
            trailing++;
            if (p == buf) {
                // Reached start of buffer with only continuation bytes
                // This means all bytes are continuation bytes → treat all as trailing
                return len;
            }
            p--;
        } else {
            // Found a lead byte (or ASCII)
            uint8_t seq_len = utf8_sequence_length(*p);
            if (seq_len == 0) {
                // This is also a continuation byte somehow — treat as trailing
                trailing++;
                p--;
                continue;
            }

            // Check if the sequence is complete
            // seq_len includes the lead byte itself
            // trailing is the number of continuation bytes already counted
            // So we need (seq_len - 1) continuation bytes after the lead byte
            if (trailing >= seq_len - 1) {
                // Sequence is complete — no truncation
                return 0;
            } else {
                // Sequence is incomplete — all bytes from the lead byte onward
                // are trailing
                return trailing + 1;
            }
        }
    }

    // Should not reach here, but if we do: treat everything as trailing
    return len;
}

}  // namespace renderer
}  // namespace enterprise_ai
