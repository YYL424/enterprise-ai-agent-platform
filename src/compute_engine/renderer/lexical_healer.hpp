// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: lexical_healer.hpp — C++ 用户态 UTF-8 词法自愈状态机头文件
//
// 职责: 对传入字节流做 UTF-8 半包/粘包自愈，保证输出端始终在完整字符边界。
//       零异常、零动态分配、纯位运算。

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace enterprise_ai {
namespace renderer {

// ---------------------------------------------------------------------------
// PendingTail — 暂存被截断的 UTF-8 多字节尾部 (最多 3 字节)
// ---------------------------------------------------------------------------
struct PendingTail {
    uint8_t bytes[3];
    uint8_t len;  // 0–3, number of pending bytes

    PendingTail() noexcept : bytes{0, 0, 0}, len(0) {}

    void reset() noexcept {
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;
        len = 0;
    }
};

// ---------------------------------------------------------------------------
// LexicalHealer — UTF-8 词法自愈状态机
// ---------------------------------------------------------------------------
// 热路径函数全部 noexcept，零动态分配，无 try/catch，无 std::string。
class LexicalHealer {
public:
    LexicalHealer(size_t max_channels);
    ~LexicalHealer() noexcept = default;

    // Non-copyable, non-movable
    LexicalHealer(const LexicalHealer&) = delete;
    LexicalHealer& operator=(const LexicalHealer&) = delete;
    LexicalHealer(LexicalHealer&&) = delete;
    LexicalHealer& operator=(LexicalHealer&&) = delete;

    // ------------------------------------------------------------------
    // heal — UTF-8 词法自愈核心
    // ------------------------------------------------------------------
    // 1. 若 channel 有 pending 字节，prepend 到 input 逻辑前端。
    // 2. 从合并后的字节流末尾向前扫描，用位运算判定最后一个完整字符边界。
    // 3. 若末尾字符不完整，将残缺字节拷贝到该通道的 PendingTail，
    //    并从输出长度中剔除。
    // 4. output_len 保证末尾字节一定在完整 UTF-8 字符边界上。
    //
    // 返回 true 表示成功处理（即使有截断尾也是正常流程）。
    bool heal(uint32_t channel_id,
              const uint8_t* input,
              size_t input_len,
              uint8_t* output,
              size_t& output_len,
              PendingTail& out_pending) noexcept;

    // Reset the pending tail for a specific channel.
    void reset_channel(uint32_t channel_id) noexcept;

    // Statistics
    size_t total_heal_calls()  const noexcept { return total_heal_calls_; }
    size_t total_truncations() const noexcept { return total_truncations_; }

private:
    // Determine the byte-length of a UTF-8 sequence given its leading byte.
    // Returns 1, 2, 3, 4 for valid lead bytes; 1 for ASCII; 0 for continuation bytes.
    static uint8_t utf8_sequence_length(uint8_t lead_byte) noexcept;

    // Find the offset of the last complete UTF-8 character boundary in a buffer.
    // Scans backward from (buf + len - 1).
    // Returns the count of trailing incomplete bytes (0–3).
    static size_t find_last_complete_boundary(const uint8_t* buf, size_t len) noexcept;

    size_t max_channels_;
    std::vector<PendingTail> pending_tails_;

    size_t total_heal_calls_  = 0;
    size_t total_truncations_ = 0;
};

}  // namespace renderer
}  // namespace enterprise_ai
