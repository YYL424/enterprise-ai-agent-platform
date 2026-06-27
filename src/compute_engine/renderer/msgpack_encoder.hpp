// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: msgpack_encoder.hpp — 轻量级二进制分帧编码器 (header-only)
//
// 职责: 手写帧头，不引入外部 msgpack 库。所有编码用纯位运算与 htobe64/htobe32。
//       零动态分配。大端序（网络字节序）。

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <endian.h>

namespace enterprise_ai {
namespace renderer {

// ---------------------------------------------------------------------------
// Frame format constants
// ---------------------------------------------------------------------------

// Magic bytes
constexpr uint8_t  MAGIC_BYTE_0 = 0xCE;
constexpr uint8_t  MAGIC_BYTE_1 = 0xFA;

// Opcodes
constexpr uint8_t  OPCODE_DATA          = 0x00;
constexpr uint8_t  OPCODE_RESET_TO_EPOCH = 0x01;

// Frame sizes
// Data frame header: Magic(2) + Channel_ID(4) + Payload_Length(4) +
// Graph_Epoch(8) + Node_Snapshot_ID(8) + Opcode(1) = 27 bytes
constexpr size_t   DATA_FRAME_HEADER_SIZE = 27;
constexpr size_t   RESET_FRAME_SIZE       = 8;
// Max payload for encoding: output_buf_size - DATA_FRAME_HEADER_SIZE
// (实际限制由调用方的 max_buf 参数决定，此常量仅供文档参考)
constexpr size_t   MAX_PAYLOAD_SIZE       = 65536 - DATA_FRAME_HEADER_SIZE;  // 65509

// ---------------------------------------------------------------------------
// ResetFrame — RESET_TO_EPOCH 控制帧布局 (验收指标 12: 严格 8 字节)
// ---------------------------------------------------------------------------
// 编译期断言保证帧大小精确为 8 字节。
struct ResetFrame {
    uint8_t magic;      // Byte 0: 0xCE
    uint8_t opcode;     // Byte 1: 0x01
    uint8_t epoch[6];   // Bytes 2-7: Graph_Epoch (48-bit big-endian)
};
static_assert(sizeof(ResetFrame) == 8, "RESET frame must be exactly 8 bytes");

// ---------------------------------------------------------------------------
// MsgPackEncoder — 二进制分帧编码器 (全静态方法)
// ---------------------------------------------------------------------------
class MsgPackEncoder {
public:
    // ------------------------------------------------------------------
    // encode_data_frame — 编码数据帧
    // ------------------------------------------------------------------
    // 帧格式 (大端序):
    //   Magic(2B):        0xCE 0xFA
    //   Channel_ID(4B):   uint32
    //   Payload_Length(4B): uint32
    //   Graph_Epoch(8B):  uint64
    //   Node_Snapshot_ID(8B): uint64
    //   Opcode(1B):       0x00 = DATA
    //   Payload(N B):     原始文本字节
    //
    // out:       输出缓冲区
    // out_len:   实际写入字节数
    // max_buf:   out 缓冲区最大容量
    //
    // 返回 true 表示编码成功。
    static bool encode_data_frame(uint32_t channel_id,
                                  const uint8_t* payload,
                                  size_t payload_len,
                                  uint64_t epoch,
                                  uint64_t snapshot_id,
                                  uint8_t* out,
                                  size_t& out_len,
                                  size_t max_buf) noexcept;

    // ------------------------------------------------------------------
    // encode_reset_frame — 编码 RESET_TO_EPOCH 控制帧 (严格 8 字节)
    // ------------------------------------------------------------------
    // 帧格式:
    //   Byte 0:    Magic = 0xCE
    //   Byte 1:    Opcode = 0x01
    //   Byte 2-7:  Graph_Epoch (48-bit, big-endian, 6 bytes)
    //
    // out: 至少 8 字节的输出缓冲区
    static void encode_reset_frame(uint64_t epoch, uint8_t out[8]) noexcept;

    // Helper: write uint64 as big-endian into buffer at offset
    static inline void write_be64(uint8_t* buf, size_t offset, uint64_t val) noexcept {
        uint64_t be = htobe64(val);
        std::memcpy(buf + offset, &be, sizeof(be));
    }

    // Helper: write uint32 as big-endian into buffer at offset
    static inline void write_be32(uint8_t* buf, size_t offset, uint32_t val) noexcept {
        uint32_t be = htobe32(val);
        std::memcpy(buf + offset, &be, sizeof(be));
    }

    // Helper: write uint48 (lower 48 bits of uint64) as big-endian into buffer at offset.
    // htobe64 produces 8 bytes in big-endian; +2 skips the upper 2 bytes (MSB),
    // copying the lower 48 bits (bytes 2-7) into the output.
    static inline void write_be48(uint8_t* buf, size_t offset, uint64_t val) noexcept {
        uint64_t be = htobe64(val);
        std::memcpy(buf + offset, reinterpret_cast<const uint8_t*>(&be) + 2, 6);
    }

private:
    // Static class only — no instances
    MsgPackEncoder() = delete;
};

// ===========================================================================
// Inline implementations
// ===========================================================================

inline bool MsgPackEncoder::encode_data_frame(
    uint32_t channel_id,
    const uint8_t* payload,
    size_t payload_len,
    uint64_t epoch,
    uint64_t snapshot_id,
    uint8_t* out,
    size_t& out_len,
    size_t max_buf) noexcept
{
    const size_t total_len = DATA_FRAME_HEADER_SIZE + payload_len;
    if (total_len > max_buf) {
        out_len = 0;
        return false;
    }

    size_t off = 0;

    // Magic (2B)
    out[off++] = MAGIC_BYTE_0;
    out[off++] = MAGIC_BYTE_1;

    // Channel_ID (4B)
    write_be32(out, off, channel_id);
    off += 4;

    // Payload_Length (4B)
    write_be32(out, off, static_cast<uint32_t>(payload_len));
    off += 4;

    // Graph_Epoch (8B)
    write_be64(out, off, epoch);
    off += 8;

    // Node_Snapshot_ID (8B)
    write_be64(out, off, snapshot_id);
    off += 8;

    // Opcode (1B)
    out[off++] = OPCODE_DATA;

    // Payload (N B)
    if (payload_len > 0 && payload != nullptr) {
        std::memcpy(out + off, payload, payload_len);
    }
    off += payload_len;

    out_len = off;
    return true;
}

inline void MsgPackEncoder::encode_reset_frame(uint64_t epoch, uint8_t out[8]) noexcept {
    out[0] = MAGIC_BYTE_0;       // Byte 0: Magic
    out[1] = OPCODE_RESET_TO_EPOCH;  // Byte 1: Opcode

    // Bytes 2-7: Graph_Epoch (48-bit big-endian)
    uint64_t be = htobe64(epoch);
    std::memcpy(out + 2, reinterpret_cast<const uint8_t*>(&be) + 2, 6);
}

}  // namespace renderer
}  // namespace enterprise_ai
