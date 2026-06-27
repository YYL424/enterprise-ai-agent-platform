// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: renderer_engine.hpp — 总控引擎头文件
//
// 职责: 聚合 SlidingBuffer、LexicalHealer、EpollReactor、DifferentialLayer、
//       MsgPackEncoder，对外暴露统一入口。
//       PyRendererEngine / RendererConfig 的 Python 绑定接口见 exports.hpp。

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

namespace enterprise_ai {
namespace renderer {

// Forward declarations (complete types in respective headers / .cpp)
class SlidingBuffer;
class LexicalHealer;
class EpollReactor;
class DifferentialLayer;

// ---------------------------------------------------------------------------
// RendererConfig — 引擎配置结构体
// ---------------------------------------------------------------------------
struct RendererConfig {
    size_t buffer_capacity_per_channel = 16 * 1024 * 1024;  // 16 MB
    size_t max_channels                = 64;
    size_t max_clients                 = 1024;
    size_t epoll_max_events            = 1024;
};

// ---------------------------------------------------------------------------
// EngineStats — 引擎运行时统计
// ---------------------------------------------------------------------------
struct EngineStats {
    // SlidingBuffer
    size_t sb_bytes_written   = 0;
    size_t sb_bytes_read      = 0;
    size_t sb_total_rewinds   = 0;

    // LexicalHealer
    size_t lh_heal_calls      = 0;
    size_t lh_truncations     = 0;

    // EpollReactor
    size_t er_syscall_count   = 0;
    size_t er_total_events    = 0;
    size_t er_broadcast_bytes = 0;

    // DifferentialLayer
    size_t dl_total_resets    = 0;

    // Engine-level
    size_t total_ingest_calls = 0;
    size_t dynamic_alloc_count = 0;  // Must remain 0 (验收指标 10)
};

// ---------------------------------------------------------------------------
// RendererEngine — 总控渲染引擎
// ---------------------------------------------------------------------------
// 线程模型:
//   - ingest() 由单生产者线程调用（或外部加锁），内部无锁。
//   - EpollReactor 内部有独立线程做 epoll_wait。
//   - Python 通过 asyncio.add_reader 在单协程线程中处理回调。
class RendererEngine {
public:
    // 热路径缓冲区常量
    // Pipeline: ingest → heal(kMaxHealBuf) → encode(kMaxFrameBuf) → broadcast
    // encode: header(27) + healed_len ≤ kMaxFrameBuf
    // ingest:  len ≤ kMaxIngestLen  ensures  healed_len + header ≤ kMaxFrameBuf
    static constexpr size_t kMaxHealBuf   = 131072;  // 128 KB (lexical healer output)
    static constexpr size_t kMaxFrameBuf  = kMaxHealBuf + 27;  // 128 KB + 27 B header = 131099
    static constexpr size_t kMaxIngestLen = kMaxHealBuf - 3;   // 131069 (reserve 3 B pending tail)

    RendererEngine() = default;
    ~RendererEngine() noexcept;

    // Non-copyable, non-movable
    RendererEngine(const RendererEngine&) = delete;
    RendererEngine& operator=(const RendererEngine&) = delete;
    RendererEngine(RendererEngine&&) = delete;
    RendererEngine& operator=(RendererEngine&&) = delete;

    // ------------------------------------------------------------------
    // init — 一次性分配所有内存，启动 epoll 线程
    // ------------------------------------------------------------------
    bool init(const RendererConfig& cfg) noexcept;

    // ------------------------------------------------------------------
    // ingest — 数据摄入（热路径, noexcept, 零堆分配）
    // ------------------------------------------------------------------
    // len 超出 kMaxIngestLen 时自动截断，防止栈溢出。
    void ingest(uint32_t channel_id,
                const uint8_t* data,
                size_t len,
                uint64_t epoch,
                uint64_t snapshot_id) noexcept;

    // ------------------------------------------------------------------
    // time_travel — 时间旅行回滚
    // ------------------------------------------------------------------
    bool time_travel(uint32_t channel_id,
                     uint64_t epoch,
                     uint64_t snapshot_id) noexcept;

    // ------------------------------------------------------------------
    // get_reactor_wakeup_fd
    // ------------------------------------------------------------------
    int get_reactor_wakeup_fd() const noexcept;

    // ------------------------------------------------------------------
    // shutdown / get_stats
    // ------------------------------------------------------------------
    void shutdown() noexcept;
    EngineStats get_stats() const noexcept;

    // ---- Component accessors (for testing, returns raw non-owning pointer) ----
    SlidingBuffer*      get_sliding_buffer()      noexcept { return sliding_buffer_.get(); }
    LexicalHealer*      get_lexical_healer()      noexcept { return lexical_healer_.get(); }
    EpollReactor*       get_epoll_reactor()       noexcept { return epoll_reactor_.get(); }
    DifferentialLayer*  get_differential_layer()  noexcept { return differential_layer_.get(); }

private:
    RendererConfig config_;
    bool initialized_ = false;

    std::unique_ptr<SlidingBuffer>      sliding_buffer_;
    std::unique_ptr<LexicalHealer>      lexical_healer_;
    std::unique_ptr<EpollReactor>       epoll_reactor_;
    std::unique_ptr<DifferentialLayer>  differential_layer_;

    size_t total_ingest_calls_ = 0;
};

}  // namespace renderer
}  // namespace enterprise_ai
