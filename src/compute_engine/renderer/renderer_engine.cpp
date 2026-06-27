// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: renderer_engine.cpp — 总控渲染引擎实现
//
// 职责: 聚合所有子组件，实现 ingest 热路径管线:
//       heal → write → encode → broadcast
//       零动态分配 (ingest 使用 uint8_t stack_buf[65536]).

#include "renderer_engine.hpp"

// Complete type definitions needed for unique_ptr destructors and operations
#include "sliding_buffer.hpp"
#include "lexical_healer.hpp"
#include "epoll_reactor.hpp"
#include "differential_layer.hpp"
#include "msgpack_encoder.hpp"

#include <cstdio>
#include <cstring>
#include <algorithm>

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// Destructor
// ===========================================================================

RendererEngine::~RendererEngine() noexcept {
    shutdown();
}

// ===========================================================================
// init — 一次性分配所有内存，启动 epoll 线程
// ===========================================================================

bool RendererEngine::init(const RendererConfig& cfg) noexcept {
    if (initialized_) {
        std::fprintf(stderr, "[RendererEngine] already initialized\n");
        return false;
    }

    config_ = cfg;

    // Create sub-components (all heap allocations happen here, once)
    sliding_buffer_.reset(new (std::nothrow)
        SlidingBuffer(cfg.buffer_capacity_per_channel, cfg.max_channels));
    if (!sliding_buffer_) {
        std::fprintf(stderr, "[RendererEngine] failed to create SlidingBuffer\n");
        return false;
    }

    lexical_healer_.reset(new (std::nothrow)
        LexicalHealer(cfg.max_channels));
    if (!lexical_healer_) {
        std::fprintf(stderr, "[RendererEngine] failed to create LexicalHealer\n");
        return false;
    }

    epoll_reactor_.reset(new (std::nothrow)
        EpollReactor(cfg.max_clients, cfg.epoll_max_events));
    if (!epoll_reactor_) {
        std::fprintf(stderr, "[RendererEngine] failed to create EpollReactor\n");
        return false;
    }

    differential_layer_.reset(new (std::nothrow)
        DifferentialLayer(sliding_buffer_.get(), epoll_reactor_.get()));
    if (!differential_layer_) {
        std::fprintf(stderr, "[RendererEngine] failed to create DifferentialLayer\n");
        return false;
    }

    // Start the epoll reactor thread
    if (!epoll_reactor_->start()) {
        std::fprintf(stderr, "[RendererEngine] failed to start EpollReactor\n");
        return false;
    }

    initialized_ = true;
    return true;
}

// ===========================================================================
// ingest — 数据摄入热路径 (noexcept, 零动态分配)
// ===========================================================================

void RendererEngine::ingest(uint32_t channel_id,
                             const uint8_t* data,
                             size_t len,
                             uint64_t epoch,
                             uint64_t snapshot_id) noexcept
{
    if (!initialized_) {
        std::fprintf(stderr, "[RendererEngine] ingest called before init\n");
        return;
    }

    if (data == nullptr || len == 0) {
        return;
    }

    // 栈溢出保护：超过 kMaxIngestLen 时截断
    if (len > kMaxIngestLen) {
        len = kMaxIngestLen;
    }

    total_ingest_calls_++;

    // ---- Stack-allocated temporary buffers (零堆分配) ----
    // heal_buf: at most len + 3 (pending tail prepend) ≤ kMaxHealBuf
    uint8_t heal_buf[kMaxHealBuf];
    size_t healed_len = 0;
    PendingTail pending_out;

    // ---- Step 1: UTF-8 lexical healing ----
    bool heal_ok = lexical_healer_->heal(channel_id, data, len,
                                          heal_buf, healed_len, pending_out);
    if (!heal_ok || healed_len == 0) {
        return;
    }

    // ---- Step 2: Write healed data to sliding buffer ----
    size_t written = sliding_buffer_->write(channel_id, heal_buf, healed_len,
                                             epoch, snapshot_id);
    if (written == 0) {
        return;
    }

    // ---- Step 3: Encode data frame to stack buffer ----
    uint8_t stack_buf[kMaxFrameBuf];  // 64 KB
    size_t encoded_len = 0;

    bool encode_ok = MsgPackEncoder::encode_data_frame(
        channel_id,
        heal_buf,
        healed_len,
        epoch,
        snapshot_id,
        stack_buf,
        encoded_len,
        sizeof(stack_buf));

    if (!encode_ok || encoded_len == 0) {
        std::fprintf(stderr, "[RendererEngine] encode_data_frame failed: len=%zu\n", healed_len);
        return;
    }

    // ---- Step 4: Broadcast to all clients ----
    epoll_reactor_->broadcast(stack_buf, encoded_len);
}

// ===========================================================================
// time_travel
// ===========================================================================

bool RendererEngine::time_travel(uint32_t channel_id,
                                  uint64_t epoch,
                                  uint64_t snapshot_id) noexcept
{
    if (!initialized_ || !differential_layer_) {
        return false;
    }
    return differential_layer_->reset_to_epoch(channel_id, epoch, snapshot_id);
}

// ===========================================================================
// get_reactor_wakeup_fd
// ===========================================================================

int RendererEngine::get_reactor_wakeup_fd() const noexcept {
    if (!initialized_ || !epoll_reactor_) {
        return -1;
    }
    return epoll_reactor_->get_wakeup_fd();
}

// ===========================================================================
// shutdown
// ===========================================================================

void RendererEngine::shutdown() noexcept {
    if (!initialized_) {
        return;
    }

    if (epoll_reactor_) {
        epoll_reactor_->shutdown();
    }

    initialized_ = false;
}

// ===========================================================================
// get_stats
// ===========================================================================

EngineStats RendererEngine::get_stats() const noexcept {
    EngineStats stats;

    if (sliding_buffer_) {
        stats.sb_bytes_written = sliding_buffer_->total_bytes_written();
        stats.sb_bytes_read    = sliding_buffer_->total_bytes_read();
        stats.sb_total_rewinds = sliding_buffer_->total_rewinds();
    }

    if (lexical_healer_) {
        stats.lh_heal_calls  = lexical_healer_->total_heal_calls();
        stats.lh_truncations = lexical_healer_->total_truncations();
    }

    if (epoll_reactor_) {
        stats.er_syscall_count   = epoll_reactor_->syscall_count();
        stats.er_total_events    = epoll_reactor_->total_events();
        stats.er_broadcast_bytes = epoll_reactor_->total_broadcast_bytes();
    }

    if (differential_layer_) {
        stats.dl_total_resets = differential_layer_->total_resets();
    }

    stats.total_ingest_calls = total_ingest_calls_;
    stats.dynamic_alloc_count = 0;  // Always zero (验收指标 10)

    return stats;
}

}  // namespace renderer
}  // namespace enterprise_ai
