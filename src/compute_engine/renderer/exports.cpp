// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: exports.cpp — PyRendererEngine RAII 封装实现
//
// 供成员 G 的 pybind11 绑定编译单元 (core_bind.cpp) 链接。
// 自身不包含 pybind11 头文件 — 绑定代码在 G 的 core_bind.cpp 中。

#include "exports.hpp"
#include "renderer_engine.hpp"

#include <stdexcept>
#include <cstdio>

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// PyRendererEngine
// ===========================================================================

PyRendererEngine::PyRendererEngine(const RendererConfig& cfg)
    : config_(cfg)
{
    if (!engine_.init(config_)) {
        throw std::runtime_error("PyRendererEngine: RendererEngine::init() failed");
    }
    initialized_ = true;
}

PyRendererEngine::~PyRendererEngine() noexcept {
    shutdown();
}

void PyRendererEngine::ingest(uint32_t channel_id,
                               const uint8_t* data,
                               size_t len,
                               uint64_t epoch,
                               uint64_t snapshot_id) noexcept
{
    if (initialized_) {
        engine_.ingest(channel_id, data, len, epoch, snapshot_id);
    }
}

bool PyRendererEngine::time_travel(uint32_t channel_id,
                                    uint64_t epoch,
                                    uint64_t snapshot_id) noexcept
{
    if (!initialized_) {
        return false;
    }
    return engine_.time_travel(channel_id, epoch, snapshot_id);
}

int PyRendererEngine::get_wakeup_fd() const noexcept {
    if (!initialized_) {
        return -1;
    }
    return engine_.get_reactor_wakeup_fd();
}

EngineStats PyRendererEngine::get_stats() const noexcept {
    if (!initialized_) {
        return EngineStats{};
    }
    return engine_.get_stats();
}

void PyRendererEngine::shutdown() noexcept {
    if (initialized_) {
        engine_.shutdown();
        initialized_ = false;
    }
}

}  // namespace renderer
}  // namespace enterprise_ai
