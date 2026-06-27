// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: exports.hpp — 供成员 G 做 pybind11 绑定的纯 C++ 接口声明
//
// 成员 G 请将此文件直接纳入 pybind11 绑定编译单元 (core_bind.cpp)。
// 不做任何业务逻辑侵入，仅做薄封装层。

#pragma once

#include <cstddef>
#include <cstdint>

#include "renderer_engine.hpp"  // for RendererConfig, EngineStats, RendererEngine

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// RendererConfig — Python 可构造的配置结构体
// ===========================================================================
// 所有字段均提供默认值，Python 侧可按需覆盖。
// 成员 G 绑定方式 (pybind11):
//   py::class_<RendererConfig>(m, "RendererConfig")
//       .def(py::init<>())
//       .def_readwrite("buffer_capacity_per_channel", &RendererConfig::buffer_capacity_per_channel)
//       .def_readwrite("max_channels",                &RendererConfig::max_channels)
//       .def_readwrite("max_clients",                 &RendererConfig::max_clients)
//       .def_readwrite("epoll_max_events",            &RendererConfig::epoll_max_events);

// RendererConfig 定义在 renderer_engine.hpp 中，此处重新导出。

// ===========================================================================
// PyRendererEngine — 封装 RendererEngine 的 RAII 包装
// ===========================================================================
// 成员 G 绑定方式 (pybind11):
//   py::class_<PyRendererEngine>(m, "RendererEngine")
//       .def(py::init<const RendererConfig&>())
//       .def("ingest",       &PyRendererEngine::ingest)
//       .def("time_travel",  &PyRendererEngine::time_travel)
//       .def("get_wakeup_fd", &PyRendererEngine::get_wakeup_fd)
//       .def("get_stats",    &PyRendererEngine::get_stats)
//       .def("shutdown",     &PyRendererEngine::shutdown);
class PyRendererEngine {
public:
    // 构造并 init。若 init 失败，抛出 std::runtime_error。
    explicit PyRendererEngine(const RendererConfig& cfg);

    // 析构时自动 shutdown。
    ~PyRendererEngine() noexcept;

    // Non-copyable, non-movable
    PyRendererEngine(const PyRendererEngine&) = delete;
    PyRendererEngine& operator=(const PyRendererEngine&) = delete;
    PyRendererEngine(PyRendererEngine&&) = delete;
    PyRendererEngine& operator=(PyRendererEngine&&) = delete;

    // ------------------------------------------------------------------
    // ingest — 摄入 Python bytes 数据
    // ------------------------------------------------------------------
    // G 绑定方式:
    //   .def("ingest", [](PyRendererEngine& self, int channel_id,
    //                     py::bytes data, uint64_t epoch, uint64_t snapshot_id) {
    //       char* buf = nullptr;
    //       Py_ssize_t len = 0;
    //       PyBytes_AsStringAndSize(data.ptr(), &buf, &len);
    //       self.ingest(static_cast<uint32_t>(channel_id),
    //                   reinterpret_cast<const uint8_t*>(buf),
    //                   static_cast<size_t>(len), epoch, snapshot_id);
    //   })
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
    // get_wakeup_fd — 获取 epoll 唤醒 fd（供 Python asyncio.add_reader）
    // ------------------------------------------------------------------
    int get_wakeup_fd() const noexcept;

    // ------------------------------------------------------------------
    // get_stats — 获取引擎统计信息（返回副本，供 Python 侧读取）
    // ------------------------------------------------------------------
    EngineStats get_stats() const noexcept;

    // ------------------------------------------------------------------
    // shutdown — 优雅关闭
    // ------------------------------------------------------------------
    void shutdown() noexcept;

private:
    RendererConfig config_;
    RendererEngine engine_;
    bool initialized_ = false;
};

}  // namespace renderer
}  // namespace enterprise_ai
