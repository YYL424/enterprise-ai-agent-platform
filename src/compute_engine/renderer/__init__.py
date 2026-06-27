"""Compute Engine — Renderer Plane（成员 I 专属）。

流式渲染引擎：SlidingBuffer + LexicalHealer + MsgPackEncoder +
EpollReactor + DifferentialLayer + RendererEngine。

C++17 编译产物为 librenderer.so，由成员 G 的 CMake/pybind11 统一链接。
Python 侧通过 exports.hpp 的 pybind11 绑定访问。
"""

# C++ 模块导出说明（编译后通过 pybind11 绑定）：
#
#   from compute_engine.bindings.renderer import RendererEngine, RendererConfig
#
#   cfg = RendererConfig()
#   cfg.buffer_capacity_per_channel = 16 * 1024 * 1024
#   engine = RendererEngine(cfg)
#   engine.ingest(0, b"hello", 1, 1)
#   engine.time_travel(0, 1, 1)
#   fd = engine.get_wakeup_fd()
#   stats = engine.get_stats()
#   engine.shutdown()
