// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: epoll_reactor.hpp — Linux Epoll ET 模式非阻塞多路复用器头文件
//
// 职责: 将 Linux epoll 事件通知桥接至 Python asyncio 事件循环。
//       通过自管道 (self-pipe) 机制唤醒 Python 协程。

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>

namespace enterprise_ai {
namespace renderer {

// ---------------------------------------------------------------------------
// EpollReactor — Epoll ET 模式多路复用器
// ---------------------------------------------------------------------------
// 线程模型:
//   - ingest 线程:      调用 broadcast() → 加锁复制 client_fds_, 非阻塞 send
//   - reactor_thread_:   执行 epoll_wait，检测 EPOLLERR/EPOLLHUP → mark_for_removal
//   - Python 协程线程:   调用 process_ready_events() → drain self-pipe
//   - 清理收敛点:       cleanup_pending_removals() 仅在 reactor_loop 和
//                       process_ready_events 末尾加锁执行, 统一 unregister+close
//
// 三线程通过 client_mutex_ 保护 client_fds_ / pending_removal_。
class EpollReactor {
public:
    EpollReactor(size_t max_clients, size_t epoll_max_events);
    ~EpollReactor() noexcept;

    // Non-copyable, non-movable
    EpollReactor(const EpollReactor&) = delete;
    EpollReactor& operator=(const EpollReactor&) = delete;
    EpollReactor(EpollReactor&&) = delete;
    EpollReactor& operator=(EpollReactor&&) = delete;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------
    bool start() noexcept;
    void shutdown() noexcept;

    // ------------------------------------------------------------------
    // Client management
    // ------------------------------------------------------------------
    bool register_client(int fd) noexcept;
    bool unregister_client(int fd) noexcept;  // caller must hold client_mutex_

    // ------------------------------------------------------------------
    // Data path (ingest 线程调用)
    // ------------------------------------------------------------------
    // 加锁复制 client_fds_ 到栈上局部变量，遍历发送。
    // send 循环直到全部发出或 EAGAIN/EPIPE。
    // 遇断连仅 mark_for_removal，不在遍历中 erase/close。
    size_t broadcast(const uint8_t* data, size_t len) noexcept;

    // ------------------------------------------------------------------
    // Python asyncio bridge (Python 协程线程调用)
    // ------------------------------------------------------------------
    void process_ready_events() noexcept;
    int get_wakeup_fd() const noexcept { return pipefd_[0]; }

    // ------------------------------------------------------------------
    // Statistics
    // ------------------------------------------------------------------
    size_t syscall_count()        const noexcept { return syscall_count_.load(); }
    size_t total_events()         const noexcept { return total_events_.load(); }
    size_t total_broadcast_bytes() const noexcept { return total_broadcast_bytes_.load(); }

private:
    void reactor_loop() noexcept;
    void wakeup_thread() noexcept;

    // 仅记录 fd 到 pending_removal_ (加锁)，不执行 erase/close。
    void mark_for_removal(int fd) noexcept;

    // 统一清理入口 (加锁): epoll_ctl DEL + erase from client_fds_ + close(fd).
    // 仅在 reactor_loop / process_ready_events 末尾调用。
    void cleanup_pending_removals() noexcept;

    // ---- configuration ----
    size_t max_clients_;
    size_t epoll_max_events_;

    // ---- epoll ----
    int epoll_fd_ = -1;

    // ---- self-pipe ----
    int pipefd_[2] = {-1, -1};

    // ---- client state (client_mutex_ 保护) ----
    std::vector<int> client_fds_;
    std::vector<int> pending_removal_;
    std::mutex client_mutex_;

    // ---- reactor thread ----
    std::thread reactor_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};

    // ---- epoll_wait output buffer (一次性分配) ----
    struct epoll_event* events_ = nullptr;

    // ---- statistics (atomic, 跨线程读) ----
    std::atomic<size_t> syscall_count_{0};
    std::atomic<size_t> total_events_{0};
    std::atomic<size_t> total_broadcast_bytes_{0};
};

}  // namespace renderer
}  // namespace enterprise_ai
