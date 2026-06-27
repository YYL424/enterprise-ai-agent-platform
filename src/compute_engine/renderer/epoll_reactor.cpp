// Author: 成员 I (Module 3 流式渲染专家)
// Date: 2026-06-27
// File: epoll_reactor.cpp — Linux Epoll ET 模式非阻塞多路复用器实现
//
// 自管道 (self-pipe) 机制桥接 Python asyncio。
// 零动态分配：client 列表预分配 1024 容量，运行期不触发重新分配。
//
// 线程安全: client_mutex_ 保护 client_fds_ 和 pending_removal_。
//   - broadcast() 加锁复制 client_fds_ 到栈上再遍历
//   - 断连只记录到 pending_removal_，不在遍历中 erase/close
//   - cleanup_pending_removals() 统一在 reactor_loop / process_ready_events 末尾执行

#define _GNU_SOURCE  // for pipe2, accept4 if needed
#include "epoll_reactor.hpp"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstdio>
#include <cstring>

namespace enterprise_ai {
namespace renderer {

// ===========================================================================
// Utility: set fd to non-blocking
// ===========================================================================

static bool set_nonblocking(int fd) noexcept {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        std::fprintf(stderr, "[EpollReactor] fcntl(F_GETFL) fd=%d errno=%d\n", fd, errno);
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        std::fprintf(stderr, "[EpollReactor] fcntl(F_SETFL) fd=%d errno=%d\n", fd, errno);
        return false;
    }
    return true;
}

// ===========================================================================
// Constructor / Destructor
// ===========================================================================

EpollReactor::EpollReactor(size_t max_clients, size_t epoll_max_events)
    : max_clients_(max_clients)
    , epoll_max_events_(epoll_max_events)
{
    // Pre-allocate client list (no reallocation at runtime)
    client_fds_.reserve(max_clients_);
    pending_removal_.reserve(max_clients_);

    // Create epoll instance
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        std::fprintf(stderr, "[EpollReactor] epoll_create1 failed: errno=%d\n", errno);
    }

    // Create self-pipe for wakeup mechanism
    if (pipe2(pipefd_, O_NONBLOCK | O_CLOEXEC) < 0) {
        std::fprintf(stderr, "[EpollReactor] pipe2 failed: errno=%d\n", errno);
    }

    // Allocate epoll_event buffer once
    events_ = new (std::nothrow) struct epoll_event[epoll_max_events_];
    if (events_ == nullptr) {
        std::fprintf(stderr, "[EpollReactor] failed to allocate epoll_event buffer\n");
    }
}

EpollReactor::~EpollReactor() noexcept {
    shutdown();

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    if (pipefd_[0] >= 0) {
        close(pipefd_[0]);
        pipefd_[0] = -1;
    }
    if (pipefd_[1] >= 0) {
        close(pipefd_[1]);
        pipefd_[1] = -1;
    }

    if (events_ != nullptr) {
        delete[] events_;
        events_ = nullptr;
    }
}

// ===========================================================================
// start
// ===========================================================================

bool EpollReactor::start() noexcept {
    if (epoll_fd_ < 0) {
        std::fprintf(stderr, "[EpollReactor] cannot start: invalid epoll_fd\n");
        return false;
    }

    // Register the read end of self-pipe with epoll (ET mode)
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = pipefd_[0];
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, pipefd_[0], &ev) < 0) {
        std::fprintf(stderr, "[EpollReactor] epoll_ctl(ADD, pipefd[0]) failed: errno=%d\n", errno);
        return false;
    }
    syscall_count_++;

    running_.store(true);
    reactor_thread_ = std::thread(&EpollReactor::reactor_loop, this);
    started_.store(true);
    return true;
}

// ===========================================================================
// shutdown
// ===========================================================================

void EpollReactor::shutdown() noexcept {
    if (!started_.load()) {
        return;
    }

    running_.store(false);

    // Wake up the reactor thread so it can exit
    wakeup_thread();

    if (reactor_thread_.joinable()) {
        reactor_thread_.join();
    }
    started_.store(false);
}

// ===========================================================================
// register_client — 加锁保护
// ===========================================================================

bool EpollReactor::register_client(int fd) noexcept {
    if (fd < 0) return false;

    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        if (client_fds_.size() >= max_clients_) {
            std::fprintf(stderr, "[EpollReactor] register_client fd=%d failed (full)\n", fd);
            return false;
        }
    }

    if (!set_nonblocking(fd)) {
        return false;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = fd;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::fprintf(stderr, "[EpollReactor] epoll_ctl(ADD, fd=%d) failed: errno=%d\n", fd, errno);
        return false;
    }
    syscall_count_++;

    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        client_fds_.push_back(fd);
    }
    return true;
}

// ===========================================================================
// unregister_client — caller must hold client_mutex_
// ===========================================================================

bool EpollReactor::unregister_client(int fd) noexcept {
    if (fd < 0) return false;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
        if (errno != ENOENT) {
            std::fprintf(stderr, "[EpollReactor] epoll_ctl(DEL, fd=%d) failed: errno=%d\n", fd, errno);
            // Continue to erase from list even if epoll_ctl fails
        }
    }
    syscall_count_++;

    for (auto it = client_fds_.begin(); it != client_fds_.end(); ++it) {
        if (*it == fd) {
            client_fds_.erase(it);
            return true;
        }
    }
    return false;
}

// ===========================================================================
// broadcast — 加锁复制 client_fds_, send 循环直到全部发出或 EAGAIN
// ===========================================================================

size_t EpollReactor::broadcast(const uint8_t* data, size_t len) noexcept {
    if (data == nullptr || len == 0) {
        return 0;
    }

    // 加锁复制 client_fds_ 到栈上局部变量，避免遍历中被 reactor_loop 修改
    std::vector<int> clients_copy;
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        clients_copy = client_fds_;  // copy
    }

    size_t sent_count = 0;

    for (int fd : clients_copy) {
        // 循环 send 直到 offset == len 或 EAGAIN/EPIPE
        size_t offset = 0;
        bool client_ok = true;
        while (offset < len && client_ok) {
            ssize_t n = send(fd, data + offset, len - offset,
                             MSG_DONTWAIT | MSG_NOSIGNAL);
            syscall_count_++;

            if (n > 0) {
                offset += static_cast<size_t>(n);
                total_broadcast_bytes_ += static_cast<size_t>(n);
            } else if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Socket buffer full — 本客户端已尽力，跳过
                    break;
                } else if (errno == EPIPE || errno == ECONNRESET) {
                    // 断连 — 只记录，不在此处 erase/close
                    mark_for_removal(fd);
                    client_ok = false;
                } else {
                    // 其他错误
                    client_ok = false;
                }
            } else {
                // n == 0: 连接正常关闭
                mark_for_removal(fd);
                client_ok = false;
            }
        }
        if (offset > 0) {
            sent_count++;
        }
    }

    return sent_count;
}

// ===========================================================================
// process_ready_events — Python 协程线程调用
// ===========================================================================

void EpollReactor::process_ready_events() noexcept {
    // Drain the self-pipe read end (ET 模式: 必须读到 EAGAIN)
    uint8_t drain_buf[64];
    while (true) {
        ssize_t n = read(pipefd_[0], drain_buf, sizeof(drain_buf));
        syscall_count_++;
        if (n > 0) {
            continue;                          // data drained
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;                          // pipe empty
            }
            if (errno == EINTR) {
                continue;                       // signal interrupted, retry
            }
            break;                              // real error
        } else {
            break;  // n == 0: pipe closed
        }
    }

    // Non-blocking epoll check for statistics
    if (epoll_fd_ >= 0 && events_ != nullptr) {
        int nfds = epoll_wait(epoll_fd_, events_,
                              static_cast<int>(epoll_max_events_), 0);
        syscall_count_++;
        if (nfds > 0) {
            total_events_ += static_cast<size_t>(nfds);
        }
    }

    // 统一清理断连客户端
    cleanup_pending_removals();
}

// ===========================================================================
// reactor_loop — epoll 线程主循环
// ===========================================================================

void EpollReactor::reactor_loop() noexcept {
    while (running_.load()) {
        if (events_ == nullptr || epoll_fd_ < 0) {
            break;
        }

        int nfds = epoll_wait(epoll_fd_, events_,
                              static_cast<int>(epoll_max_events_), 100);
        syscall_count_++;

        if (nfds < 0) {
            if (errno == EINTR) {
                cleanup_pending_removals();  // 超时前先清理已标记的断连客户端
                continue;
            }
            std::fprintf(stderr, "[EpollReactor] epoll_wait error: errno=%d\n", errno);
            break;
        }

        if (nfds == 0) {
            cleanup_pending_removals();  // timeout 时也要清理 pending_removal_
            continue;
        }

        total_events_ += static_cast<size_t>(nfds);

        for (int i = 0; i < nfds; ++i) {
            int fd = events_[i].data.fd;

            if (fd == pipefd_[0]) {
                // Self-pipe wakeup — drain it (ET 模式: 必须读到 EAGAIN)
                uint8_t drain_buf[64];
                while (true) {
                    ssize_t n = read(pipefd_[0], drain_buf, sizeof(drain_buf));
                    syscall_count_++;
                    if (n > 0) continue;               // data drained
                    if (n < 0 && errno == EINTR) continue;  // signal interrupted, retry
                    break;  // EAGAIN (drained) or real error / EOF
                }
            } else {
                // Client fd event
                uint32_t flags = events_[i].events;
                if ((flags & (EPOLLERR | EPOLLHUP)) != 0) {
                    mark_for_removal(fd);
                }
            }
        }

        // 统一清理：在 epoll_wait 间隙安全执行 erase + close
        cleanup_pending_removals();
    }
}

// ===========================================================================
// mark_for_removal — 加锁记录，不执行 erase/close
// ===========================================================================

void EpollReactor::mark_for_removal(int fd) noexcept {
    std::lock_guard<std::mutex> lock(client_mutex_);
    for (int existing : pending_removal_) {
        if (existing == fd) {
            return;  // 已标记，去重
        }
    }
    pending_removal_.push_back(fd);
}

// ===========================================================================
// cleanup_pending_removals — 统一清理入口 (加锁)
// ===========================================================================

void EpollReactor::cleanup_pending_removals() noexcept {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (pending_removal_.empty()) {
        return;
    }

    for (int fd : pending_removal_) {
        // epoll_ctl DEL
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            if (errno != ENOENT) {
                std::fprintf(stderr, "[EpollReactor] cleanup: epoll_ctl(DEL, fd=%d) errno=%d\n",
                             fd, errno);
            }
        }
        syscall_count_++;

        // erase from client_fds_
        for (auto it = client_fds_.begin(); it != client_fds_.end(); ++it) {
            if (*it == fd) {
                client_fds_.erase(it);
                break;
            }
        }

        // close fd
        close(fd);
    }

    pending_removal_.clear();
}

// ===========================================================================
// wakeup_thread
// ===========================================================================

void EpollReactor::wakeup_thread() noexcept {
    if (pipefd_[1] >= 0) {
        uint8_t byte = 0xFF;
        ssize_t n = write(pipefd_[1], &byte, 1);
        syscall_count_++;
        (void)n;  // best-effort, 忽略 EAGAIN
    }
}

}  // namespace renderer
}  // namespace enterprise_ai
