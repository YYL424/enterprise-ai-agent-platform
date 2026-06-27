"""整树进程杀灭工具 — 防僵尸/孤儿进程逃逸。

在非 root 环境下通过 systemd scope 销毁或 os.kill 递归杀灭进程树。
"""

import os
import signal
import subprocess
from pathlib import Path

import psutil
from loguru import logger


class ProcessKiller:
    """整树进程杀灭工具。

    双路径策略：
    1. **systemd 路径（首选）**：通过 ``systemctl --user stop`` 销毁 scope
    2. **裸进程路径（降级）**：通过 ``psutil`` 递归杀灭 PID 树
    """

    @staticmethod
    def kill_scope(scope_name: str) -> bool:
        """通过 systemd 销毁整个 scope 单元。

        Args:
            scope_name: systemd scope 名称，如 ``"sandbox_job_abc123.scope"``

        Returns:
            True 如果成功销毁，False 如果 scope 不存在或操作失败。
        """
        try:
            result = subprocess.run(
                ["systemctl", "--user", "stop", scope_name],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                logger.info(
                    "[ProcessKiller] Scope destroyed via systemd | scope={}",
                    scope_name,
                )
                return True
            else:
                logger.warning(
                    "[ProcessKiller] systemctl stop failed | scope={} | "
                    "stderr={}",
                    scope_name,
                    result.stderr.strip(),
                )
                return False
        except FileNotFoundError:
            logger.warning(
                "[ProcessKiller] systemctl not available, "
                "falling back to PID tree kill"
            )
            return False
        except subprocess.TimeoutExpired:
            logger.error(
                "[ProcessKiller] systemctl stop timed out | scope={}",
                scope_name,
            )
            return False

    @staticmethod
    def kill_pid_tree(root_pid: int) -> None:
        """递归杀灭 root_pid 及其所有派生子进程。

        策略：
        1. 先发 SIGTERM 请求优雅退出
        2. 等待 3 秒后发 SIGKILL 强制杀灭
        3. 从叶子进程开始向上收割，避免孤儿进程

        Args:
            root_pid: 进程树的根 PID
        """
        try:
            parent = psutil.Process(root_pid)
        except psutil.NoSuchProcess:
            logger.info(
                "[ProcessKiller] PID {} already exited", root_pid,
            )
            return

        # 收集所有子进程（从叶子到根）
        children = parent.children(recursive=True)

        # 先发 SIGTERM
        for child in reversed(children):
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
        try:
            parent.terminate()
        except psutil.NoSuchProcess:
            pass

        # 等待 3 秒
        _, alive = psutil.wait_procs(
            [parent] + children, timeout=3,
        )

        # SIGKILL 兜底
        for proc in alive:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                pass

        logger.info(
            "[ProcessKiller] PID tree killed | root_pid={} | "
            "children_count={}",
            root_pid,
            len(children),
        )

    @staticmethod
    def is_alive(pid: int) -> bool:
        """检查进程是否仍在运行。"""
        try:
            proc = psutil.Process(pid)
            return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
