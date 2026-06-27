"""Conda 环境常驻进程预热池 — 50ms 冷启动目标。

系统启动时预创建常驻进程，通过 Unix Domain Socket 实现代码注入，
避免每次任务都需要重新加载 PyTorch/Transformers。
"""

import os
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import yaml
from loguru import logger

from src.compute_engine.isolation.sandbox_error import (
    SandboxError,
    PrewarmPoolExhaustedError,
)


class WarmPreheater:
    """Conda 环境常驻进程预热池。

    工作原理：
    1. ``initialize()`` 启动 pool_size 个常驻 Python 进程
    2. 每个进程监听一个 Unix Domain Socket（如 ``/tmp/sandbox_warm_0.sock``）
    3. 调度器通过 ``get_warmed_slot()`` 获取空闲 socket 路径
    4. 通过 ``inject_code()`` 将用户代码写入 socket，进程执行后返回结果

    Attributes:
        pool_size: 预热槽位数量
        env_name: Conda 环境名
        socket_dir: Unix Domain Socket 存放目录
    """

    # 预热进程执行的监听脚本
    _LISTENER_SCRIPT = r"""
import socket, sys, os, json, traceback

def main():
    sock_path = sys.argv[1]
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen(1)
    os.chmod(sock_path, 0o600)
    print(f"WARM_READY:{sock_path}", flush=True)
    while True:
        conn, _ = server.accept()
        try:
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(data) > 10 * 1024 * 1024:  # 10 MB limit
                    break
            code = data.decode("utf-8")
            local_ns = {}
            exec(code, {"__builtins__": __builtins__}, local_ns)
            result = json.dumps({"ok": True, "result": str(local_ns.get("result", ""))})
            conn.sendall(result.encode("utf-8"))
        except Exception as e:
            err = json.dumps({"ok": False, "error": str(e), "traceback": traceback.format_exc()})
            conn.sendall(err.encode("utf-8"))
        finally:
            conn.close()

if __name__ == "__main__":
    main()
"""

    def __init__(
        self,
        config_path: str | None = None,
        pool_size: int = 2,
        env_name: str = "sandbox",
    ) -> None:
        self._config: dict = {}
        if config_path is not None and Path(config_path).exists():
            with open(config_path, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f).get(
                    "sandbox_resource_isolation", {}
                ).get("prewarm", {})

        self.pool_size: int = pool_size or self._config.get("pool_size", 2)
        self.env_name: str = env_name or self._config.get("env_name", "sandbox")
        self.socket_dir: str = self._config.get(
            "socket_dir", "/tmp/sandbox_warm"
        )

        self._processes: dict[str, subprocess.Popen] = {}  # socket_path → Popen
        self._free_slots: list[str] = []  # 空闲 slot 的 socket 路径
        self._lock: threading.Lock = threading.Lock()
        self._initialized: bool = False

    def initialize(self) -> None:
        """预热：启动 pool_size 个常驻进程，每个绑定独立 socket。"""
        Path(self.socket_dir).mkdir(parents=True, exist_ok=True)

        for i in range(self.pool_size):
            sock_path = os.path.join(self.socket_dir, f"warm_{i}.sock")
            self._launch_listener(sock_path, i)

        self._initialized = True
        logger.info(
            "[WarmPreheater] 预热完成 | pool_size={} | env={}",
            self.pool_size,
            self.env_name,
        )

    def _launch_listener(self, sock_path: str, slot_index: int) -> None:
        """启动单个常驻监听进程。"""
        if os.path.exists(sock_path):
            os.unlink(sock_path)

        script = self._LISTENER_SCRIPT
        proc = subprocess.Popen(
            [
                "conda", "run", "-n", self.env_name,
                "python", "-c", script, sock_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # 等待进程就绪信号
        start = time.monotonic()
        while (time.monotonic() - start) < 15.0:
            line = proc.stdout.readline() if proc.stdout else ""
            if f"WARM_READY:{sock_path}" in line:
                break
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise SandboxError(
                    f"预热进程 {slot_index} 启动失败 | exit_code={proc.returncode} | "
                    f"stderr={stderr[:500]}"
                )

        if proc.poll() is not None:
            raise SandboxError(
                f"预热进程 {slot_index} 超时未就绪"
            )

        with self._lock:
            self._processes[sock_path] = proc
            self._free_slots.append(sock_path)

        logger.info(
            "[WarmPreheater] 槽位 {} 就绪 | sock={}", slot_index, sock_path,
        )

    def get_warmed_slot(self) -> str | None:
        """获取一个空闲的预热槽位 socket 路径。

        Returns:
            socket 路径如 ``"/tmp/sandbox_warm/warm_0.sock"``，
            或 None 表示无空闲槽位。
        """
        with self._lock:
            if not self._free_slots:
                return None
            return self._free_slots.pop(0)

    def inject_code(self, socket_path: str, code: str, timeout: float = 5.0) -> str:
        """向预热进程注入代码并返回执行结果。

        Args:
            socket_path: Unix Domain Socket 路径
            code: 要执行的 Python 代码
            timeout: 最大等待时间

        Returns:
            JSON 字符串 ``{"ok": true, "result": "..."}``

        Raises:
            SandboxError: 注入超时或通信失败
        """
        import json as _json

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        try:
            sock.connect(socket_path)
            sock.sendall(code.encode("utf-8"))
            sock.shutdown(socket.SHUT_WR)

            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk

            result = _json.loads(data.decode("utf-8"))
            if result.get("ok"):
                logger.info(
                    "[WarmPreheater] 代码注入成功 | sock={} | result_len={}",
                    socket_path,
                    len(str(result.get("result", ""))),
                )
                return str(result.get("result", ""))
            else:
                raise SandboxError(
                    f"代码执行失败: {result.get('error', 'unknown')}"
                )
        except socket.timeout:
            raise SandboxError(
                f"代码注入超时 ({timeout}s) | sock={socket_path}"
            )
        finally:
            sock.close()

    def recycle_slot(self, socket_path: str) -> None:
        """回收槽位，清理执行上下文但不销毁进程。

        向预热进程发送重置指令，使其恢复到干净状态。
        """
        try:
            self.inject_code(
                socket_path,
                "# warm-preheater: reset context\n"
                "import gc; gc.collect()\n"
                "result = 'slot_recycled'\n",
                timeout=3.0,
            )
            with self._lock:
                self._free_slots.append(socket_path)
            logger.info("[WarmPreheater] 槽位已回收 | sock={}", socket_path)
        except SandboxError:
            # 重置失败，销毁并重建槽位
            logger.warning(
                "[WarmPreheater] 槽位重置失败，销毁重建 | sock={}", socket_path,
            )
            self._destroy_slot(socket_path)
            idx = len(self._processes)
            new_path = os.path.join(self.socket_dir, f"warm_{idx}.sock")
            self._launch_listener(new_path, idx)

    def _destroy_slot(self, socket_path: str) -> None:
        """销毁指定槽位的进程和 socket 文件。"""
        with self._lock:
            proc = self._processes.pop(socket_path, None)
            if socket_path in self._free_slots:
                self._free_slots.remove(socket_path)

        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

        if os.path.exists(socket_path):
            os.unlink(socket_path)

    def shutdown(self) -> None:
        """关闭所有预热槽位并清理资源。"""
        for sock_path in list(self._processes.keys()):
            self._destroy_slot(sock_path)
        logger.info("[WarmPreheater] 全部槽位已关闭")
