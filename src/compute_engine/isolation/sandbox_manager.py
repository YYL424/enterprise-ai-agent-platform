"""沙箱管理器 — systemd-run 隔离 + cgroups 资源限制 + 日志审计。

成员 E 的核心模块，负责：
1. 通过 ``systemd-run --user --scope`` 启动隔离沙箱
2. 通过 cgroups v2 限制内存/CPU/IO/进程数
3. 整树进程杀灭（防僵尸）
4. stdout/stderr 日志捕获 + Redis 异步推送
"""

import os
import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger

from src.compute_engine.isolation.process_killer import ProcessKiller
from src.compute_engine.isolation.sandbox_error import (
    SandboxError,
    SandboxLaunchError,
    SandboxTerminateError,
    SandboxTimeoutError,
)

# ── 类型别名 ────────────────────────────────────────────────────────────────

JobContext = Dict[str, Any]  # 由成员 D 的 scheduling 模块定义具体模型


# ═══════════════════════════════════════════════════════════════════════════════
# SandboxManager
# ═══════════════════════════════════════════════════════════════════════════════


class SandboxManager:
    """systemd 沙箱管理器。

    通过 ``systemd-run --user --scope`` 在非 root 环境下创建
    cgroups 隔离的进程沙箱，限制内存、CPU、IO、进程数，
    并将 stdout/stderr 重定向到日志文件。

    Args:
        config_path: sandbox_limit.yaml 路径，默认 ``configs/module2/sandbox_limit.yaml``
    """

    _DEFAULT_CONFIG = "configs/module2/sandbox_limit.yaml"

    def __init__(self, config_path: Optional[str] = None) -> None:
        self._config_path: str = config_path or self._DEFAULT_CONFIG
        self._config: dict[str, Any] = {}
        self._jobs: dict[str, subprocess.Popen] = {}  # job_id → Popen
        self._log_threads: dict[str, threading.Thread] = {}
        self._reload_config()

    # ── 配置管理 ──────────────────────────────────────────────────────────

    def _reload_config(self) -> None:
        """加载或重新加载 YAML 配置。"""
        config_file = Path(self._config_path)
        if not config_file.exists():
            logger.warning(
                "[SandboxManager] 配置文件不存在，使用默认值 | path={}",
                self._config_path,
            )
            self._config = {}
            return

        with open(config_file, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f).get(
                "sandbox_resource_isolation", {}
            )
        logger.info(
            "[SandboxManager] 配置已加载 | path={}", self._config_path,
        )

    def reload(self) -> None:
        """公开的热加载接口。"""
        self._reload_config()

    # ── 配置读取辅助 ──────────────────────────────────────────────────────

    def _cgroup_limits(self) -> dict[str, Any]:
        return self._config.get("cgroups_limits", {})

    def _cpu_slot(self, slot_id: int) -> dict[str, Any] | None:
        slots = self._config.get("cpu_affinity", {}).get("slots", [])
        for s in slots:
            if s.get("slot_id") == slot_id:
                return s
        return None

    def _scope_prefix(self) -> str:
        return self._config.get("systemd", {}).get("scope_prefix", "sandbox_job")

    def _log_dir(self) -> Path:
        return Path(
            self._config.get("logging", {}).get("log_dir", "logs")
        )

    def _redis_prefix(self) -> str:
        return self._config.get("logging", {}).get(
            "redis_list_prefix", "data_plane:logs"
        )

    # ── 公共接口 ──────────────────────────────────────────────────────────

    def launch(self, job_ctx: JobContext) -> str:
        """启动沙箱任务。

        Args:
            job_ctx: 作业上下文，包含：
                - job_id (str): 唯一作业 ID
                - command (str): 要执行的 Python 代码
                - gpu_memory_required_mb (int): GPU 显存需求
                - cpu_cores_required (int): CPU 核心需求
                - slot_id (int, optional): CPU 亲和性槽位
                - priority (int, optional): 优先级

        Returns:
            job_id: 与 ``job_ctx["job_id"]`` 一致

        Raises:
            SandboxLaunchError: systemd-run 执行失败
        """
        job_id: str = job_ctx.get("job_id", "")
        command: str = job_ctx.get("command", "")
        slot_id: int = job_ctx.get("slot_id", 0)

        if not job_id:
            raise SandboxLaunchError("job_id is required")

        if not command:
            raise SandboxLaunchError("command is required", job_id=job_id)

        logger.info(
            "[SandboxManager] 启动沙箱 | job_id={} | slot={} | cmd_len={}",
            job_id, slot_id, len(command),
        )

        limits = self._cgroup_limits()
        scope_name = f"{self._scope_prefix()}_{job_id}.scope"
        log_dir = self._log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{job_id}.log"

        # ── 构造 systemd-run 命令 ────────────────────────────────────
        cmd: list[str] = ["systemd-run", "--user", "--scope"]

        # 资源限制
        mem_bytes = limits.get("memory_limit_bytes", 16 * 1024**3)
        cmd.append(f"--property=MemoryMax={mem_bytes}")

        cpu_quota = limits.get("cpu_quota_percent", 400)
        cmd.append(f"--property=CPUQuota={cpu_quota}%")

        tasks_max = limits.get("tasks_max", 64)
        cmd.append(f"--property=TasksMax={tasks_max}")

        # CPU 亲和性
        affinity = self._config.get("cpu_affinity", {})
        if affinity.get("enabled", False):
            slot = self._cpu_slot(slot_id)
            if slot is not None:
                cores = slot.get("cores", "")
                cmd.append(f"--property=CPUAffinity={cores}")
                logger.info(
                    "[SandboxManager] CPU 亲和性绑定 | job_id={} | cores={}",
                    job_id, cores,
                )

        # 环境变量
        cmd.append("--setenv=CONDA_ENV=sandbox")

        # 单元名称
        cmd.append(f"--unit={scope_name}")

        # 执行命令
        cmd.extend(["conda", "run", "-n", "sandbox", "python", "-c", command])

        # ── systemd-run 不可用时的降级方案 ──────────────────────────
        try:
            use_systemd = self._check_systemd_available()
        except Exception:
            use_systemd = False

        if not use_systemd:
            logger.warning(
                "[SandboxManager] systemd-run 不可用，降级为裸 subprocess | "
                "job_id={}",
                job_id,
            )
            return self._launch_fallback(job_id, command, log_path, limits)

        # ── 启动 systemd-run ─────────────────────────────────────────
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except FileNotFoundError:
            logger.warning(
                "[SandboxManager] systemd-run 未安装，降级为裸 subprocess"
            )
            return self._launch_fallback(job_id, command, log_path, limits)

        # 等待 systemd-run 完成（它会在子进程启动后立即退出）
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise SandboxLaunchError(
                f"systemd-run 启动超时 | job_id={job_id}", job_id=job_id,
            )

        if proc.returncode != 0:
            raise SandboxLaunchError(
                f"systemd-run 启动失败 | returncode={proc.returncode} | "
                f"job_id={job_id}",
                job_id=job_id,
            )

        self._jobs[job_id] = proc

        # ── 启动异步日志线程 ─────────────────────────────────────────
        self._start_log_pusher(job_id, log_path)

        logger.info(
            "[SandboxManager] 沙箱已启动 | job_id={} | scope={} | log={}",
            job_id, scope_name, log_path,
        )
        return job_id

    def terminate(self, job_id: str) -> bool:
        """强制终止沙箱任务并回收资源。

        Args:
            job_id: 作业 ID

        Returns:
            True 如果成功终止，False 如果任务不存在或已退出
        """
        if job_id not in self._jobs:
            logger.warning(
                "[SandboxManager] terminate — 任务不存在 | job_id={}",
                job_id,
            )
            return False

        scope_name = f"{self._scope_prefix()}_{job_id}.scope"
        logger.info(
            "[SandboxManager] 终止沙箱 | job_id={} | scope={}",
            job_id, scope_name,
        )

        # 首选：systemd scope 销毁
        killed = ProcessKiller.kill_scope(scope_name)

        # 降级：直接杀进程
        if not killed:
            proc = self._jobs.get(job_id)
            if proc is not None:
                ProcessKiller.kill_pid_tree(proc.pid)
                killed = True

        # 清理
        self._jobs.pop(job_id, None)
        log_thread = self._log_threads.pop(job_id, None)
        if log_thread is not None and log_thread.is_alive():
            log_thread.join(timeout=2)

        logger.info(
            "[SandboxManager] 沙箱已终止 | job_id={} | killed={}",
            job_id, killed,
        )
        return killed

    def get_logs(self, job_id: str, tail: int = 100) -> list[str]:
        """读取沙箱日志文件。

        Args:
            job_id: 作业 ID
            tail: 返回最后 N 行

        Returns:
            日志行列表，文件不存在时返回空列表
        """
        log_path = self._log_dir() / f"{job_id}.log"
        if not log_path.exists():
            return []

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        return [line.rstrip("\n") for line in lines[-tail:]]

    def is_running(self, job_id: str) -> bool:
        """检查沙箱是否仍在运行。

        通过 systemd 检查 scope 是否活跃。
        """
        scope_name = f"{self._scope_prefix()}_{job_id}.scope"
        try:
            result = subprocess.run(
                ["systemctl", "--user", "is-active", scope_name],
                capture_output=True, text=True, timeout=5,
            )
            return result.stdout.strip() == "active"
        except Exception:
            # 降级：检查 Popen 进程
            proc = self._jobs.get(job_id)
            if proc is not None:
                return proc.poll() is None
            return False

    # ── 内部方法 ──────────────────────────────────────────────────────────

    @staticmethod
    def _check_systemd_available() -> bool:
        """检查 systemd-run --user 是否可用。"""
        try:
            result = subprocess.run(
                ["systemd-run", "--user", "--scope", "--help"],
                capture_output=True, timeout=3,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _launch_fallback(
        self, job_id: str, command: str, log_path: Path, limits: dict,
    ) -> str:
        """降级方案：裸 subprocess.Popen + psutil 资源限制。"""
        log_file = open(log_path, "w", encoding="utf-8")

        proc = subprocess.Popen(
            ["conda", "run", "-n", "sandbox", "python", "-c", command],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )

        self._jobs[job_id] = proc
        self._start_log_pusher(job_id, log_path)

        logger.warning(
            "[SandboxManager] 降级启动（无 systemd）| job_id={} | pid={}",
            job_id, proc.pid,
        )
        return job_id

    def _start_log_pusher(self, job_id: str, log_path: Path) -> None:
        """启动异步日志推送线程。

        将日志文件中的新行推送到 Redis List：
        ``data_plane:logs:{job_id}``
        """
        try:
            import redis as _redis

            redis_client: _redis.Redis | None = _redis.Redis(
                host=os.getenv("REDIS_HOST", "127.0.0.1"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                password=os.getenv("REDIS_PASSWORD", ""),
                db=int(os.getenv("REDIS_DB_LOGS", "1")),
                decode_responses=True,
                socket_connect_timeout=2,
            )
            redis_client.ping()
        except Exception:
            redis_client = None

        interval_ms: int = self._config.get("logging", {}).get(
            "async_push_interval_ms", 100,
        )
        list_key: str = f"{self._redis_prefix()}:{job_id}"

        def _push_loop() -> None:
            last_pos = 0
            while self._jobs.get(job_id) is not None:
                try:
                    if not log_path.exists():
                        time.sleep(interval_ms / 1000.0)
                        continue

                    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(last_pos)
                        new_lines = f.readlines()
                        if new_lines and redis_client is not None:
                            try:
                                redis_client.rpush(
                                    list_key,
                                    *[line.rstrip("\n") for line in new_lines],
                                )
                            except Exception:
                                pass
                        last_pos = f.tell()
                except Exception:
                    pass
                time.sleep(interval_ms / 1000.0)

        thread = threading.Thread(
            target=_push_loop, daemon=True,
            name=f"log-pusher-{job_id}",
        )
        thread.start()
        self._log_threads[job_id] = thread
