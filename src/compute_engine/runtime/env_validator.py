"""
Enterprise AI Platform - Module 2 (Runtime Plane)
Path: src/compute_engine/runtime/env_validator.py
Author: 成员 F
Description: Conda 环境静态数字指纹校验系统 (基于 Lock 文件无状态比对)。
"""

import os
import hashlib
import subprocess
import yaml
from pathlib import Path
from loguru import logger

class CriticalEnvMismatchError(RuntimeError):
    """环境指纹校验失败的致命异常"""
    pass

class EnvironmentValidator:
    def __init__(self, workspace_root: str = "."):
        self.root = Path(workspace_root).resolve()
        # 🌟 核心变化一：不再盯精简版的 environment.yml，而是盯死这把沉甸甸的物理锁
        self.lock_path = self.root / "environment.lock.yml"

    def _compute_md5(self, content: str) -> str:
        """计算字符串的 MD5 指纹"""
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def _get_active_env_name(self) -> str:
        """动态读取环境名称（延续上一轮的纠偏）"""
        config_path = self.root / "config" / "module2" / "sandbox_limit.yaml"
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
                return config.get("sandbox_env", {}).get("active_env_name", "sandbox")
        except Exception as e:
            return "sandbox"

    def _clean_conda_export(self, content: str) -> str:
        """
        🌟 核心变化二：强大的噪音清洗器
        Conda export 导出的文件第一行带有系统绝对路径 prefix，且带有时间戳注释
        必须把这些环境强相关的“噪音”清洗掉，只保留核心的依赖树字符串进行哈希。
        """
        return "\n".join(
            line for line in content.splitlines() 
            if line.strip() 
            and not line.startswith("#") 
            and not line.startswith("prefix:")
        )

    def validate_environment(self) -> bool:
        """
        核心校验逻辑：拿当前的运行时快照，与代码库里的 Lock 文件做生死决斗。
        """
        if not self.lock_path.exists():
            logger.warning(f"未找到物理锁文件 {self.lock_path.name}，跳过强校验。请联系运维执行 conda env export 生成。")
            return True

        env_name = self._get_active_env_name()
        
        # 1. 提取锁文件的纯净哈希
        lock_content = self.lock_path.read_text(encoding="utf-8")
        lock_md5 = self._compute_md5(self._clean_conda_export(lock_content))

        # 2. 现场给服务器做 B 超：获取当前真实的运行时物理快照
        try:
            result = subprocess.run(
                f"conda env export -n {env_name}", 
                shell=True, 
                capture_output=True, 
                text=True, 
                check=True
            )
            runtime_md5 = self._compute_md5(self._clean_conda_export(result.stdout))
        except subprocess.CalledProcessError as e:
            logger.error(f"无法获取 Conda 环境 [{env_name}] 的运行时状态: {e.stderr}")
            return False

        # 3. 终极对决：如果现实与契约不符，直接拔除！
        if lock_md5 != runtime_md5:
            error_msg = (
                f"[CRITICAL_ENV_MISMATCH] 致命拦截！\n"
                f"当前黄金基座环境 [{env_name}] 的物理依赖拓扑，与 Git 锁定的 environment.lock.yml 不一致。\n"
                f"原因：有人私自在基座中执行了 pip/conda install，或者未提交最新的 lock 文件。\n"
                f"请立刻使用临时沙箱做测试，或回滚你的违规操作！"
            )
            logger.critical(error_msg)
            raise CriticalEnvMismatchError(error_msg)

        logger.success(f"环境指纹 [{lock_md5[:8]}] 校验通过，底层物理基座绝对纯净。")
        return True