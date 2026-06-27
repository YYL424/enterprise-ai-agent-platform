"""沙箱异常类 — 成员 E 专属的异常体系。"""


class SandboxError(Exception):
    """沙箱执行异常基类。

    所有 E 侧代码必须抛出此异常或其子类，禁止裸 ``raise Exception``。
    """

    def __init__(self, message: str, job_id: str | None = None) -> None:
        super().__init__(message)
        self.job_id = job_id


class SandboxLaunchError(SandboxError):
    """沙箱启动失败（systemd-run 错误、资源不足等）。"""
    pass


class SandboxTerminateError(SandboxError):
    """沙箱终止失败。"""
    pass


class SandboxTimeoutError(SandboxError):
    """沙箱执行超时。"""
    pass


class PrewarmPoolExhaustedError(SandboxError):
    """预热槽位池耗尽。"""
    pass
