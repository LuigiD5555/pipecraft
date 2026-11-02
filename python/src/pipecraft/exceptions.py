"""Exceptions raised by the thin PipeCraft Python client."""


class PipeCraftError(RuntimeError):
    pass


class PipeCraftCommandError(PipeCraftError):
    def __init__(self, message: str, *, returncode: int | None = None, stderr: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr
