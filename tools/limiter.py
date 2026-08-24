import asyncio


class ToolLimiter:
    """Tool 级并发限制（§18）：每个 Tool 一个 asyncio.Semaphore。"""

    def __init__(self) -> None:
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._limits: dict[str, int] = {}

    def acquire(self, tool_name: str, max_concurrency: int) -> asyncio.Semaphore:
        if max_concurrency < 1:
            raise ValueError(f"max_concurrency must be >= 1, got {max_concurrency}")
        sem = self._semaphores.get(tool_name)
        if sem is None:
            sem = asyncio.Semaphore(max_concurrency)
            self._semaphores[tool_name] = sem
            self._limits[tool_name] = max_concurrency
        elif self._limits[tool_name] != max_concurrency:
            raise ValueError(
                f"tool {tool_name!r} concurrency limit is already "
                f"{self._limits[tool_name]}, got {max_concurrency}"
            )
        return sem
