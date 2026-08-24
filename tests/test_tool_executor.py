import asyncio
import time

import pytest

from runtime.errors import (
    NonRetryableToolError,
    RetryableToolError,
    ToolTimeout,
)
from tools.base import Tool
from tools.executor import ToolExecutor
from tools.limiter import ToolLimiter
from tools.registry import ToolRegistry
from tools.retry import RetryPolicy


def make_executor(tool, **kwargs):
    registry = ToolRegistry()
    registry.register(tool)
    policy = kwargs.pop("retry_policy", RetryPolicy(base_delay=0.001, max_delay=0.01))
    return ToolExecutor(registry, retry_policy=policy)


class SlowTool(Tool):
    name = "slow"
    timeout = 0.05
    max_retries = 0

    def __init__(self):
        self.calls = 0

    async def execute(self, arguments):
        self.calls += 1
        await asyncio.sleep(10)
        return "done"


async def test_tool_timeout():
    tool = SlowTool()
    executor = make_executor(tool)
    start = time.monotonic()
    with pytest.raises(ToolTimeout, match="slow"):
        await executor.execute("slow", {})
    assert time.monotonic() - start < 1.0  # 0.05s 超时生效，而非等 10s
    assert tool.calls == 1


class FlakyTool(Tool):
    name = "flaky"
    timeout = 5.0
    max_retries = 3

    def __init__(self):
        self.calls = 0

    async def execute(self, arguments):
        self.calls += 1
        if self.calls < 3:
            raise RetryableToolError("transient failure")
        return "ok"


async def test_tool_retry():
    tool = FlakyTool()
    executor = make_executor(tool)
    result = await executor.execute("flaky", {})
    assert result == "ok"
    assert tool.calls == 3  # 失败 2 次后第 3 次成功


class AlwaysFailTool(Tool):
    name = "always_fail"
    timeout = 5.0
    max_retries = 2

    def __init__(self):
        self.calls = 0

    async def execute(self, arguments):
        self.calls += 1
        raise RetryableToolError("always fails")


async def test_retry_exhausted_raises_last_error():
    tool = AlwaysFailTool()
    executor = make_executor(tool)
    with pytest.raises(RetryableToolError, match="always fails"):
        await executor.execute("always_fail", {})
    assert tool.calls == 3  # 1 次原始 + 2 次重试


class BadArgTool(Tool):
    name = "badarg"
    timeout = 5.0
    max_retries = 3

    def __init__(self):
        self.calls = 0

    async def execute(self, arguments):
        self.calls += 1
        raise NonRetryableToolError("invalid arguments")


async def test_tool_non_retryable_error():
    tool = BadArgTool()
    executor = make_executor(tool)
    with pytest.raises(NonRetryableToolError, match="invalid"):
        await executor.execute("badarg", {})
    assert tool.calls == 1  # 不重试


class SlowRetryableTool(Tool):
    name = "slow_retryable"
    timeout = 0.05
    max_retries = 2

    def __init__(self):
        self.calls = 0

    async def execute(self, arguments):
        self.calls += 1
        await asyncio.sleep(10)
        return "done"


async def test_tool_timeout_is_not_retried():
    # 超时直接失败，不进入 Retry（§15 伪代码只捕获 RetryableError）
    tool = SlowRetryableTool()
    executor = make_executor(tool)
    with pytest.raises(ToolTimeout):
        await executor.execute("slow_retryable", {})
    assert tool.calls == 1


class CountingTool(Tool):
    name = "counting"
    timeout = 5.0
    max_retries = 0
    max_concurrency = 2

    def __init__(self):
        self.active = 0
        self.peak = 0

    async def execute(self, arguments):
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0.05)
        finally:
            self.active -= 1
        return "ok"


async def test_tool_concurrency_limit():
    tool = CountingTool()
    executor = make_executor(tool)
    results = await asyncio.gather(
        *[executor.execute("counting", {}) for _ in range(6)]
    )
    assert results == ["ok"] * 6
    assert tool.peak == 2  # max_concurrency=2：并发峰值恰好为 2


async def test_per_tool_limiter_isolation():
    a = CountingTool()
    a.name = "count_a"
    b = CountingTool()
    b.name = "count_b"
    limiter = ToolLimiter()
    assert limiter.acquire("count_a", a.max_concurrency) is not limiter.acquire(
        "count_b", b.max_concurrency
    )
