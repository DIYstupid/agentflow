"""开发期 Mock Tools（§37 / §48），用于测试 Retry / Timeout / Cancellation / Recovery。"""

import asyncio
import random
from typing import Any

from runtime.errors import NonRetryableToolError, RetryableToolError
from tools.base import Tool


class EchoTool(Tool):
    name = "echo"
    description = "Return the input arguments unchanged."
    timeout = 30.0
    max_retries = 0
    max_concurrency = 10

    async def execute(self, arguments: dict[str, Any]) -> Any:
        return arguments


class SleepTool(Tool):
    name = "sleep"
    description = "Sleep for `seconds`, then return."
    timeout = 60.0
    max_retries = 0
    max_concurrency = 10

    async def execute(self, arguments: dict[str, Any]) -> Any:
        if "seconds" not in arguments:
            raise NonRetryableToolError("sleep tool requires 'seconds' argument")
        seconds = float(arguments["seconds"])
        await asyncio.sleep(seconds)
        return {"slept": seconds}


class FailTool(Tool):
    name = "fail"
    description = "Always raise RetryableToolError."
    timeout = 30.0
    max_retries = 0
    max_concurrency = 10

    async def execute(self, arguments: dict[str, Any]) -> Any:
        raise RetryableToolError("fail tool always fails")


class RandomFailTool(Tool):
    name = "random_fail"
    description = "Fail with probability `failure_rate`."
    timeout = 30.0
    max_retries = 0
    max_concurrency = 10

    def __init__(self, failure_rate: float = 0.5) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError(f"failure_rate must be in [0, 1], got {failure_rate}")
        self.failure_rate = failure_rate

    async def execute(self, arguments: dict[str, Any]) -> Any:
        if random.random() < self.failure_rate:
            raise RetryableToolError("random failure")
        return {"failed": False}
