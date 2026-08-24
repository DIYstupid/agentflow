import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential Backoff（§16）：delay = min(base_delay * 2^attempt, max_delay)。

    示例（base=0.5, max=4.0）：0.5s / 1s / 2s / 4s / 4s ...
    """

    base_delay: float = 0.5
    max_delay: float = 4.0

    def delay(self, attempt: int) -> float:
        return min(self.base_delay * (2**attempt), self.max_delay)

    async def sleep(self, attempt: int) -> None:
        await asyncio.sleep(self.delay(attempt))
