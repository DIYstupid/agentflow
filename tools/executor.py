import asyncio
from typing import Any

from runtime.errors import RetryableToolError, ToolTimeout
from tools.limiter import ToolLimiter
from tools.registry import ToolRegistry
from tools.retry import RetryPolicy


class ToolExecutor:
    """统一执行流程（§15）：

        Registry -> Concurrency Limiter -> Timeout -> Execute -> (失败) Retry

    - 只有 RetryableToolError 触发自动 Retry（Exponential Backoff）
    - NonRetryableToolError 直接向上传播，不重试
    - 超时抛 ToolTimeout，不重试（超时操作可能已卡死，重试只会堆积）
    - 并发槽位覆盖整个重试循环（下游资源在重试期间仍被占用）
    """

    def __init__(
        self,
        registry: ToolRegistry,
        limiter: ToolLimiter | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.registry = registry
        self.limiter = limiter or ToolLimiter()
        self.retry_policy = retry_policy or RetryPolicy()

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        tool = self.registry.get(tool_name)
        async with self.limiter.acquire(tool_name, tool.max_concurrency):
            for attempt in range(tool.max_retries + 1):
                try:
                    async with asyncio.timeout(tool.timeout) as timeout_scope:
                        return await tool.execute(arguments)
                except TimeoutError:
                    # asyncio.timeout() 和工具/下游 SDK 都可能抛 TimeoutError。
                    # 只有当前 timeout scope 确实到期时才能转换为 ToolTimeout；
                    # 工具自身的 TimeoutError 必须保留，避免错误分类。
                    if timeout_scope.expired():
                        raise ToolTimeout(
                            f"tool {tool.name!r} timed out after {tool.timeout}s"
                        ) from None
                    raise
                except RetryableToolError:
                    if attempt >= tool.max_retries:
                        raise
                    await self.retry_policy.sleep(attempt)
