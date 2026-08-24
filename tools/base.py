from abc import ABC, abstractmethod
from typing import Any


class Tool(ABC):
    """Tool 统一接口（§13）。子类通过类属性配置行为，见 mocks 示例。"""

    name: str = ""
    description: str = ""
    timeout: float = 30.0
    max_retries: int = 0
    max_concurrency: int = 1

    @abstractmethod
    async def execute(self, arguments: dict[str, Any]) -> Any:
        """执行 Tool。失败时按 §17 抛 RetryableToolError / NonRetryableToolError。"""
