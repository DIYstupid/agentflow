from runtime.errors import ToolAlreadyRegistered, ToolNotFound
from tools.base import Tool


class ToolRegistry:
    """所有 Tool 统一注册（§14）：register() / get()。

    重复 name → ToolAlreadyRegistered；不存在 → ToolNotFound。
    Agent 禁止直接调用 Tool.execute()，必须经 ToolExecutor。
    """

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        if not tool.name:
            raise ValueError("tool name must not be empty")
        if tool.name in self._tools:
            raise ToolAlreadyRegistered(f"tool {tool.name!r} is already registered")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool:
        try:
            return self._tools[name]
        except KeyError:
            raise ToolNotFound(f"tool {name!r} is not registered") from None
