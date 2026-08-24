import pytest

from runtime.errors import ToolAlreadyRegistered, ToolNotFound
from tools.registry import ToolRegistry
from tools.mocks import EchoTool


def test_tool_registration():
    registry = ToolRegistry()
    tool = EchoTool()
    registry.register(tool)
    assert registry.get("echo") is tool


def test_duplicate_tool_name_raises():
    registry = ToolRegistry()
    registry.register(EchoTool())
    with pytest.raises(ToolAlreadyRegistered, match="echo"):
        registry.register(EchoTool())


def test_tool_not_found():
    registry = ToolRegistry()
    with pytest.raises(ToolNotFound, match="missing"):
        registry.get("missing")
