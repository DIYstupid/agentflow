"""Task Runtime 公共 API（Milestone 3）。"""

from typing import Any

__all__ = ["AgentTask", "TaskStatus", "Scheduler", "TaskManager"]


def __getattr__(name: str) -> Any:
    """惰性导出，避免 graph.node -> runtime.context -> runtime 的循环导入。"""
    if name == "TaskManager":
        from runtime.manager import TaskManager

        return TaskManager
    if name == "Scheduler":
        from runtime.scheduler import Scheduler

        return Scheduler
    if name in {"AgentTask", "TaskStatus"}:
        from runtime.task import AgentTask, TaskStatus

        return {"AgentTask": AgentTask, "TaskStatus": TaskStatus}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
