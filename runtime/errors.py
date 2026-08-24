"""AgentFlow 统一错误层级（DESIGN.md §17）。

只有 RetryableToolError 允许自动 Retry；参数错误等 NonRetryable 直接失败。
"""


class AgentFlowError(Exception):
    """所有 AgentFlow 错误的基类。"""


class GraphError(AgentFlowError):
    """Graph 构建 / 校验 / 执行错误。"""


class NodeExecutionError(AgentFlowError):
    """Node 执行失败（用户 handler 抛出的异常包装）。"""


class ToolError(AgentFlowError):
    """Tool 相关错误基类。"""


class ToolNotFound(ToolError):
    """Registry 中不存在该 Tool（§14）。"""


class ToolAlreadyRegistered(ToolError):
    """重复注册同名 Tool（§14）。"""


class ToolTimeout(ToolError):
    """Tool 执行超过 timeout（§17）。"""


class RetryableToolError(ToolError):
    """允许自动 Retry 的 Tool 错误（§17）。"""


class NonRetryableToolError(ToolError):
    """不允许 Retry 的 Tool 错误，直接失败（§17）。"""


class TaskCancelledError(AgentFlowError):
    """任务被取消（Milestone 3+ 使用）。"""


class TaskTimeoutError(AgentFlowError):
    """任务超时（Milestone 3+ 使用）。"""
