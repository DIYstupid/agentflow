from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionContext:
    """保存一次 Agent 执行过程中的状态（DSEIGN.md §7）。

    所有 Agent State 必须通过 ExecutionContext 显式传递，禁止全局变量：

    - variables:    任务输入与节点写入的共享状态
    - node_outputs: 每个已执行 Node 的输出，key 为 node id
    - metadata:     与 Runtime 语义无关的附加信息
    """

    task_id: str
    variables: dict[str, Any] = field(default_factory=dict)
    node_outputs: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
