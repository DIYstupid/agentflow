# AgentFlow：高并发可恢复 Agent Runtime

> Version: v0.1  
> Language: Python 3.12+  
> Framework: FastAPI + asyncio  
> Storage: SQLite（V1）  
> Target: 构建一个不依赖 LangGraph 的轻量级 Agent Runtime，支持 Agent 工作流执行、Tool Calling、任务调度、超时重试、Checkpoint、Crash Recovery、流式事件以及基础可观测性。

---

# 1. 项目目标

AgentFlow 不是一个具体的聊天机器人，而是一个用于运行不同 Agent 的通用执行引擎。

系统需要解决以下问题：

1. Agent 如何描述自己的执行流程
2. Agent 节点如何调度
3. Tool 如何注册与执行
4. 多个 Tool 如何并发运行
5. 如何限制下游资源并发
6. Tool 失败后如何 Retry
7. Agent 如何 Timeout / Cancel
8. 服务异常退出后如何恢复任务
9. 如何记录 Agent 的完整执行轨迹
10. 如何通过 SSE 向客户端实时返回执行事件

最终系统应支持：

```text
                  ┌──────────────┐
                  │   FastAPI    │
                  └──────┬───────┘
                         │
                         ▼
                ┌─────────────────┐
                │  Agent Runtime  │
                └────────┬────────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
      Scheduler       Graph Engine   State Store
          │              │              │
          ▼              ▼              ▼
     Tool Runtime    Agent Nodes     Checkpoint
          │                             │
          ▼                             ▼
       Tools                         Event Log
```

AgentFlow V1 只负责：

> **可靠地执行 Agent Workflow。**

不负责：

- 训练模型
- 模型推理框架
- 向量数据库实现
- MCP Server
- 分布式集群
- Kubernetes
- GPU 调度

---

# 2. 设计原则

整个项目必须遵守以下原则。

## 2.1 Runtime 与业务解耦

Runtime 不允许感知：

- Research Agent
- NPC Agent
- Coding Agent

Runtime 只认识：

```text
Graph
Node
Task
Context
Tool
Event
```

例如：

```python
runtime.execute(graph, input)
```

而不是：

```python
runtime.execute_research_agent(...)
```

---

# 2.2 LLM 只是普通 Node

禁止围绕 LLM 设计整个系统。

LLM 在 Runtime 中只是一种 Node：

```text
Node
├── LLMNode
├── ToolNode
├── FunctionNode
├── ConditionNode
└── ParallelNode
```

后续可以增加其他 Node。

---

# 2.3 Runtime 不依赖 LangGraph

V1 不允许使用：

```text
LangGraph
LangChain Agent Executor
CrewAI
AutoGen
```

可以使用官方 LLM SDK。

目的是自己实现：

- Graph
- State Machine
- Scheduler
- Tool Runtime
- Checkpoint

---

# 2.4 所有执行必须可追踪

任何 Node 的执行必须产生事件。

例如：

```text
TASK_STARTED
NODE_STARTED
LLM_STARTED
LLM_COMPLETED
TOOL_STARTED
TOOL_RETRY
TOOL_COMPLETED
NODE_COMPLETED
TASK_COMPLETED
```

不能存在无法观察的执行流程。

---

# 2.5 所有长时间操作必须可取消

包括：

```text
LLM Call
Tool Call
sleep
retry backoff
parallel tasks
```

禁止出现：

```python
while True:
    ...
```

且没有 Cancellation 检查的长期任务。

---

# 3. V1 功能范围

V1 必须实现以下功能：

### Workflow

- DAG Graph
- Node
- Edge
- Condition
- 顺序执行
- 条件分支
- 并行执行

### Task Runtime

- 创建 Task
- Running
- Completed
- Failed
- Cancelled
- Timeout

### Tool Runtime

- Tool Registry
- Tool Schema
- async Tool
- Timeout
- Retry
- Retry Backoff
- Concurrency Limit

### Scheduler

- asyncio Task 调度
- Bounded Queue
- Global concurrency
- Per-Tool concurrency
- Backpressure

### Persistence

- SQLite
- Task State
- Checkpoint
- Event Log

### Recovery

- Runtime 重启
- 加载未完成 Task
- 从最后 Checkpoint 恢复

### Observability

- Trace ID
- Task ID
- Node latency
- Tool latency
- Retry count
- Error

### API

- 创建 Agent Task
- 查询 Task
- Cancel Task
- SSE Event Stream

---

# 4. 非 V1 功能

Coding Agent 不允许在 V1 中自行添加以下功能：

```text
Redis
Kafka
Celery
RabbitMQ
PostgreSQL
Kubernetes
Docker Swarm
gRPC
分布式锁
分布式 Scheduler
复杂 RBAC
Web 前端
MCP
多租户
```

除非后续设计文档明确要求。

原则：

> V1 首先保证 Runtime 核心逻辑正确。

---

# 5. 目录结构

项目使用以下目录：

```text
agentflow/
│
├── app/
│   ├── main.py
│   └── config.py
│
├── api/
│   ├── task.py
│   └── stream.py
│
├── runtime/
│   ├── runtime.py
│   ├── scheduler.py
│   ├── task.py
│   ├── context.py
│   └── cancellation.py
│
├── graph/
│   ├── graph.py
│   ├── node.py
│   ├── edge.py
│   └── executor.py
│
├── tools/
│   ├── base.py
│   ├── registry.py
│   ├── executor.py
│   ├── retry.py
│   └── limiter.py
│
├── llm/
│   ├── base.py
│   └── mock.py
│
├── storage/
│   ├── database.py
│   ├── task_repository.py
│   ├── checkpoint_repository.py
│   └── event_repository.py
│
├── events/
│   ├── event.py
│   └── bus.py
│
├── observability/
│   ├── trace.py
│   └── metrics.py
│
├── agents/
│   ├── simple_agent.py
│   └── research_agent.py
│
├── tests/
│
├── scripts/
│
├── requirements.txt
└── README.md
```

禁止跨层随意引用。

例如：

```text
graph
```

不得依赖：

```text
FastAPI
```

Runtime 核心必须可以脱离 Web Server 单独运行。

---

# 6. 核心领域模型

---

## 6.1 Task

Task 表示一次完整 Agent 执行。

```python
class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
```

Task：

```python
@dataclass
class AgentTask:
    task_id: str
    graph_id: str

    status: TaskStatus

    input: dict
    output: dict | None

    current_node: str | None

    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    error: str | None
```

Task ID 使用：

```text
UUID v4
```

---

# 7. ExecutionContext

ExecutionContext 保存 Agent 执行过程中的状态。

```python
@dataclass
class ExecutionContext:
    task_id: str

    variables: dict[str, Any]

    node_outputs: dict[str, Any]

    metadata: dict[str, Any]
```

例如：

```json
{
  "variables": {
    "question": "Why does LSM Tree need compaction?"
  },

  "node_outputs": {
    "planner": {
      "queries": [
        "LSM Tree compaction"
      ]
    }
  }
}
```

禁止：

```python
global_context = {}
```

所有 Agent State 必须通过：

```text
ExecutionContext
```

显式传递。

---

# 8. Graph 模型

Graph：

```python
class Graph:
    graph_id: str

    nodes: dict[str, Node]

    edges: list[Edge]

    start_node: str
```

示例：

```text
START
  │
  ▼
Planner
  │
  ▼
Search
  │
  ▼
Writer
  │
  ▼
END
```

Graph 必须在执行前 Validate。

检查：

```text
start node 是否存在
edge target 是否存在
是否存在无法到达 Node
普通 DAG 是否存在非法 cycle
Node ID 是否重复
```

---

# 9. Node 模型

Node 基类：

```python
class Node(ABC):

    @abstractmethod
    async def execute(
        self,
        context: ExecutionContext
    ) -> NodeResult:
        ...
```

NodeResult：

```python
@dataclass
class NodeResult:

    output: Any

    next_node: str | None = None
```

V1 实现以下 Node：

```text
FunctionNode
LLMNode
ToolNode
ConditionNode
ParallelNode
```

---

# 10. FunctionNode

用于执行普通 Python 逻辑。

例如：

```python
async def parse_question(context):
    ...
```

包装：

```python
FunctionNode(
    id="parse",
    handler=parse_question
)
```

---

# 11. ConditionNode

用于条件路由。

例如：

```text
          Condition
          /       \
        yes        no
        /           \
     Search        Finish
```

接口：

```python
ConditionNode(
    id="check",
    condition=check_condition,
    branches={
        "yes": "search",
        "no": "finish"
    }
)
```

condition 返回：

```text
branch key
```

例如：

```python
"yes"
```

---

# 12. ParallelNode

支持同时执行多个独立 Node。

例如：

```text
             Search
          /     |      \
         /      |       \
      Google   Wiki    Database
         \      |       /
          \     |      /
              Merge
```

必须使用：

```python
asyncio.gather
```

或者：

```python
asyncio.TaskGroup
```

优先使用：

```text
TaskGroup
```

Python 3.12。

如果任一子任务失败：

V1 默认：

```text
Fail Fast
```

后续再支持 partial success。

---

# 13. Tool 定义

Tool 必须继承统一接口。

```python
class Tool(ABC):

    name: str

    description: str

    timeout: float

    max_retries: int

    max_concurrency: int

    @abstractmethod
    async def execute(
        self,
        arguments: dict
    ) -> Any:
        ...
```

例如：

```python
class SearchTool(Tool):

    name = "search"

    timeout = 10

    max_retries = 3

    max_concurrency = 10
```

---

# 14. Tool Registry

所有 Tool 统一注册：

```python
registry.register(tool)
```

读取：

```python
tool = registry.get("search")
```

重复 Tool Name：

```text
raise ToolAlreadyRegistered
```

不存在：

```text
raise ToolNotFound
```

禁止 Agent 直接：

```python
SearchTool().execute(...)
```

必须通过：

```text
ToolExecutor
```

执行。

---

# 15. Tool Executor

统一执行流程：

```text
Tool Call
    │
    ▼
Registry
    │
    ▼
Concurrency Limiter
    │
    ▼
Timeout
    │
    ▼
Execute
    │
    ├── success
    │
    └── failure
          │
          ▼
        Retry
```

伪代码：

```python
async def execute(tool_name, arguments):

    tool = registry.get(tool_name)

    async with limiter.acquire(tool_name):

        for attempt in range(tool.max_retries + 1):

            try:

                async with asyncio.timeout(tool.timeout):

                    return await tool.execute(arguments)

            except RetryableError:

                await retry_policy.sleep(attempt)
```

---

# 16. Retry

V1 使用：

```text
Exponential Backoff
```

公式：

```text
delay = min(
    base_delay * 2 ^ attempt,
    max_delay
)
```

例如：

```text
0.5s
1s
2s
4s
```

可增加：

```text
random jitter
```

避免 retry storm。

---

# 17. 错误分类

至少定义：

```text
AgentFlowError
│
├── GraphError
├── NodeExecutionError
│
├── ToolError
│   ├── ToolNotFound
│   ├── ToolTimeout
│   ├── RetryableToolError
│   └── NonRetryableToolError
│
├── TaskCancelledError
│
└── TaskTimeoutError
```

只有：

```text
RetryableToolError
```

允许自动 Retry。

参数错误等：

```text
NonRetryable
```

直接失败。

---

# 18. 并发限制

系统实现两层并发限制。

## Global

例如：

```text
MAX_RUNNING_TASKS = 100
```

使用：

```python
asyncio.Semaphore
```

---

## Tool Level

例如：

```text
Search Tool

max_concurrency = 10
```

```text
LLM Tool

max_concurrency = 5
```

维护：

```python
dict[str, asyncio.Semaphore]
```

---

# 19. Scheduler

Scheduler 负责：

```text
Task Queue
Task Admission
Global Concurrency
Task Execution
Cancellation
```

结构：

```text
API
 │
 ▼
Bounded Queue
 │
 ▼
Scheduler
 │
 ├── Worker
 ├── Worker
 ├── Worker
 └── Worker
```

V1 使用：

```python
asyncio.Queue(maxsize=N)
```

禁止使用无限 Queue。

---

# 20. Backpressure

当 Queue 满时：

V1 API 返回：

```text
HTTP 429
```

或者：

```text
HTTP 503
```

推荐：

```text
429 Too Many Requests
```

客户端可以 Retry。

必须记录：

```text
queue_depth
queue_rejected_total
```

---

# 21. Cancellation

每个 Task 必须支持：

```text
cancel(task_id)
```

取消后：

```text
Pending -> Cancelled
Running -> Cancelled
```

正在运行的：

```text
Tool
LLM
Parallel Node
retry sleep
```

必须能响应 cancellation。

API：

```text
POST /tasks/{task_id}/cancel
```

---

# 22. Task Timeout

Task 可以配置：

```text
timeout_seconds
```

例如：

```text
300s
```

外层使用：

```python
asyncio.timeout()
```

超时：

```text
RUNNING
   ↓
FAILED
```

error：

```text
TaskTimeout
```

---

# 23. Event System

所有 Runtime 行为转换为 Event。

Event：

```python
@dataclass
class Event:

    event_id: str

    task_id: str

    event_type: EventType

    node_id: str | None

    timestamp: datetime

    data: dict
```

EventType：

```text
TASK_CREATED

TASK_STARTED

NODE_STARTED

NODE_COMPLETED

NODE_FAILED

TOOL_STARTED

TOOL_RETRY

TOOL_COMPLETED

TOOL_FAILED

TASK_COMPLETED

TASK_FAILED

TASK_CANCELLED
```

---

# 24. Event Bus

Runtime 内部事件统一发送：

```python
await event_bus.publish(event)
```

消费者：

```text
Persistence Subscriber
SSE Subscriber
Metrics Subscriber
Logging Subscriber
```

结构：

```text
                 Runtime
                    │
                    ▼
                EventBus
             /      |      \
            /       |       \
      Database     SSE     Metrics
```

Runtime 不应该直接操作 SSE。

---

# 25. Checkpoint

每个成功完成的 Node 后保存一次 Checkpoint。

Checkpoint：

```python
@dataclass
class Checkpoint:

    task_id: str

    node_id: str

    context: dict

    created_at: datetime
```

流程：

```text
Node Execute
     │
     ▼
Node Result
     │
     ▼
Update Context
     │
     ▼
Save Checkpoint
     │
     ▼
Next Node
```

必须保证：

> Checkpoint 成功后，才能继续执行下一个 Node。

---

# 26. Crash Recovery

Runtime 启动时：

```text
SELECT tasks
WHERE status = RUNNING
```

对于异常终止 Task：

读取：

```text
latest checkpoint
```

恢复：

```text
ExecutionContext
current node
```

继续执行。

流程：

```text
Process Crash

      ↓

Restart

      ↓

RecoveryManager

      ↓

Load Task

      ↓

Load Checkpoint

      ↓

Restore Context

      ↓

Resume
```

---

# 27. V1 Recovery 语义

V1 实现：

> **At-least-once Node Execution**

因此某些 Node 在 Crash 临界区可能执行两次。

例如：

```text
Tool Completed
      ↓
Process Crash
      ↓
Checkpoint 未保存
```

重启以后 Tool 会再次执行。

因此下一阶段必须实现：

```text
Idempotency
```

---

# 28. Tool Idempotency

Tool Call 生成：

```text
tool_call_id
```

建议：

```text
task_id + node_id + call_sequence
```

例如：

```text
a6f3...:send_email:0
```

执行完成后保存：

```text
tool_call_id
result
status
```

Retry / Recovery 时：

先查询：

```text
tool_call_id
```

存在成功结果：

```text
直接复用
```

而不是重新执行。

---

# 29. SQLite 数据模型

至少建立以下表：

```text
tasks

checkpoints

events

tool_calls
```

---

## tasks

字段：

```text
task_id

graph_id

status

input

output

current_node

created_at

started_at

completed_at

error
```

JSON 使用：

```text
TEXT
```

保存。

---

## checkpoints

```text
id

task_id

node_id

context_json

created_at
```

索引：

```text
(task_id, created_at)
```

---

## events

```text
event_id

task_id

event_type

node_id

data_json

created_at
```

索引：

```text
(task_id, created_at)
```

---

## tool_calls

```text
tool_call_id

task_id

node_id

tool_name

arguments_json

result_json

status

created_at

completed_at
```

`tool_call_id`

必须：

```text
UNIQUE
```

---

# 30. Repository Pattern

Runtime 不允许直接写 SQL。

必须经过：

```text
TaskRepository

CheckpointRepository

EventRepository

ToolCallRepository
```

例如：

```python
await task_repository.update_status(
    task_id,
    TaskStatus.RUNNING
)
```

---

# 31. API

---

## 创建 Task

```text
POST /tasks
```

Request：

```json
{
  "graph_id": "research-agent",

  "input": {
    "question": "What is LSM Tree?"
  }
}
```

Response：

```json
{
  "task_id": "...",

  "status": "pending"
}
```

---

## 获取 Task

```text
GET /tasks/{task_id}
```

---

## Cancel

```text
POST /tasks/{task_id}/cancel
```

---

## Event Stream

```text
GET /tasks/{task_id}/events
```

返回：

```text
text/event-stream
```

例如：

```text
event: node_started
data: {...}

event: tool_started
data: {...}

event: tool_completed
data: {...}
```

---

# 32. Trace

一次 Task：

```text
Trace
 │
 ├── Node: Planner
 │      └── LLM Call
 │
 ├── Node: Search
 │      ├── Tool Call #1
 │      ├── Tool Call #2
 │      └── Tool Call #3
 │
 └── Node: Writer
        └── LLM Call
```

必须记录：

```text
start_time

end_time

duration

status

error
```

---

# 33. Metrics

V1 至少维护：

```text
task_total

task_running

task_failed_total

task_latency

node_latency

tool_latency

tool_retry_total

tool_error_total

queue_depth

queue_rejected_total
```

不要求第一版立即接 Prometheus。

可以先实现：

```text
InMemoryMetrics
```

后续替换。

---

# 34. 日志格式

禁止：

```python
print(...)
```

统一使用：

```python
logging
```

推荐 Structured Logging：

```text
task_id

node_id

tool_name

event

duration_ms
```

例如：

```json
{
  "event": "tool_completed",
  "task_id": "abc",
  "node_id": "search",
  "tool": "web_search",
  "duration_ms": 134
}
```

---

# 35. LLM 抽象

LLM 必须通过接口访问。

```python
class LLMClient(ABC):

    @abstractmethod
    async def generate(
        self,
        messages: list[Message]
    ) -> LLMResponse:
        ...
```

V1 首先实现：

```text
MockLLMClient
```

然后才接真实模型。

这样测试 Graph Runtime：

```text
不依赖网络
不消耗 Token
结果可重复
```

---

# 36. 第一版 Demo Agent

第一版不要实现复杂 Multi-Agent。

只实现：

> Simple Research Agent

流程：

```text
START
  │
  ▼
Planner
  │
  ▼
Parallel Search
 ├────┬────┐
 ▼    ▼    ▼
S1    S2    S3
 └────┴────┘
      │
      ▼
    Writer
      │
      ▼
     END
```

目的主要是验证：

```text
Graph

Parallel Node

Tool Runtime

Retry

Checkpoint

SSE

Metrics
```

而不是追求 Research 效果。

---

# 37. Mock Tools

开发阶段实现：

```text
SleepTool

FailTool

EchoTool

RandomFailTool
```

例如：

```python
RandomFailTool(
    failure_rate=0.5
)
```

用于测试：

```text
Retry

Timeout

Cancellation

Recovery
```

---

# 38. 测试策略

至少包含：

```text
Unit Test

Integration Test

Recovery Test

Concurrency Test
```

使用：

```text
pytest

pytest-asyncio
```

---

# 39. 必须存在的 Unit Tests

### Graph

```text
test_graph_validation

test_invalid_edge

test_duplicate_node

test_condition_branch

test_parallel_node
```

### Tool

```text
test_tool_registration

test_tool_not_found

test_tool_timeout

test_tool_retry

test_tool_non_retryable_error

test_tool_concurrency_limit
```

### Runtime

```text
test_task_success

test_task_failure

test_task_cancel

test_task_timeout
```

### Checkpoint

```text
test_checkpoint_save

test_checkpoint_restore
```

---

# 40. Recovery Test

必须存在自动化测试模拟：

```text
Node A
 ↓
Node B
 ↓
Node C
```

执行完成：

```text
Node A
Node B
```

之后模拟 Crash。

重新创建 Runtime：

```text
RecoveryManager.restore()
```

验证：

```text
Node A 不执行

Node B 不执行

Node C 继续执行
```

---

# 41. Concurrency Test

创建：

```text
100
500
1000
```

个模拟 Agent Task。

Tool 使用：

```python
await asyncio.sleep(...)
```

模拟外部 I/O。

测试：

```text
Throughput

P50

P95

P99

Queue Depth

Rejected Requests

Memory
```

结果写入：

```text
benchmark/
```

---

# 42. Benchmark 输出

例如：

```text
Concurrency: 500

Tasks: 10000

Throughput:
1210 tasks/s

Latency:

P50  = 120ms

P95  = 290ms

P99  = 440ms

Rejected:
0.4%

Peak Queue:
814
```

禁止只写：

```text
性能良好。
```

必须有数据。

---

# 43. Graceful Shutdown

收到：

```text
SIGTERM
SIGINT
```

执行：

```text
Stop Accept Task

      ↓

Cancel / Drain Scheduler

      ↓

Persist Checkpoint

      ↓

Close Database

      ↓

Exit
```

不能直接终止事件循环。

---

# 44. Coding 规范

使用：

```text
Python 3.12+

async / await

type hints

dataclass

Enum

ABC
```

禁止滥用：

```text
dict[str, Any]
```

核心领域对象应该定义类型。

---

# 45. 依赖原则

优先使用标准库。

可以使用：

```text
FastAPI

uvicorn

Pydantic

aiosqlite

pytest

pytest-asyncio

httpx
```

V1 不要引入大量第三方框架。

---

# 46. Coding Agent 工作规则

Coding Agent 在实现本项目时必须遵循以下要求。

### Rule 1

不得一次生成整个项目。

必须按照 Milestone 逐阶段开发。

### Rule 2

每完成一个 Milestone：

```text
运行测试
```

确认通过后才能进入下一阶段。

### Rule 3

不得在没有设计文档要求的情况下：

```text
增加 Redis

增加 Celery

增加 Kafka

增加 LangGraph
```

### Rule 4

修改核心接口前必须说明：

```text
修改原因

影响模块

兼容性
```

### Rule 5

每个模块必须：

```text
职责单一

接口明确

可以独立测试
```

### Rule 6

不要为了“架构模式”而增加：

```text
FactoryFactory

复杂 DI 框架

过度抽象
```

优先保证代码可读。

---

# 47. Milestone 1：Graph Engine

实现：

```text
ExecutionContext

Node

FunctionNode

ConditionNode

ParallelNode

Graph

GraphExecutor
```

Demo：

```text
START
 ↓
A
 ↓
Condition
 ↙      ↘
B        C
```

### 验收标准

```text
Graph 可以运行

Condition 可以分支

Parallel 可以并行

异常可以向上传递

测试覆盖核心路径
```

本阶段：

```text
不要 FastAPI
不要 SQLite
不要真实 LLM
```

---

# 48. Milestone 2：Tool Runtime

实现：

```text
Tool

ToolRegistry

ToolExecutor

Timeout

Retry

Concurrency Limit
```

Mock：

```text
EchoTool

SleepTool

RandomFailTool
```

### 验收标准

证明：

```text
Timeout 生效

Retry 生效

Max Concurrency 生效

Non-Retryable 不会重试
```

---

# 49. Milestone 3：Task Runtime

实现：

```text
AgentTask

TaskManager

Scheduler

Bounded Queue

Cancellation

Task Timeout
```

### 验收标准

可以同时提交：

```text
100+
```

模拟 Agent Task。

Queue 满以后产生：

```text
TaskRejected
```

---

# 50. Milestone 4：Persistence

加入：

```text
SQLite

TaskRepository

CheckpointRepository

EventRepository
```

实现：

```text
Node 执行成功后保存 Checkpoint。
```

---

# 51. Milestone 5：Recovery

实现：

```text
RecoveryManager
```

启动时扫描：

```text
RUNNING Task
```

根据 Checkpoint 恢复。

### 必须通过

```text
Crash Recovery Test
```

---

# 52. Milestone 6：Event + SSE

实现：

```text
Event

EventBus

SSE
```

客户端能够实时看到：

```text
Task Started

Node Started

Tool Started

Tool Retry

Tool Completed

Task Completed
```

---

# 53. Milestone 7：Observability

实现：

```text
Trace

Metrics

Structured Logging
```

可以输出：

```text
Task P95

Task P99

Tool P99

Retry Count

Queue Depth
```

---

# 54. Milestone 8：Research Agent Demo

实现：

```text
Planner

Parallel Search

Writer
```

此时再接真实：

```text
LLM API
```

Research Agent 只是：

> AgentFlow 的应用 Demo。

不得反向污染 Runtime。

---

# 55. Milestone 9：Idempotency

实现：

```text
ToolCallStore
```

状态：

```text
PENDING

RUNNING

SUCCESS

FAILED
```

Recovery / Retry：

先查询：

```text
tool_call_id
```

已经 SUCCESS：

```text
return saved_result
```

---

# 56. Milestone 10：Benchmark

建立：

```text
benchmark/run.py
```

自动生成：

```text
CSV / Markdown
```

至少测试：

```text
100 concurrency

500 concurrency

1000 concurrency
```

记录：

```text
Throughput

P50

P95

P99

CPU

Memory

Queue Depth
```

---

# 57. 后续 C++ Runtime 演进

Python V1 完成后：

不得直接把全部代码翻译成 C++。

先进行 Profile。

只有确认以下模块成为性能瓶颈后才考虑下沉：

```text
Scheduler

Executor

Timer Queue

Task Queue
```

目标架构：

```text
                    Python
        ┌────────────────────────┐
        │ Agent                  │
        │ Graph                  │
        │ LLM                    │
        │ RAG                    │
        │ FastAPI                │
        └────────────┬───────────┘
                     │
                   binding
                     │
                     ▼
                  C++
        ┌────────────────────────┐
        │ Executor               │
        │ Thread Pool            │
        │ Bounded Queue          │
        │ Timer Queue            │
        │ Cancellation           │
        │ Metrics                │
        └────────────────────────┘
```

---

# 58. C++ Runtime 推荐研究点

未来 C++ 实现可使用：

```text
C++20
```

重点研究：

```cpp
std::jthread

std::stop_token

std::condition_variable

std::atomic

std::chrono

std::future / promise
```

可以进一步研究：

```text
Work Stealing

Priority Scheduling

Timer Wheel

MPMC Queue
```

但不是 V1 必需。

---

# 59. 最终项目能力目标

完成 AgentFlow 后，应能够解释：

### Agent

```text
Agent Workflow 如何表示？
```

### Scheduler

```text
大量 Agent Task 如何调度？
```

### Backpressure

```text
下游 Tool 被打满怎么办？
```

### Reliability

```text
执行一半 Crash 怎么恢复？
```

### Idempotency

```text
Tool 重试如何避免重复副作用？
```

### Concurrency

```text
为什么需要 Global 与 Per-Tool 两级限流？
```

### Observability

```text
为什么 Agent 慢？
时间花在哪里？
```

### Performance

```text
系统 P99 是多少？
瓶颈在哪里？
```

---

# 60. Definition of Done

项目只有满足以下条件才能认为核心 Runtime 完成：

- [ ] 不依赖 LangGraph 完成 Agent Graph 执行
- [ ] 支持顺序、条件、并行 Node
- [ ] 实现统一 Tool Registry / Tool Runtime
- [ ] Tool 支持 Timeout
- [ ] Tool 支持 Retry
- [ ] Tool 支持并发限制
- [ ] Scheduler 使用 Bounded Queue
- [ ] 实现 Backpressure
- [ ] Task 可以 Cancel
- [ ] Task 支持 Timeout
- [ ] Node 完成后持久化 Checkpoint
- [ ] Runtime 重启可以恢复未完成任务
- [ ] Tool 支持 Idempotency Key
- [ ] 所有 Runtime 行为产生 Event
- [ ] 支持 SSE 实时查看执行过程
- [ ] 支持 Trace
- [ ] 支持基础 Metrics
- [ ] 有单元测试
- [ ] 有 Recovery Test
- [ ] 有 Concurrency Test
- [ ] 有 P50 / P95 / P99 Benchmark
- [ ] Research Agent 可以作为 Demo 正常运行

---

# 61. 给 Coding Agent 的首次任务

Coding Agent 第一次收到本文档后，只执行：

> 实现 Milestone 1：Graph Engine。

具体要求：

1. 初始化 Python 项目结构。
2. 实现 `ExecutionContext`。
3. 实现 `Node` 抽象类。
4. 实现 `FunctionNode`。
5. 实现 `ConditionNode`。
6. 实现 `ParallelNode`。
7. 实现 `Graph`。
8. 实现 `GraphExecutor`。
9. 实现 Graph Validation。
10. 使用 pytest 编写完整单元测试。
11. 创建一个 Simple Graph Demo。
12. 运行测试并确保全部通过。

本阶段禁止实现：

```text
FastAPI

LLM

SQLite

Scheduler

Tool Runtime

LangGraph
```

完成后输出：

```text
项目目录结构

核心设计说明

测试结果

下一阶段建议
```

然后停止，等待下一条指令。

---

# 62. 项目核心理念

AgentFlow 的核心不是：

> “让 LLM 更聪明。”

而是：

> **让不可靠、耗时、存在副作用的 Agent 工作流能够可靠地运行。**

因此本项目最重要的技术关键词是：

```text
Agent Runtime

Workflow

State Machine

Scheduler

Async IO

Backpressure

Retry

Timeout

Cancellation

Checkpoint

Crash Recovery

Idempotency

Observability

Benchmark
```

而不是：

```text
Prompt Engineering

Multi-Agent 对话

角色扮演
```

项目的技术价值应建立在：

> **系统设计与工程实现**

之上。