# AgentFlow

AgentFlow 是一个不依赖 LangGraph/LangChain 的轻量级 Agent Runtime。当前已完成 DAG 工作流和 Tool Runtime；任务调度、持久化、Crash Recovery、SSE 与可观测性将按后续 Milestone 实现。

- 语言：Python 3.12+
- 框架：FastAPI + asyncio
- 存储：SQLite（V1）

详细设计：`DESIGN.md`。

## 进度

- [x] Milestone 1：Graph Engine
  - ExecutionContext / Node / FunctionNode / ConditionNode / ParallelNode
  - Graph / Edge / Graph Validation / GraphExecutor

- [x] Milestone 2：Tool Runtime
  - Tool / ToolRegistry / ToolExecutor（Limiter → Timeout → Retry）
  - RetryPolicy（Exponential Backoff）、ToolLimiter（Per-Tool Semaphore）
  - Mock：EchoTool / SleepTool / FailTool / RandomFailTool

- [x] Milestone 3：Task Runtime
  - AgentTask / TaskManager / Scheduler
  - Bounded Queue / Global Concurrency / Backpressure
  - Pending 与 Running Task Cancellation / Task Timeout

- [x] Milestone 4：Persistence
  - SQLite / TaskRepository / CheckpointRepository / EventRepository
  - Task 状态持久化 / 每个成功 Node 后持久化 Checkpoint

- [x] Milestone 5：Recovery
  - 启动扫描 RUNNING Task / 加载最新 Checkpoint
  - 从下一 Node 恢复 / At-least-once Node Execution

- [x] Milestone 6：Event + SSE
  - Event / EventBus / SQLite Event Subscriber
  - Task / Node / Tool 生命周期事件
  - 历史事件回放 + 实时 SSE Stream

## 环境

```bash
python3.12 -m venv .venv        # 或 uv venv .venv --python 3.12
.venv/bin/pip install -r requirements.txt
```

## 测试

```bash
.venv/bin/pytest -v
```

## Demo

```bash
.venv/bin/python scripts/simple_graph_demo.py
```
