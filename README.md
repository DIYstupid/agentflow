# AgentFlow

高并发可恢复 Agent Runtime。不依赖 LangGraph/LangChain 的轻量级 Agent 执行引擎，支持 DAG 工作流、Tool Calling、任务调度、超时重试、Checkpoint、Crash Recovery、SSE 事件流与基础可观测性。

- 语言：Python 3.12+
- 框架：FastAPI + asyncio
- 存储：SQLite（V1）

详细设计：`DSEIGN.md`。

## 进度

- [x] Milestone 1：Graph Engine
  - ExecutionContext / Node / FunctionNode / ConditionNode / ParallelNode
  - Graph / Edge / Graph Validation / GraphExecutor

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
