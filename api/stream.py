import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from events import Event, EventBus, TERMINAL_EVENT_TYPES
from runtime.manager import TaskManager
from storage.event_repository import EventRepository


def encode_sse(event: Event) -> str:
    payload = {
        "event_id": event.event_id,
        "task_id": event.task_id,
        "node_id": event.node_id,
        "timestamp": event.timestamp.isoformat(),
        "data": event.data,
    }
    return (
        f"id: {event.event_id}\n"
        f"event: {event.event_type.value}\n"
        f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
    )


async def stream_task_events(
    task_id: str,
    event_bus: EventBus,
    event_repository: EventRepository,
    max_queue_size: int = 256,
) -> AsyncIterator[str]:
    """先回放数据库历史，再无缝切换到实时 EventBus 订阅。"""
    subscription = event_bus.subscribe(task_id, max_queue_size=max_queue_size)
    seen: set[str] = set()
    try:
        for event in await event_repository.list_events(task_id):
            seen.add(event.event_id)
            yield encode_sse(event)
            if event.event_type in TERMINAL_EVENT_TYPES:
                return

        async for event in subscription:
            if event.event_id in seen:
                continue
            seen.add(event.event_id)
            yield encode_sse(event)
            if event.event_type in TERMINAL_EVENT_TYPES:
                return
    finally:
        subscription.close()


def create_stream_router(
    task_manager: TaskManager,
    event_bus: EventBus,
    event_repository: EventRepository,
) -> APIRouter:
    router = APIRouter()

    @router.get("/tasks/{task_id}/events")
    async def task_events(task_id: str) -> StreamingResponse:
        if await task_manager.load(task_id) is None:
            raise HTTPException(status_code=404, detail="task not found")
        return StreamingResponse(
            stream_task_events(task_id, event_bus, event_repository),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    return router
