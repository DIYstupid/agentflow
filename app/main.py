from fastapi import FastAPI

from api.stream import create_stream_router
from events import EventBus
from runtime.manager import TaskManager
from storage.event_repository import EventRepository


def create_app(
    task_manager: TaskManager,
    event_bus: EventBus,
    event_repository: EventRepository,
) -> FastAPI:
    app = FastAPI(title="AgentFlow", version="0.1.0")
    app.include_router(
        create_stream_router(task_manager, event_bus, event_repository)
    )
    return app
