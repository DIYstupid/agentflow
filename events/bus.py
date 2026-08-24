import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from events.event import Event
from runtime.errors import AgentFlowError


EventSubscriber = Callable[[Event], Awaitable[None]]


class EventStreamOverflow(AgentFlowError):
    """实时消费者太慢，其有界缓冲区已经溢出。"""


@dataclass(frozen=True)
class _Closed:
    overflowed: bool = False


class EventSubscription:
    def __init__(self, bus: "EventBus", task_id: str, max_queue_size: int) -> None:
        self._bus = bus
        self.task_id = task_id
        self._queue: asyncio.Queue[Event | _Closed] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._closed = False

    def __aiter__(self) -> "EventSubscription":
        return self

    async def __anext__(self) -> Event:
        item = await self._queue.get()
        if isinstance(item, _Closed):
            if item.overflowed:
                raise EventStreamOverflow(
                    f"event stream for task {self.task_id!r} overflowed"
                )
            raise StopAsyncIteration
        return item

    def close(self) -> None:
        self._bus.unsubscribe(self)

    def _deliver(self, event: Event) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._bus.unsubscribe(self, overflowed=True)

    def _finish(self, overflowed: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        if self._queue.full():
            self._queue.get_nowait()
        self._queue.put_nowait(_Closed(overflowed=overflowed))


class EventBus:
    """Runtime 事件的统一发布入口和按 task 隔离的实时订阅。"""

    def __init__(self) -> None:
        self._subscribers: list[EventSubscriber] = []
        self._streams: dict[str, set[EventSubscription]] = {}
        self._publish_lock = asyncio.Lock()

    def add_subscriber(self, subscriber: EventSubscriber) -> None:
        if subscriber not in self._subscribers:
            self._subscribers.append(subscriber)

    def remove_subscriber(self, subscriber: EventSubscriber) -> None:
        try:
            self._subscribers.remove(subscriber)
        except ValueError:
            pass

    async def publish(self, event: Event) -> None:
        async with self._publish_lock:
            for subscriber in tuple(self._subscribers):
                await subscriber(event)
            for stream in tuple(self._streams.get(event.task_id, ())):
                stream._deliver(event)

    def subscribe(
        self, task_id: str, max_queue_size: int = 256
    ) -> EventSubscription:
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be at least 1")
        subscription = EventSubscription(self, task_id, max_queue_size)
        self._streams.setdefault(task_id, set()).add(subscription)
        return subscription

    def unsubscribe(
        self, subscription: EventSubscription, overflowed: bool = False
    ) -> None:
        streams = self._streams.get(subscription.task_id)
        if streams is not None:
            streams.discard(subscription)
            if not streams:
                self._streams.pop(subscription.task_id, None)
        subscription._finish(overflowed=overflowed)
