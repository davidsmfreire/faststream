import time

import anyio
import pytest

from faststream._internal.broker.broker import BrokerUsecase
from faststream.confluent import KafkaBroker


class _SlowSubscriber:
    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.stopped = False

    async def stop(self) -> None:
        await anyio.sleep(self.delay)
        self.stopped = True


@pytest.mark.asyncio()
async def test_subscribers_are_stopped_concurrently() -> None:
    broker = KafkaBroker()
    subscribers = [_SlowSubscriber(0.1) for _ in range(10)]
    for subscriber in subscribers:
        broker._subscribers.add(subscriber)

    started = time.monotonic()
    await BrokerUsecase.stop(broker)
    elapsed = time.monotonic() - started

    assert all(subscriber.stopped for subscriber in subscribers)
    assert elapsed < 0.5, (
        f"10 x 0.1s took {elapsed:.2f}s - subscribers stop one at a time"
    )
