import asyncio
from dataclasses import dataclass

import pytest

from src.scraper.scheduler import Scheduler
from src.scraper.tasks import Priority, ScanThreadsTask, Task


@dataclass
class DummyConfig:
    scheduler_interval_seconds: int = 0  # avoid real sleep in tests if invoked
    good_page_every_ticks: int = 2
    mode: str = "periodic"
    max_backfill_pages: int = 5
    default_forums: list[str] | None = None
    groups: list | None = None
    pydantic_config: object = None

    def __post_init__(self):
        if self.default_forums is None:
            self.default_forums = ["bar", "baz"]  # Default list for tests
        if self.groups is None:
            self.groups = []

        # Mocking the structure of pydantic_config.tieba.backfill_force_scan
        if self.pydantic_config is None:

            class Tieba:
                backfill_force_scan = False

            class PydanticConfig:
                tieba = Tieba()

            self.pydantic_config = PydanticConfig()


@dataclass
class DummyForum:
    fid: int
    fname: str


@dataclass
class DummyContainer:
    config: DummyConfig
    forums: list[DummyForum]


@pytest.mark.asyncio
async def test_scheduler_backfill_enqueue_start_page_hybrid():
    q: asyncio.PriorityQueue = asyncio.PriorityQueue()
    container = DummyContainer(config=DummyConfig(mode="hybrid"), forums=[DummyForum(1, "bar")])
    s = Scheduler(q, container)  # type: ignore[arg-type]
    await s._schedule_backfill_homepage(container.forums[0], container.config.max_backfill_pages, is_good=False)  # type: ignore
    task = await q.get()
    assert isinstance(task.content, ScanThreadsTask)
    assert task.content.pn == 2  # hybrid start from page 2
    assert task.priority == Priority.BACKFILL


@pytest.mark.asyncio
async def test_scheduler_periodic_enqueues_home_and_good_and_maintenance(monkeypatch):
    q: asyncio.PriorityQueue = asyncio.PriorityQueue()
    cfg = DummyConfig(scheduler_interval_seconds=0)
    container = DummyContainer(config=cfg, forums=[DummyForum(1, "bar"), DummyForum(2, "baz")])
    s = Scheduler(q, container)  # type: ignore[arg-type]

    call = {"cnt": 0}

    async def fake_sleep(_):
        call["cnt"] += 1
        # stop after first loop
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await s._run_periodic()  # type: ignore

    # After one tick, expect: for 2 forums -> 2 homepage, plus good-section 2 homepage
    tasks = []
    while not q.empty():
        tasks.append(q.get_nowait())

    kinds = [type(t.content) for t in tasks]
    assert kinds.count(ScanThreadsTask) == 4


def test_task_priority_ordering():
    t1 = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="x", pn=1))
    t2 = Task(priority=Priority.BACKFILL, content=ScanThreadsTask(fid=1, fname="x", pn=2))

    tasks = sorted([t2, t1])
    assert [t.priority for t in tasks] == [Priority.HIGH, Priority.BACKFILL]
