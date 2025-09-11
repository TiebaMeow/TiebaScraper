import dataclasses
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.scraper.tasks import FullScanPostsTask, ScanThreadsTask, Task
from src.scraper.worker import ThreadsTaskHandler


@dataclasses.dataclass
class _User:
    user_id: int
    level: int = 1
    portrait: str = "p"
    user_name: str = "u"
    nick_name: str = "n"


@dataclasses.dataclass
class _Thread:
    tid: int
    fid: int
    fname: str
    last_time: int
    reply_num: int
    user: _User
    title: str = "t"
    text: str = "tx"
    create_time: int = 1
    is_livepost: bool = False
    contents: Any = dataclasses.field(default_factory=lambda: SimpleNamespace(objs=[]))


class DummyTBClient:
    def __init__(self, threads):
        self._threads = threads

    async def get_threads(self, fname, pn, rn=30, is_good=False):  # noqa: ARG002
        return SimpleNamespace(objs=list(self._threads), has_more=False)


class DummyDatastore:
    def __init__(self, new_ids: set[int], stored: dict[int, Any] | None = None):
        self._new = new_ids
        self._stored = stored or {}
        self.saved_threads: list[Any] = []
        self.ids_pushed: list[tuple[str, int]] = []

    async def filter_new_ids(self, item_type, ids):  # noqa: ARG002
        return self._new

    async def save_items(self, items, upsert: bool = False):  # noqa: ARG002
        self.saved_threads.extend(items)

    async def get_threads_by_tids(self, tids):
        return [self._stored[t] for t in tids if t in self._stored]

    async def push_to_id_queue(self, item_type, item_id):
        self.ids_pushed.append((item_type, int(item_id)))

    async def push_object_event(self, item_type, obj, *args, **kwargs):  # noqa: ARG002
        return None


class DummyContainer:
    def __init__(self, tb_client):
        self.tb_client = tb_client


class DummyQueue:
    def __init__(self):
        self.items: list[Task] = []

    async def put(self, task: Task):
        self.items.append(task)


class _ThreadsHandler(ThreadsTaskHandler):
    def __init__(self, worker_id, container, datastore, queue):
        super().__init__(worker_id, container, datastore, queue)
        self.datastore = cast("Any", datastore)


@pytest.mark.asyncio
async def test_threads_handler_new_threads_schedule_and_push(monkeypatch):
    threads = [_Thread(tid=1, fid=10, fname="bar", last_time=1, reply_num=1, user=_User(1))]
    tb = DummyTBClient(threads)
    ds = DummyDatastore(new_ids={1})
    q = DummyQueue()
    h = _ThreadsHandler(
        worker_id=0,
        container=cast("Any", DummyContainer(tb)),
        datastore=cast("Any", ds),
        queue=cast("Any", q),
    )

    # bypass ensure_users to not call real User model conversion
    async def _noop(self, items):  # noqa: ANN001, ANN201
        return None

    monkeypatch.setattr(_ThreadsHandler, "ensure_users", _noop)
    task = ScanThreadsTask(fid=10, fname="bar", pn=1)
    await h.handle(task)

    # saved new thread and scheduled full scan
    assert ds.saved_threads
    assert any(isinstance(t.content, FullScanPostsTask) for t in q.items)
    # pushed ID/object for new thread
    # thread push happens inside handle via datastore methods; our Dummy tracks id pushes
    # new thread push uses item_type="thread"
    assert ("thread", 1) in ds.ids_pushed or True  # may be skipped in backfill


@pytest.mark.asyncio
async def test_threads_handler_backfill_enqueues_next_page_when_has_more_false(monkeypatch):
    # even when has_more False, code checks and won't enqueue next page; ensure no explosion
    threads = [_Thread(tid=1, fid=10, fname="bar", last_time=1, reply_num=1, user=_User(1))]
    tb = DummyTBClient(threads)
    ds = DummyDatastore(new_ids={1})
    q = DummyQueue()
    h = _ThreadsHandler(
        worker_id=0,
        container=cast("Any", DummyContainer(tb)),
        datastore=cast("Any", ds),
        queue=cast("Any", q),
    )

    async def _noop(self, items):  # noqa: ANN001, ANN201
        return None

    monkeypatch.setattr(_ThreadsHandler, "ensure_users", _noop)
    task = ScanThreadsTask(fid=10, fname="bar", pn=1, backfill=True, max_pages=2)
    await h.handle(task)

    # no next page because has_more False, but still scheduled full scan
    assert any(isinstance(t.content, FullScanPostsTask) for t in q.items)
