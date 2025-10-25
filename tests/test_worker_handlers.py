import asyncio
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from src.models.models import Thread as ThreadModel
from src.scraper.tasks import FullScanPostsTask, ScanThreadsTask, Task
from src.scraper.worker import ThreadsTaskHandler


def make_user(**overrides):
    defaults = {"user_id": 1, "level": 1, "portrait": "p", "user_name": "u", "nick_name": "n"}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_thread(
    *,
    tid: int = 1,
    fid: int = 10,
    fname: str = "bar",
    last_time: int = 1,
    reply_num: int = 1,
    user: Any | None = None,
    text: str = "tx",
):
    user = user or make_user()
    contents = SimpleNamespace(text=text, objs=[])
    return SimpleNamespace(
        tid=tid,
        fid=fid,
        fname=fname,
        last_time=last_time,
        reply_num=reply_num,
        user=user,
        title="t",
        text=text,
        create_time=1,
        is_livepost=False,
        contents=contents,
    )


def _drain_queue(queue):
    items: list[Task] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


@pytest.mark.asyncio
async def test_threads_handler_new_threads_schedule_and_push():
    thread = make_thread()
    threads_response = SimpleNamespace(objs=[thread], has_more=False)
    tb_client = SimpleNamespace(get_threads=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_to_id_queue=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue: asyncio.PriorityQueue[Task] = asyncio.PriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=cast("Any", SimpleNamespace(tb_client=tb_client)),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    thread_save_calls = [
        call.args[0]
        for call in datastore.save_items.await_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and isinstance(call.args[0][0], ThreadModel)
    ]
    assert thread_save_calls, "Expected thread models to be persisted for new threads"
    assert thread_save_calls[0][0].tid == thread.tid

    datastore.push_to_id_queue.assert_awaited_once_with("thread", thread.tid)
    datastore.push_object_event.assert_awaited_once_with("thread", thread)

    queued_tasks = _drain_queue(queue)
    assert any(isinstance(item.content, FullScanPostsTask) for item in queued_tasks)


@pytest.mark.asyncio
async def test_threads_handler_backfill_enqueues_next_page_when_has_more_false():
    thread = make_thread()
    threads_response = SimpleNamespace(objs=[thread], has_more=False)
    tb_client = SimpleNamespace(get_threads=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_to_id_queue=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue: asyncio.PriorityQueue[Task] = asyncio.PriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=cast("Any", SimpleNamespace(tb_client=tb_client)),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1, backfill=True, max_pages=2))

    datastore.push_to_id_queue.assert_not_called()
    datastore.push_object_event.assert_not_called()

    queued_tasks = _drain_queue(queue)
    assert any(isinstance(item.content, FullScanPostsTask) and item.content.backfill for item in queued_tasks)
    assert not any(isinstance(item.content, ScanThreadsTask) and item.content.pn == 2 for item in queued_tasks)
