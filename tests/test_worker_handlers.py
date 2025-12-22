import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from tiebameow.models.dto import (
    BaseForumDTO,
    PageInfoDTO,
    PostDTO,
    PostsDTO,
    PostUserDTO,
    ShareThreadDTO,
    ThreadDTO,
    ThreadsDTO,
    ThreadUserDTO,
)
from tiebameow.models.orm import Post as PostModel
from tiebameow.models.orm import Thread as ThreadModel
from tiebameow.schemas.fragments import Fragment, FragTextModel

from src.scraper.tasks import FullScanPostsTask, ScanThreadsTask, Task
from src.scraper.worker import FullScanPostsTaskHandler, ThreadsTaskHandler


def make_user(dto_class, **overrides):
    defaults = {
        "user_id": 1,
        "portrait": "p",
        "user_name": "u",
        "nick_name_new": "n",
        "level": 1,
        "glevel": 1,
        "gender": "UNKNOWN",
        "icons": [],
        "is_bawu": False,
        "is_vip": False,
        "is_god": False,
        "priv_like": "PUBLIC",
        "priv_reply": "ALL",
    }
    if dto_class is PostUserDTO:
        defaults["ip"] = "127.0.0.1"

    defaults.update(overrides)
    return dto_class(**defaults)


def make_contents() -> list[Fragment]:
    return [FragTextModel(text="sample text")]


def make_thread(
    *,
    tid: int = 1,
    fid: int = 10,
    fname: str = "bar",
    last_time: int = 1,
    reply_num: int = 1,
    user: ThreadUserDTO | None = None,
    text: str = "tx",
):
    user = user or make_user(ThreadUserDTO)

    return ThreadDTO(
        tid=tid,
        fid=fid,
        fname=fname,
        pid=tid,
        author_id=user.user_id,
        author=user,
        title="t",
        contents=make_contents(),
        is_good=False,
        is_top=False,
        is_share=False,
        is_hide=False,
        is_livepost=False,
        is_help=False,
        agree_num=0,
        disagree_num=0,
        reply_num=reply_num,
        view_num=0,
        share_num=0,
        create_time=datetime.fromtimestamp(1),
        last_time=datetime.fromtimestamp(last_time),
        thread_type=0,
        tab_id=0,
        share_origin=ShareThreadDTO(pid=0, tid=0, fid=0, fname="", author_id=0, title="", contents=[]),
    )


def _drain_queue(queue):
    items: list[Task] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


@pytest.mark.asyncio
async def test_threads_handler_new_threads_schedule_and_push():
    thread = make_thread()
    page_info = PageInfoDTO(page_size=30, current_page=1, total_page=1, total_count=1, has_more=False, has_prev=False)
    forum = BaseForumDTO(fid=thread.fid, fname=thread.fname)
    threads_response = ThreadsDTO(objs=[thread], page=page_info, forum=forum)

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
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
    page_info = PageInfoDTO(page_size=30, current_page=1, total_page=2, total_count=60, has_more=False, has_prev=False)
    forum = BaseForumDTO(fid=thread.fid, fname=thread.fname)
    threads_response = ThreadsDTO(objs=[thread], page=page_info, forum=forum)

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
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


def make_post(
    *,
    pid: int = 100,
    tid: int = 1,
    fid: int = 10,
    floor: int = 2,
    reply_num: int = 0,
    user: PostUserDTO | None = None,
):
    user = user or make_user(PostUserDTO)
    return PostDTO(
        pid=pid,
        tid=tid,
        fid=fid,
        fname="bar",  # Added fname
        author_id=user.user_id,
        author=user,
        contents=make_contents(),
        sign="",
        comments=[],
        is_aimeme=False,
        is_thread_author=False,
        agree_num=0,
        disagree_num=0,
        reply_num=reply_num,
        create_time=datetime.fromtimestamp(1),
        floor=floor,
    )


@pytest.mark.asyncio
async def test_full_scan_posts_handler_process_page():
    post = make_post()
    page_info = PageInfoDTO(page_size=30, current_page=1, total_page=1, total_count=1, has_more=False, has_prev=False)
    forum = BaseForumDTO(fid=post.fid, fname=post.fname)
    posts_response = PostsDTO(objs=[post], page=page_info, forum=forum)

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        save_items=AsyncMock(),
        push_to_id_queue=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue: asyncio.PriorityQueue[Task] = asyncio.PriorityQueue()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=cast("Any", SimpleNamespace(tb_client=tb_client)),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(FullScanPostsTask(tid=post.tid))

    # Check if posts were saved

    post_save_calls = [
        call.args[0]
        for call in datastore.save_items.await_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and isinstance(call.args[0][0], PostModel)
    ]
    assert post_save_calls, "Expected post models to be persisted"
    assert post_save_calls[0][0].pid == post.pid

    # Check if events were pushed
    datastore.push_to_id_queue.assert_awaited_once_with("post", post.pid)
    datastore.push_object_event.assert_awaited_once_with("post", post)
