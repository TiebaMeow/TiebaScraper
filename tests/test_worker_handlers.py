"""Worker 任务处理器测试。"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from conftest import (
    make_comment,
    make_comments_response,
    make_post,
    make_posts_response,
    make_thread,
    make_threads_response,
)
from tiebameow.models.orm import Post as PostModel
from tiebameow.models.orm import Thread as ThreadModel

from src.scraper.queue import UniquePriorityQueue
from src.scraper.tasks import (
    FullScanCommentsTask,
    FullScanPostsTask,
    IncrementalScanCommentsTask,
    IncrementalScanPostsTask,
    ScanThreadsTask,
    Task,
)
from src.scraper.worker import (
    FullScanCommentsTaskHandler,
    FullScanPostsTaskHandler,
    IncrementalScanCommentsTaskHandler,
    IncrementalScanPostsTaskHandler,
    ThreadsTaskHandler,
    Worker,
)


def _drain_queue(queue: UniquePriorityQueue) -> list[Task]:
    """从队列中取出所有任务"""
    items: list[Task] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


def create_mock_container(tb_client=None, redis_client=None, config=None):
    return cast(
        "Any",
        SimpleNamespace(
            tb_client=tb_client,
            redis_client=redis_client,
            config=config,
            tb_clients=None,
            threads_limiter=AsyncMock(),
            posts_limiter=AsyncMock(),
            comments_limiter=AsyncMock(),
            limiter=AsyncMock(),
        ),
    )


# ==================== ThreadsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_threads_handler_new_threads_schedule_and_push():
    """测试 ThreadsTaskHandler 处理新主题帖"""
    Worker._memory_locks.clear()

    thread = make_thread(tid=1, reply_num=5)
    threads_response = make_threads_response([thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    # 新主题帖应该推送到对象流
    datastore.push_object_event.assert_awaited_once_with("thread", thread)

    # 应该生成 FullScanPostsTask
    queued_tasks = _drain_queue(queue)
    assert any(isinstance(item.content, FullScanPostsTask) for item in queued_tasks)


@pytest.mark.asyncio
async def test_threads_handler_new_thread_zero_replies():
    """测试 ThreadsTaskHandler 处理没有回复的新主题帖"""
    Worker._memory_locks.clear()

    thread = make_thread(tid=1, reply_num=0)
    threads_response = make_threads_response([thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    # 没有回复的主题帖应该直接保存元数据
    thread_save_calls = [
        call.args[0]
        for call in datastore.save_items.await_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and isinstance(call.args[0][0], ThreadModel)
    ]
    assert thread_save_calls, "Expected thread models to be persisted for zero-reply thread"

    # 不应该生成 FullScanPostsTask
    queued_tasks = _drain_queue(queue)
    assert not any(isinstance(item.content, FullScanPostsTask) for item in queued_tasks)


@pytest.mark.asyncio
async def test_threads_handler_old_thread_with_updates():
    """测试 ThreadsTaskHandler 处理有更新的旧主题帖"""
    Worker._memory_locks.clear()

    # 旧主题帖：数据库中的 last_time 是 1，新抓取的是 100
    old_last_time = datetime.fromtimestamp(1)
    new_last_time = datetime.fromtimestamp(100)

    thread = make_thread(tid=1, reply_num=10, last_time=new_last_time)
    threads_response = make_threads_response([thread])

    # 模拟数据库中的旧数据（ThreadModel 没有 fname 字段）
    stored_thread = ThreadModel(tid=1, fid=10, reply_num=5, last_time=old_last_time)

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),  # 不是新帖
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[stored_thread]),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))

    # 应该生成 IncrementalScanPostsTask
    queued_tasks = _drain_queue(queue)
    incremental_tasks = [t for t in queued_tasks if isinstance(t.content, IncrementalScanPostsTask)]
    assert len(incremental_tasks) == 1
    assert isinstance(incremental_tasks[0].content, IncrementalScanPostsTask)
    assert incremental_tasks[0].content.tid == thread.tid


@pytest.mark.asyncio
async def test_threads_handler_backfill_enqueues_next_page():
    """测试 ThreadsTaskHandler 在回溯模式下生成下一页任务"""
    Worker._memory_locks.clear()

    thread = make_thread(tid=1, reply_num=5)
    threads_response = make_threads_response([thread], has_more=True, current_page=1, total_page=3)

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1, backfill=True, max_pages=5))

    # 回溯模式不应该推送对象事件
    datastore.push_object_event.assert_not_called()

    # 应该生成下一页的 ScanThreadsTask
    queued_tasks = _drain_queue(queue)
    next_page_tasks = [t for t in queued_tasks if isinstance(t.content, ScanThreadsTask) and t.content.pn == 2]
    assert len(next_page_tasks) == 1
    assert isinstance(next_page_tasks[0].content, ScanThreadsTask)
    assert next_page_tasks[0].content.backfill is True


@pytest.mark.asyncio
async def test_threads_handler_filters_livepost():
    """测试 ThreadsTaskHandler 过滤直播帖"""
    Worker._memory_locks.clear()

    normal_thread = make_thread(tid=1, reply_num=5, is_livepost=False)
    live_thread = make_thread(tid=2, reply_num=5, is_livepost=True)
    threads_response = make_threads_response([normal_thread, live_thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={1}),  # 只有 tid=1 是新的
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(ScanThreadsTask(fid=10, fname="bar", pn=1))

    # filter_new_ids 应该只接收过滤后的 tid
    call_args = datastore.filter_new_ids.call_args
    assert 2 not in call_args[0][1]  # 直播帖 tid=2 应该被过滤


# ==================== FullScanPostsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_full_scan_posts_handler_process_page():
    """测试 FullScanPostsTaskHandler 处理回复页面"""
    Worker._memory_locks.clear()

    post = make_post(pid=100, tid=1, floor=2, reply_num=0)
    posts_response = make_posts_response([post])

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={post.pid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(FullScanPostsTask(tid=post.tid))

    # 检查是否保存了回复
    post_save_calls = [
        call.args[0]
        for call in datastore.save_items.await_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and isinstance(call.args[0][0], PostModel)
    ]
    assert post_save_calls, "Expected post models to be persisted"
    assert post_save_calls[0][0].pid == post.pid

    # 检查是否推送了事件
    datastore.push_object_event.assert_awaited_once_with("post", post)


@pytest.mark.asyncio
async def test_full_scan_posts_handler_generates_comment_task():
    """测试 FullScanPostsTaskHandler 在楼中楼超过10条时生成扫描任务"""
    Worker._memory_locks.clear()

    # 回复有超过10条楼中楼
    post = make_post(pid=100, tid=1, floor=2, reply_num=15)
    posts_response = make_posts_response([post])

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={post.pid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(FullScanPostsTask(tid=post.tid))

    # 应该生成 FullScanCommentsTask
    queued_tasks = _drain_queue(queue)
    comment_tasks = [t for t in queued_tasks if isinstance(t.content, FullScanCommentsTask)]
    assert len(comment_tasks) == 1
    assert isinstance(comment_tasks[0].content, FullScanCommentsTask)
    assert comment_tasks[0].content.pid == post.pid


@pytest.mark.asyncio
async def test_full_scan_posts_handler_updates_thread_metadata():
    """测试 FullScanPostsTaskHandler 在完成后更新主题帖元数据"""
    Worker._memory_locks.clear()

    thread = make_thread(tid=1, reply_num=5)
    post = make_post(pid=100, tid=1, floor=2)
    posts_response = make_posts_response([post])

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(return_value=posts_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = FullScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(FullScanPostsTask(tid=1, thread_dto=thread))

    # 应该保存主题帖元数据
    thread_save_calls = [
        call
        for call in datastore.save_items.await_args_list
        if call.args and isinstance(call.args[0], list) and call.args[0] and isinstance(call.args[0][0], ThreadModel)
    ]
    assert thread_save_calls, "Expected thread metadata to be updated"


# ==================== IncrementalScanPostsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_incremental_scan_posts_handler():
    """测试 IncrementalScanPostsTaskHandler 增量扫描"""
    Worker._memory_locks.clear()

    stored_last_time = datetime.fromtimestamp(1)
    new_post_time = datetime.fromtimestamp(100)

    # 新回复（时间戳大于 stored_last_time）
    new_post = make_post(pid=200, tid=1, floor=3, create_time=new_post_time)
    posts_response = make_posts_response([new_post], current_page=1, total_page=1)

    async def mock_get_posts_dto(*args, **kwargs):
        return posts_response

    tb_client = SimpleNamespace(get_posts_dto=AsyncMock(side_effect=mock_get_posts_dto))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={new_post.pid}),
        save_items=AsyncMock(),
        get_posts_by_pids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
        update_thread_metadata=AsyncMock(),
    )

    class MockConfig:
        deep_scan_enabled = False
        deep_scan_depth = 3

    queue = UniquePriorityQueue()
    handler = IncrementalScanPostsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client, config=MockConfig()),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    target_last_time = datetime.fromtimestamp(200)
    await handler.handle(
        IncrementalScanPostsTask(
            tid=1,
            stored_last_time=stored_last_time,
            stored_reply_num=5,
            target_last_time=target_last_time,
            target_reply_num=10,
        )
    )

    # 应该更新主题帖元数据
    datastore.update_thread_metadata.assert_awaited()


# ==================== FullScanCommentsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_full_scan_comments_handler():
    """测试 FullScanCommentsTaskHandler 全量扫描楼中楼"""
    comment = make_comment(cid=1000, pid=100, tid=1)
    comments_response = make_comments_response([comment])

    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=comments_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={comment.cid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(FullScanCommentsTask(tid=1, pid=100))

    # 应该推送楼中楼事件
    datastore.push_object_event.assert_awaited_with("comment", comment)


@pytest.mark.asyncio
async def test_full_scan_comments_handler_backfill_no_push():
    """测试 FullScanCommentsTaskHandler 回溯模式不推送事件"""
    comment = make_comment(cid=1000, pid=100, tid=1)
    comments_response = make_comments_response([comment])

    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=comments_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={comment.cid}),
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = FullScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(FullScanCommentsTask(tid=1, pid=100, backfill=True))

    # 回溯模式不应该推送事件
    datastore.push_object_event.assert_not_called()


# ==================== IncrementalScanCommentsTaskHandler 测试 ====================


@pytest.mark.asyncio
async def test_incremental_scan_comments_handler():
    """测试 IncrementalScanCommentsTaskHandler 增量扫描楼中楼"""
    new_comment = make_comment(cid=2000, pid=100, tid=1)
    old_comment = make_comment(cid=1000, pid=100, tid=1)
    comments_response = make_comments_response([new_comment, old_comment], current_page=1, total_page=1)

    tb_client = SimpleNamespace(get_comments_dto=AsyncMock(return_value=comments_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={new_comment.cid}),  # 只有 cid=2000 是新的
        save_items=AsyncMock(),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()
    handler = IncrementalScanCommentsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    await handler.handle(IncrementalScanCommentsTask(tid=1, pid=100))

    # 应该只推送新楼中楼
    datastore.push_object_event.assert_awaited_with("comment", new_comment)


# ==================== Worker 测试 ====================


@pytest.mark.asyncio
async def test_worker_processes_task():
    """测试 Worker 处理任务"""
    Worker._memory_locks.clear()

    thread = make_thread(tid=1, reply_num=0)
    threads_response = make_threads_response([thread])

    tb_client = SimpleNamespace(get_threads_dto=AsyncMock(return_value=threads_response))
    datastore = SimpleNamespace(
        filter_new_ids=AsyncMock(return_value={thread.tid}),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        push_object_event=AsyncMock(),
    )
    queue = UniquePriorityQueue()

    worker = Worker(
        worker_id=0,
        queue=queue,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
    )

    # 添加任务到队列
    from src.scraper.tasks import Priority

    task = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=thread.fid, fname=thread.fname, pn=1))
    await queue.put(task)

    # 启动 worker 并在处理完一个任务后取消
    async def run_worker_briefly():
        worker_task = asyncio.create_task(worker.run())
        # 等待任务被处理
        await asyncio.sleep(0.1)
        worker_task.cancel()
        try:
            await worker_task
        except (asyncio.CancelledError, ValueError):
            # ValueError 可能由于 task_done() 调用时机问题
            pass

    await run_worker_briefly()

    # 验证任务被处理
    tb_client.get_threads_dto.assert_awaited()


@pytest.mark.asyncio
async def test_worker_memory_lock():
    """测试 Worker 内存锁功能"""
    Worker._memory_locks.clear()

    tb_client = SimpleNamespace()
    datastore = SimpleNamespace()
    queue = UniquePriorityQueue()

    # 创建一个 handler 来测试锁
    handler = ThreadsTaskHandler(
        worker_id=0,
        container=create_mock_container(tb_client=tb_client),
        datastore=cast("Any", datastore),
        queue=queue,
    )

    # 第一次获取锁应该成功
    lock_key = "test:lock:1"
    assert await handler.try_acquire_lock(lock_key, ttl=10)

    # 第二次获取同一个锁应该失败
    assert not await handler.try_acquire_lock(lock_key, ttl=10)

    # 释放锁后应该可以重新获取
    await handler.release_lock(lock_key)
    assert await handler.try_acquire_lock(lock_key, ttl=10)

    # 清理
    await handler.release_lock(lock_key)
