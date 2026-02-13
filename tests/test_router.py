"""QueueRouter 单元测试。"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.scraper.router import QueueRouter
from src.scraper.tasks import (
    DeepScanTask,
    FullScanCommentsTask,
    FullScanPostsTask,
    IncrementalScanCommentsTask,
    IncrementalScanPostsTask,
    Priority,
    ScanThreadsTask,
    Task,
)

# ==================== 路由映射测试 ====================


@pytest.mark.asyncio
async def test_router_routes_threads_task():
    """ScanThreadsTask 应路由到 threads 通道"""
    router = QueueRouter()
    task = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="test", pn=1))
    await router.put(task)

    assert router.threads_queue.qsize() == 1
    assert router.posts_queue.qsize() == 0
    assert router.comments_queue.qsize() == 0


@pytest.mark.asyncio
async def test_router_routes_posts_tasks():
    """FullScanPostsTask / IncrementalScanPostsTask / DeepScanTask 应路由到 posts 通道"""
    router = QueueRouter()

    tasks = [
        Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=1)),
        Task(
            priority=Priority.MEDIUM,
            content=IncrementalScanPostsTask(
                tid=2,
                stored_last_time=datetime(2024, 1, 1, 0, 0, 0),
                stored_reply_num=0,
                target_last_time=None,
                target_reply_num=10,
            ),
        ),
        Task(priority=Priority.MEDIUM, content=DeepScanTask(tid=3, total_pages=10)),
    ]
    for t in tasks:
        await router.put(t)

    assert router.threads_queue.qsize() == 0
    assert router.posts_queue.qsize() == 3
    assert router.comments_queue.qsize() == 0


@pytest.mark.asyncio
async def test_router_routes_comments_tasks():
    """FullScanCommentsTask / IncrementalScanCommentsTask 应路由到 comments 通道"""
    router = QueueRouter()

    tasks = [
        Task(priority=Priority.LOW, content=FullScanCommentsTask(tid=1, pid=10)),
        Task(priority=Priority.LOW, content=IncrementalScanCommentsTask(tid=1, pid=20)),
    ]
    for t in tasks:
        await router.put(t)

    assert router.threads_queue.qsize() == 0
    assert router.posts_queue.qsize() == 0
    assert router.comments_queue.qsize() == 2


# ==================== resolve_lane 测试 ====================


def test_resolve_lane_unknown_raises():
    """未知任务类型应抛出 ValueError"""
    from dataclasses import dataclass

    @dataclass
    class UnknownContent:
        unique_key: int = 0

    router = QueueRouter()
    task = Task(priority=Priority.HIGH, content=UnknownContent())  # type: ignore
    with pytest.raises(ValueError, match="Unknown task content type"):
        router.resolve_lane(task)


# ==================== put_nowait 测试 ====================


def test_put_nowait():
    """put_nowait 同步入队"""
    router = QueueRouter()
    task = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="t", pn=1))
    router.put_nowait(task)
    assert router.threads_queue.qsize() == 1


# ==================== total_qsize 测试 ====================


@pytest.mark.asyncio
async def test_total_qsize():
    """total_qsize 返回所有通道的总任务数"""
    router = QueueRouter()
    await router.put(Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="t", pn=1)))
    await router.put(Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=1)))
    await router.put(Task(priority=Priority.LOW, content=FullScanCommentsTask(tid=1, pid=10)))

    assert router.total_qsize() == 3


# ==================== get_queue / lanes 测试 ====================


def test_get_queue():
    """get_queue 返回对应通道的队列"""
    router = QueueRouter()
    assert router.get_queue("threads") is router.threads_queue
    assert router.get_queue("posts") is router.posts_queue
    assert router.get_queue("comments") is router.comments_queue


def test_lanes_property():
    """lanes 属性返回所有通道的字典副本"""
    router = QueueRouter()
    lanes = router.lanes
    assert set(lanes.keys()) == {"threads", "posts", "comments"}
    # 应该是副本, 修改不影响内部状态
    lanes.pop("threads")
    assert "threads" in router.lanes


# ==================== 去重测试（继承自 UniquePriorityQueue）====================


@pytest.mark.asyncio
async def test_router_dedup():
    """同一 unique_key 任务不应重复入队"""
    router = QueueRouter()
    t1 = Task(priority=Priority.HIGH, content=FullScanPostsTask(tid=1))
    t2 = Task(priority=Priority.HIGH, content=FullScanPostsTask(tid=1))

    await router.put(t1)
    await router.put(t2)

    assert router.posts_queue.qsize() == 1
