"""BackfillTracker 跨通道任务计数器测试。"""

from __future__ import annotations

import asyncio

import pytest

from src.scraper.router import BackfillTracker, QueueRouter
from src.scraper.tasks import (
    FullScanPostsTask,
    Priority,
    ScanThreadsTask,
    Task,
)

# ==================== BackfillTracker 基础测试 ====================


def test_tracker_initial_state():
    """新建 tracker 时计数应为 0"""
    tracker = BackfillTracker()
    assert tracker.pending == 0


@pytest.mark.asyncio
async def test_tracker_put_done_balance():
    """on_put 和 on_done 应保持平衡"""
    tracker = BackfillTracker()
    tracker.on_put()
    assert tracker.pending == 1
    tracker.on_put()
    assert tracker.pending == 2
    tracker.on_done()
    assert tracker.pending == 1
    tracker.on_done()
    assert tracker.pending == 0


@pytest.mark.asyncio
async def test_tracker_wait_idle_immediate():
    """无任务时 wait_idle 应立即返回"""
    tracker = BackfillTracker()
    await asyncio.wait_for(tracker.wait_idle(), timeout=1)
    assert tracker.pending == 0


@pytest.mark.asyncio
async def test_tracker_wait_idle_blocks_until_done():
    """有任务时 wait_idle 应阻塞，直到 on_done 使计数归零"""
    tracker = BackfillTracker()
    tracker.on_put()
    assert tracker.pending == 1

    async def complete_later():
        await asyncio.sleep(0)  # yield to let wait_idle start blocking
        tracker.on_done()

    asyncio.create_task(complete_later())
    await asyncio.wait_for(tracker.wait_idle(), timeout=1)
    assert tracker.pending == 0


@pytest.mark.asyncio
async def test_tracker_wait_idle_dynamic_subtasks():
    """模拟动态子任务生成：父任务完成前先 on_put 子任务，wait_idle 应等待全部完成"""
    tracker = BackfillTracker()
    tracker.on_put()  # parent

    async def process_parent():
        tracker.on_put()  # child generated during parent processing
        tracker.on_done()  # parent done — count is now 1, not 0
        await asyncio.sleep(0)  # yield — wait_idle should still be blocked (count=1)
        tracker.on_done()  # child done — count 0

    asyncio.create_task(process_parent())
    await asyncio.wait_for(tracker.wait_idle(), timeout=1)
    assert tracker.pending == 0


# ==================== QueueRouter 集成测试 ====================


@pytest.mark.asyncio
async def test_router_wait_idle_empty():
    """空队列时 wait_idle 应立即返回"""
    router = QueueRouter()
    await asyncio.wait_for(router.wait_idle(), timeout=1)


@pytest.mark.asyncio
async def test_router_wait_idle_with_consumed_task():
    """入队并消费任务后 wait_idle 应返回"""
    router = QueueRouter()
    task = Task(priority=Priority.HIGH, content=FullScanPostsTask(tid=1))
    await router.put(task)

    q = router.get_queue("posts")
    got = await q.get()
    q.task_done(got)

    await asyncio.wait_for(router.wait_idle(), timeout=1)


@pytest.mark.asyncio
async def test_router_wait_idle_not_done_yet():
    """未调用 task_done 时 wait_idle 不应返回"""
    router = QueueRouter()
    task = Task(priority=Priority.HIGH, content=FullScanPostsTask(tid=1))
    await router.put(task)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(router.wait_idle(), timeout=0.1)


@pytest.mark.asyncio
async def test_router_wait_idle_dynamic_subtasks():
    """模拟 Worker 动态产生子任务：wait_idle 应等待全部（含子任务）完成"""
    router = QueueRouter()
    parent_task = Task(
        priority=Priority.BACKFILL_THREADS,
        content=ScanThreadsTask(fid=1, fname="test", pn=1, backfill=True),
    )
    await router.put(parent_task)

    threads_q = router.get_queue("threads")

    async def worker_simulation():
        got = await threads_q.get()
        # 父任务处理中生成子任务
        child_task = Task(
            priority=Priority.BACKFILL_POSTS,
            content=FullScanPostsTask(tid=100, backfill=True),
        )
        await router.put(child_task)
        threads_q.task_done(got)  # 父完成 — tracker 仍有 1 个子任务

        # 处理子任务
        posts_q = router.get_queue("posts")
        child = await posts_q.get()
        posts_q.task_done(child)

    asyncio.create_task(worker_simulation())
    await asyncio.wait_for(router.wait_idle(), timeout=1)
    assert router.tracker.pending == 0


@pytest.mark.asyncio
async def test_router_dedup_no_counter_leak():
    """去重跳过的任务不应影响计数器"""
    router = QueueRouter()
    t1 = Task(priority=Priority.HIGH, content=FullScanPostsTask(tid=1))
    t2 = Task(priority=Priority.HIGH, content=FullScanPostsTask(tid=1))  # 重复

    await router.put(t1)
    assert router.tracker.pending == 1

    await router.put(t2)  # 去重跳过
    assert router.tracker.pending == 1  # 不变

    q = router.get_queue("posts")
    got = await q.get()
    q.task_done(got)

    assert router.tracker.pending == 0
    await asyncio.wait_for(router.wait_idle(), timeout=1)


@pytest.mark.asyncio
async def test_router_tracker_counts_across_lanes():
    """跨通道任务计数：threads 和 posts 的任务应合并计数"""
    router = QueueRouter()
    thread_task = Task(priority=Priority.HIGH, content=ScanThreadsTask(fid=1, fname="t", pn=1))
    post_task = Task(priority=Priority.MEDIUM, content=FullScanPostsTask(tid=1))

    await router.put(thread_task)
    await router.put(post_task)
    assert router.tracker.pending == 2

    # 完成一个
    tq = router.get_queue("threads")
    got = await tq.get()
    tq.task_done(got)
    assert router.tracker.pending == 1

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(router.wait_idle(), timeout=0.1)

    # 完成另一个
    pq = router.get_queue("posts")
    got = await pq.get()
    pq.task_done(got)
    assert router.tracker.pending == 0

    await asyncio.wait_for(router.wait_idle(), timeout=1)
