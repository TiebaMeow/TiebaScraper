"""多通道队列路由器。

根据 API 资源类型将任务路由到不同的 ``UniquePriorityQueue``，
让每条通道拥有独立的 Worker 组，从而利用独立限速最大化吞吐量。

通道映射：
- threads: ``ScanThreadsTask``
- posts:   ``FullScanPostsTask``, ``IncrementalScanPostsTask``, ``DeepScanTask``
- comments: ``FullScanCommentsTask``, ``IncrementalScanCommentsTask``
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

from .queue import UniquePriorityQueue

if TYPE_CHECKING:
    from .tasks import (
        DeepScanTask,
        FullScanCommentsTask,
        FullScanPostsTask,
        IncrementalScanCommentsTask,
        IncrementalScanPostsTask,
        ScanThreadsTask,
        Task,
    )

    type TaskContent = (
        ScanThreadsTask
        | FullScanPostsTask
        | IncrementalScanPostsTask
        | FullScanCommentsTask
        | IncrementalScanCommentsTask
        | DeepScanTask
    )

type Lane = Literal["threads", "posts", "comments"]

# task content 类名 → 通道名
_LANE_MAP: dict[str, Lane] = {
    "ScanThreadsTask": "threads",
    "FullScanPostsTask": "posts",
    "IncrementalScanPostsTask": "posts",
    "DeepScanTask": "posts",
    "FullScanCommentsTask": "comments",
    "IncrementalScanCommentsTask": "comments",
}


class BackfillTracker:
    """跨通道任务计数器。

    追踪所有通道中正在处理（已入队但未完成）的任务总数。用于替代
    ``queue.join()`` 作为回溯模式的完成信号——``join()`` 在队列瞬间
    清空时会提前返回，而 ``BackfillTracker`` 只有在没有任何任务正在
    处理（包括正在动态产生子任务的任务）时才认为完成。

    原理：``on_put`` 在 ``task_done`` 之前调用（子任务在父任务完成前
    入队），因此计数不会在任务处理过程中提前归零。
    """

    def __init__(self) -> None:
        self._count = 0
        self._zero_event = asyncio.Event()
        self._zero_event.set()

    def on_put(self) -> None:
        """任务入队时调用（去重跳过的任务不调用）。"""
        self._count += 1
        self._zero_event.clear()

    def on_done(self) -> None:
        """任务完成时调用。"""
        self._count -= 1
        if self._count <= 0:
            self._count = 0
            self._zero_event.set()

    @property
    def pending(self) -> int:
        """当前正在处理的任务数。"""
        return self._count

    async def wait_idle(self) -> None:
        """等待所有任务完成（计数归零）。"""
        while self._count > 0:
            await self._zero_event.wait()


class QueueRouter:
    """多通道队列路由器。

    维护 3 条独立的 ``UniquePriorityQueue``，提供按通道名或任务类型
    自动路由的 ``put`` 方法。

    Attributes:
        threads_queue: threads 通道队列
        posts_queue: posts 通道队列
        comments_queue: comments 通道队列
        tracker: 跨通道任务计数器
    """

    def __init__(self, maxsize: int = 0) -> None:
        self.tracker = BackfillTracker()
        self.threads_queue = UniquePriorityQueue(lane="threads", maxsize=maxsize, tracker=self.tracker)
        self.posts_queue = UniquePriorityQueue(lane="posts", maxsize=maxsize, tracker=self.tracker)
        self.comments_queue = UniquePriorityQueue(lane="comments", maxsize=maxsize, tracker=self.tracker)
        self._queues: dict[Lane, UniquePriorityQueue] = {
            "threads": self.threads_queue,
            "posts": self.posts_queue,
            "comments": self.comments_queue,
        }

    def get_queue(self, lane: Lane) -> UniquePriorityQueue:
        """获取指定通道的队列。"""
        return self._queues[lane]

    def resolve_lane(self, task: Task) -> Lane:
        """根据任务内容类型解析通道名。"""
        cls_name = type(task.content).__name__
        lane = _LANE_MAP.get(cls_name)
        if lane is None:
            raise ValueError(f"Unknown task content type: {cls_name}")
        return lane

    async def put(self, task: Task) -> None:
        """自动路由任务到对应通道队列。"""
        lane = self.resolve_lane(task)
        await self._queues[lane].put(task)

    def put_nowait(self, task: Task) -> None:
        """非阻塞版本的自动路由入队。"""
        lane = self.resolve_lane(task)
        self._queues[lane].put_nowait(task)

    def total_qsize(self) -> int:
        """所有通道队列的总任务数。"""
        return sum(q.qsize() for q in self._queues.values())

    async def wait_idle(self) -> None:
        """等待所有通道的任务全部完成（含动态产生的子任务）。"""
        await self.tracker.wait_idle()

    @property
    def lanes(self) -> dict[Lane, UniquePriorityQueue]:
        """返回所有通道队列的字典。"""
        return dict(self._queues)
