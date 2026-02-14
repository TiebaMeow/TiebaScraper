"""多通道队列路由器。

根据 API 资源类型将任务路由到不同的 ``UniquePriorityQueue``，
让每条通道拥有独立的 Worker 组，从而利用独立限速最大化吞吐量。

通道映射：
- threads: ``ScanThreadsTask``
- posts:   ``FullScanPostsTask``, ``IncrementalScanPostsTask``, ``DeepScanTask``
- comments: ``FullScanCommentsTask``, ``IncrementalScanCommentsTask``
"""

from __future__ import annotations

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


class QueueRouter:
    """多通道队列路由器。

    维护 3 条独立的 ``UniquePriorityQueue``，提供按通道名或任务类型
    自动路由的 ``put`` 方法。

    Attributes:
        threads_queue: threads 通道队列
        posts_queue: posts 通道队列
        comments_queue: comments 通道队列
    """

    def __init__(self, maxsize: int = 0) -> None:
        self.threads_queue = UniquePriorityQueue(lane="threads", maxsize=maxsize)
        self.posts_queue = UniquePriorityQueue(lane="posts", maxsize=maxsize)
        self.comments_queue = UniquePriorityQueue(lane="comments", maxsize=maxsize)
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

    @property
    def lanes(self) -> dict[Lane, UniquePriorityQueue]:
        """返回所有通道队列的字典。"""
        return dict(self._queues)
