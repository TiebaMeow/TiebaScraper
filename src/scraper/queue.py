"""唯一优先级队列模块。

实现了带有去重功能的优先级队列，防止相同任务重复入队。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .tasks import Task


class UniquePriorityQueue(asyncio.PriorityQueue):
    """带去重功能的优先级队列。

    该队列维护一个已入队任务的集合，在入队时检查是否已存在相同任务（相同优先级和内容）。
    如果存在，则跳过入队操作。出队时会从集合中移除对应记录。

    Attributes:
        _unique_set: 存储已入队任务的唯一标识集合。
    """

    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self._unique_set: set[Task] = set()

    def put_nowait(self, item: Task):
        """非阻塞入队，带去重检查。"""
        if item in self._unique_set:
            return

        self._unique_set.add(item)
        try:
            super().put_nowait(item)
        except Exception:
            self._unique_set.discard(item)
            raise

    async def put(self, item: Task):
        """阻塞入队，带去重检查。"""
        if item in self._unique_set:
            return

        self._unique_set.add(item)
        try:
            await super().put(item)
        except Exception:
            self._unique_set.discard(item)
            raise

    def _get(self):
        """
        重写内部方法 _get。
        asyncio.Queue 的 get() 和 get_nowait() 最终都会调用此方法取数据。
        """
        item = super()._get()
        self._unique_set.discard(item)

        return item
