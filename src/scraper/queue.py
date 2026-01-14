"""唯一优先级队列模块。

实现了带有去重功能的优先级队列，防止相同任务重复入队。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .tasks import Task


class UniquePriorityQueue:
    """带去重功能的优先级队列。

    该队列维护一个已入队任务的集合，在入队时检查是否已存在相同任务（相同优先级和内容）。
    如果存在，则跳过入队操作。出队时会从集合中移除对应记录。

    Attributes:
        _unique_set: 存储已入队任务的唯一标识集合。
        _q: 内部 asyncio.PriorityQueue
    """

    def __init__(self, maxsize: int = 0):
        self._q = asyncio.PriorityQueue(maxsize=maxsize)
        self._unique_set: set[tuple[int, str, object]] = set()

    def _get_unique_key(self, item: Task) -> tuple[int, str, object]:
        """生成任务的唯一标识键。

        直接调用 Task 对象的 unique_key 属性获取唯一标识。
        """
        return item.unique_key

    def qsize(self) -> int:
        return self._q.qsize()

    def empty(self) -> bool:
        return self._q.empty()

    def full(self) -> bool:
        return self._q.full()

    def put_nowait(self, item: Task):
        """非阻塞入队，带去重检查。"""
        unique_key = self._get_unique_key(item)
        if unique_key in self._unique_set:
            return

        self._unique_set.add(unique_key)
        try:
            self._q.put_nowait(item)
        except Exception:
            self._unique_set.discard(unique_key)
            raise

    async def put(self, item: Task):
        """阻塞入队，带去重检查。"""
        unique_key = self._get_unique_key(item)
        if unique_key in self._unique_set:
            return

        self._unique_set.add(unique_key)
        try:
            await self._q.put(item)
        except Exception:
            self._unique_set.discard(unique_key)
            raise

    async def get(self) -> Task:
        """获取任务，并从去重集合中移除。"""
        item = await self._q.get()
        try:
            unique_key = self._get_unique_key(item)
            self._unique_set.discard(unique_key)
        except Exception:
            pass
        return item

    def get_nowait(self) -> Task:
        item = self._q.get_nowait()
        try:
            unique_key = self._get_unique_key(item)
            self._unique_set.discard(unique_key)
        except Exception:
            pass
        return item

    def task_done(self):
        self._q.task_done()

    async def join(self):
        await self._q.join()
