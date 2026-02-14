from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from main import _recover_pending_tasks
from src.scraper.tasks import FullScanCommentsTask, FullScanPostsTask, IncrementalScanCommentsTask


class DummyRouter:
    def __init__(self) -> None:
        self.tasks: list[Any] = []

    async def put(self, task: Any) -> None:
        self.tasks.append(task)


class DummyDataStore:
    def __init__(self, pending_threads: list[Any], pending_comments: list[Any], threads: list[Any]) -> None:
        self._pending_threads = pending_threads
        self._pending_comments = pending_comments
        self._threads = threads
        self.queried_tids: set[int] | None = None

    async def get_all_pending_thread_scans(self) -> list[Any]:
        return self._pending_threads

    async def get_all_pending_comment_scans(self) -> list[Any]:
        return self._pending_comments

    async def get_threads_by_tids(self, tids: list[int] | set[int]) -> list[Any]:
        self.queried_tids = set(tids)
        return [t for t in self._threads if t.tid in tids]


@pytest.mark.asyncio
async def test_recover_pending_tasks_only_for_responsible_forums() -> None:
    pending_threads = [
        SimpleNamespace(tid=1001, fid=1, backfill=False),
        SimpleNamespace(tid=1002, fid=2, backfill=False),
    ]
    pending_comments = [
        SimpleNamespace(tid=1001, pid=2001, fid=1, backfill=False, task_kind="incremental"),
        SimpleNamespace(tid=1002, pid=2002, fid=2, backfill=False, task_kind="full"),
    ]
    thread_rows = [
        SimpleNamespace(tid=1001, fid=1),
        SimpleNamespace(tid=1002, fid=2),
    ]

    datastore = DummyDataStore(
        pending_threads=pending_threads,
        pending_comments=pending_comments,
        threads=thread_rows,
    )
    router = DummyRouter()

    await _recover_pending_tasks(cast("Any", datastore), cast("Any", router), responsible_fids={1})

    assert datastore.queried_tids is None
    assert len(router.tasks) == 2

    thread_tasks = [t for t in router.tasks if isinstance(t.content, FullScanPostsTask)]
    comment_tasks = [
        t for t in router.tasks if isinstance(t.content, (FullScanCommentsTask, IncrementalScanCommentsTask))
    ]

    assert len(thread_tasks) == 1
    assert thread_tasks[0].content.tid == 1001

    assert len(comment_tasks) == 1
    assert comment_tasks[0].content.tid == 1001
    assert isinstance(comment_tasks[0].content, IncrementalScanCommentsTask)
