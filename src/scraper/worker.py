"""工作器模块。

该模块实现了多种任务处理器和工作器，负责执行爬虫的具体工作。
使用策略模式设计，每种任务类型都有对应的处理器。

主要组件：
- TaskHandler: 任务处理器抽象基类
- ThreadsTaskHandler: 处理贴吧页面扫描任务
- FullPostsTaskHandler: 处理单个主题贴内回复扫描任务
- IncrementalScanPostsTaskHandler: 处理单个主题贴增量回复扫描任务
- FullScanCommentsTaskHandler: 处理单个回复下的楼中楼扫描任务
- IncrementalScanCommentsTaskHandler: 处理单个回复下的楼中楼增量扫描任务
- Worker: 工作器主类，统一调度各种处理器
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from asyncio import PriorityQueue
from typing import TYPE_CHECKING, ClassVar

from aiotieba.enums import PostSortType
from tiebameow.models.orm import Comment, Post, Thread, User
from tiebameow.utils.logger import logger

from ..core import Container, DataStore
from .tasks import (
    FullScanCommentsTask,
    FullScanPostsTask,
    IncrementalScanCommentsTask,
    IncrementalScanPostsTask,
    Priority,
    ScanThreadsTask,
    Task,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from tiebameow.models.dto import CommentDTO, CommentsDTO, PostDTO, PostsDTO, ThreadDTO, ThreadsDTO


class TaskHandler(ABC):
    """任务处理器抽象基类。

    Attributes:
        worker_id: Worker ID，用于日志标识。
        container: 依赖注入容器。
        datastore: 数据存储层实例。
        queue: 任务队列，用于生成新的子任务。
        log: 日志记录器。
    """

    def __init__(self, worker_id: int, container: Container, datastore: DataStore, queue: PriorityQueue):
        self.worker_id = worker_id
        self.container = container
        self.datastore = datastore
        self.queue = queue
        self.batch_size = 100
        self.log = logger.bind(name=f"Worker-{worker_id}")

    async def try_acquire_lock(self, key: str, ttl: int = 1800) -> bool:
        """尝试获取锁。优先使用Redis，降级使用内存锁。"""
        if self.container.redis_client:
            return await self.container.redis_client.set(key, "1", ex=ttl, nx=True)
        else:
            now = time.time()
            expired = [k for k, t in Worker._memory_locks.items() if now > t]
            for k in expired:
                Worker._memory_locks.pop(k, None)

            if key in Worker._memory_locks:
                return False

            Worker._memory_locks[key] = now + ttl
            return True

    async def release_lock(self, key: str):
        """释放锁。"""
        if self.container.redis_client:
            await self.container.redis_client.delete(key)
        else:
            Worker._memory_locks.pop(key, None)

    async def ensure_users(self, items: Sequence[ThreadDTO | PostDTO | CommentDTO]):
        """确保用户信息已保存到数据库。

        Args:
            items: 包含用户信息的主题贴、回复或评论列表。
        """
        if not items:
            return

        user_infos = [item.author for item in items if item.author and item.author.user_id]
        if not user_infos:
            return

        all_user_ids = [u.user_id for u in user_infos]
        new_user_ids = await self.datastore.filter_new_ids("user", all_user_ids)

        if not new_user_ids:
            return

        new_users_map = {u.user_id: u for u in user_infos if u.user_id in new_user_ids}
        user_info_models = [User.from_dto(u) for u in new_users_map.values()]

        await self.datastore.save_items(user_info_models)
        self.log.debug("Saved {} new users to DB.", len(user_info_models))

    @abstractmethod
    async def handle(self, task_content):
        """处理任务的抽象方法"""
        raise NotImplementedError


class ThreadsTaskHandler(TaskHandler):
    """处理扫描指定贴吧特定页码的任务处理器。

    负责处理ScanThreadsTask类型的任务，扫描指定贴吧的特定页码，
    获取主题贴列表并分别处理新旧主题贴。
    """

    async def handle(self, task_content: ScanThreadsTask):
        """处理扫描指定贴吧特定页码的任务。

        主要流程：
        1. 获取指定页码的主题贴列表
        2. 过滤掉直播贴
        3. 根据tid去重，分类新旧主题贴
        4. 分别处理新旧主题贴

        Args:
            task_content: ScanThreadsTask实例，包含贴吧名、fid和页码信息。
        """
        fname = task_content.fname
        pn = task_content.pn
        rn = task_content.rn
        is_good = task_content.is_good
        backfill = task_content.backfill
        max_pages = task_content.max_pages

        try:
            threads_data = await self.container.tb_client.get_threads_dto(fname, pn=pn, rn=rn, is_good=is_good)  # type: ignore

            if not threads_data.objs:
                self.log.debug("No threads found for {}吧, pn={}. Task finished.", fname, pn)
                return

            threads_data.objs = [t for t in threads_data.objs if not t.is_livepost]
            all_tids = [t.tid for t in threads_data.objs]

            if task_content.force:
                new_tids = set(all_tids)
                old_tids = set()
                self.log.info(
                    "{}吧, pn={}: Force scan enabled. Processing {} threads as new.", fname, pn, len(all_tids)
                )
            else:
                new_tids = await self.datastore.filter_new_ids("thread", all_tids)
                old_tids = set(all_tids) - new_tids
                self.log.debug("{}吧, pn={}: Found {} threads, {} are new.", fname, pn, len(all_tids), len(new_tids))

            await self._process_new_threads(threads_data, new_tids, backfill)

            await self._process_old_threads(threads_data, old_tids, backfill)

            if backfill and pn < max_pages and threads_data.page.has_more:
                next_task_content = ScanThreadsTask(
                    fid=task_content.fid,
                    fname=fname,
                    pn=pn + 1,
                    rn=rn,
                    is_good=is_good,
                    backfill=backfill,
                    max_pages=max_pages,
                    force=task_content.force,
                )
                await self.queue.put(Task(priority=Priority.BACKFILL, content=next_task_content))
                self.log.info("[{}吧] Scheduled backfill ScanThreadsTask, pn={}", fname, pn + 1)

        except Exception as e:
            self.log.exception("Failed to process ScanThreadsTask for {}吧, pn={}: {}", fname, pn, e)

    async def _process_new_threads(self, threads_data: ThreadsDTO, new_tids: set[int], backfill: bool):
        """处理新发现的主题贴。

        主要流程：
        1. 确保用户信息已保存
        2. 非回溯模式下推送到消费者队列
        3. 生成回复全量扫描任务（携带元数据用于延迟更新）

        Args:
            threads_data: aiotieba返回的Threads对象。
            new_tids: 新主题贴的tid集合。
        """
        if not new_tids:
            return

        new_threads = [t for t in threads_data.objs if t.tid in new_tids]

        await self.ensure_users(new_threads)

        priority = Priority.BACKFILL if backfill else Priority.MEDIUM

        for thread in new_threads:
            lock_key = f"tieba:scrapper:lock:tid:{thread.tid}"
            if not await self.try_acquire_lock(lock_key):
                continue

            if not backfill:
                await self.datastore.push_to_id_queue("thread", thread.tid)
                await self.datastore.push_object_event("thread", thread)
                self.log.debug("Pushed new thread tid={} to ID queue or object stream.", thread.tid)

            new_task_content = FullScanPostsTask(
                tid=thread.tid,
                backfill=backfill,
                thread_dto=thread,
            )
            await self.queue.put(Task(priority=priority, content=new_task_content))
            self.log.info("[{}吧] Scheduled FullScanPostsTask for new tid={}", thread.fname, thread.tid)

    async def _process_old_threads(self, threads_data: ThreadsDTO, old_tids: set[int], backfill: bool):
        """处理已存在的主题贴，检查是否有更新。

        主要流程：
        1. 对比已存储的主题贴的最后回复时间，检查是否有更新
        2. 如果有更新，生成增量扫描任务
        3. 生成回复增量扫描任务（携带元数据用于延迟更新）

        Args:
            threads_data: aiotieba返回的Threads对象。
            old_tids: 已存在主题贴的tid集合。
        """
        if not old_tids:
            return

        old_threads = [t for t in threads_data.objs if t.tid in old_tids]
        stored_threads = await self.datastore.get_threads_by_tids(old_tids)
        stored_threads_map = {t.tid: t for t in stored_threads}

        priority = Priority.BACKFILL if backfill else Priority.HIGH

        for thread_data in old_threads:
            stored_thread = stored_threads_map.get(thread_data.tid)
            if stored_thread and thread_data.last_time > stored_thread.last_time:
                lock_key = f"tieba:scrapper:lock:tid:{thread_data.tid}"
                if not await self.try_acquire_lock(lock_key):
                    continue

                self.log.debug(
                    "Thread tid={} has updates. Reply count: {} -> {}",
                    thread_data.tid,
                    stored_thread.reply_num,
                    thread_data.reply_num,
                )

                update_task_content = IncrementalScanPostsTask(
                    tid=thread_data.tid,
                    last_time=thread_data.last_time,
                    last_floor=stored_thread.reply_num,
                    backfill=backfill,
                    target_last_time=thread_data.last_time,
                    target_reply_num=thread_data.reply_num,
                )
                await self.queue.put(Task(priority=priority, content=update_task_content))
                self.log.info(
                    "[{}吧] Scheduled IncrementalScanPostsTask for updated tid={}",
                    thread_data.fname,
                    thread_data.tid,
                )


class FullScanPostsTaskHandler(TaskHandler):
    """处理全量扫描单个主题贴内容的任务处理器。

    负责处理FullScanPostsTask类型的任务，全量扫描指定主题贴的所有回复内容。
    """

    async def handle(self, task_content: FullScanPostsTask):
        """处理扫描单个主题贴内容的任务。

        主要流程：
        从第一页逐页扫描并处理主题贴的回复内容，直至has_more为False。
        任务完成后，更新主题贴的元数据到数据库。

        Args:
            task_content: ScanPostsTask实例，包含主题贴tid和每页条目数量。
        """
        tid = task_content.tid
        rn = task_content.rn
        pn = 1
        backfill = task_content.backfill

        processed_num = 0

        try:
            while True:
                page = await self.container.tb_client.get_posts_dto(  # type: ignore
                    tid,
                    pn,
                    rn=rn,
                    with_comments=True,
                    comment_sort_by_agree=False,
                    comment_rn=10,
                )

                if not page or not page.objs:
                    self.log.debug("No posts found for tid={}, pn={}. Task finished.", tid, pn)
                    break

                await self._process_posts_on_page(tid, page, backfill)
                processed_num += len(page.objs)

                if not page.page.has_more:
                    self.log.debug("All pages processed for tid={}. Task finished.", tid)
                    break

                if processed_num >= self.batch_size:
                    await asyncio.sleep(0)
                    processed_num = 0

                pn += 1

            # 任务完成后更新元数据
            if task_content.thread_dto:
                t_model = Thread.from_dto(task_content.thread_dto)
                await self.datastore.save_items([t_model], upsert=True)
                self.log.debug("Updated thread metadata for tid={} after full scan.", tid)

        except Exception as e:
            self.log.exception("Failed to process ScanThreadTask for tid={}: {}", tid, e)
        finally:
            await self.release_lock(f"tieba:scrapper:lock:tid:{tid}")

    async def _process_posts_on_page(self, tid: int, posts_page: PostsDTO, backfill: bool):
        """处理单个页面上的所有回复。

        主要流程：
        1. 转换为数据库模型并保存
        2. 非回溯模式下推送到消费者队列
        3. 如果携带的楼中楼超过10条，生成楼中楼全量扫描任务

        Args:
            tid: 主题贴tid
            posts_page: 该页面的回复数据
        """
        posts = [p for p in posts_page.objs if p.floor != 1]

        await self.ensure_users(posts)

        post_models = [Post.from_dto(p) for p in posts]
        await self.datastore.save_items(post_models)

        priority = Priority.BACKFILL if backfill else Priority.LOW

        for post in posts:
            if not backfill:
                await self.datastore.push_to_id_queue("post", post.pid)
                await self.datastore.push_object_event("post", post)
                self.log.debug("Pushed post pid={} to ID queue or object stream.", post.pid)

            await self.ensure_users(post.comments)

            comment_models = [Comment.from_dto(c) for c in post.comments]
            await self.datastore.save_items(comment_models)

            if post.reply_num > 10 or len(post.comments) != post.reply_num:
                comment_task = FullScanCommentsTask(tid=tid, pid=post.pid, backfill=backfill)
                await self.queue.put(Task(priority=priority, content=comment_task))
                self.log.info("[{}吧] Scheduled FullScanCommentsTask for pid={} in tid={}", post.fname, post.pid, tid)


class IncrementalScanPostsTaskHandler(TaskHandler):
    """处理增量扫描单个主题贴内容的任务处理器。

    负责处理IncrementalScanPostsTask类型的任务，增量扫描指定主题贴的回复内容。
    """

    async def handle(self, task_content: IncrementalScanPostsTask):
        """处理增量扫描单个主题贴内容的任务。

        主要流程：
        1. 通过pn=0xFFFF获取最新一页数据
        2. 从最新一页的总页数开始，逐页倒序扫描主题贴的所有回复内容
        3. 任务完成后，更新主题贴的元数据到数据库。

        Args:
            task_content: IncrementalScanPostsTask实例，包含主题贴tid和已知回复数。
        """
        tid = task_content.tid
        last_time = task_content.last_time
        last_floor = task_content.last_floor
        rn = task_content.rn
        backfill = task_content.backfill

        try:
            latest_page = await self.container.tb_client.get_posts_dto(  # type: ignore
                tid,
                pn=0xFFFF,
                rn=rn,
                sort=PostSortType.DESC,
                with_comments=True,
                comment_sort_by_agree=False,
                comment_rn=10,
            )

            if not latest_page or not latest_page.objs:
                self.log.debug("No posts found for tid={}. Task finished.", tid)
                return

            start_pn = latest_page.page.total_page

            self.log.debug("Incremental scan for tid={}. Starting from page {}.", tid, start_pn)

            await self._scan_thread_pages(tid, start_pn, rn, last_time, last_floor, latest_page, backfill)

            self.log.debug("Finished incremental scan for tid={}.", tid)

            # 任务完成后更新元数据
            if task_content.target_last_time and task_content.target_reply_num is not None:
                await self.datastore.update_thread_metadata(
                    tid, task_content.target_last_time, task_content.target_reply_num
                )
                self.log.debug("Updated thread metadata for tid={} after incremental scan.", tid)

        except Exception as e:
            self.log.exception("Failed to process IncrementalScanPostsTask for tid={}: {}", tid, e)
        finally:
            await self.release_lock(f"tieba:scrapper:lock:tid:{tid}")

    async def _scan_thread_pages(
        self,
        tid: int,
        start_pn: int,
        rn: int,
        last_time: datetime,
        last_floor: int,
        initial_page_data: PostsDTO,
        backfill: bool,
    ):
        """倒序扫描主题贴回复页面。

        从指定的起始页开始，逐页倒序扫描主题贴的所有回复内容。
        默认扫描一次最新一页数据和热门序第一页数据。

        Args:
            tid: 主题贴tid
            start_pn: 扫描起始页码
            rn: 每页条目数量
            last_time: 上次扫描的最新回复时间戳
            last_floor: 上次扫描的最新楼层
            initial_page_data: 最新一页的数据
        """
        for pn in range(start_pn, 0, -1):
            self.log.debug("Scanning tid={}, Posts page {}.", tid, pn)

            if pn == start_pn and initial_page_data:
                posts_page = initial_page_data
            else:
                posts_page = await self.container.tb_client.get_posts_dto(  # type: ignore
                    tid,
                    pn,
                    rn=rn,
                    with_comments=True,
                    comment_sort_by_agree=False,
                    comment_rn=10,
                )

            if not posts_page or not posts_page.objs:
                self.log.warning("No posts found on tid={}, pn={}. Skipping page.", tid, pn)
                continue

            has_more = await self._process_posts_on_page(last_time, last_floor, posts_page, backfill)

            if not has_more:
                self.log.debug("No new posts found on tid={}, pn={}. Stopping scan.", tid, pn)
                break

        hot_page = await self.container.tb_client.get_posts_dto(  # type: ignore
            tid,
            pn=1,
            rn=rn,
            sort=PostSortType.HOT,
            with_comments=True,
            comment_sort_by_agree=False,
            comment_rn=10,
        )

        if hot_page and hot_page.objs:
            self.log.debug("Hot posts for tid={} found on page 1. Processing hot posts.", tid)
            await self._process_posts_on_page(last_time, last_floor, hot_page, backfill)

    async def _process_posts_on_page(
        self, last_time: datetime, last_floor: int, posts_page: PostsDTO, backfill: bool
    ) -> bool:
        """处理单个页面上的所有回复。

        主要流程：
        1. 过滤出新回复和旧回复
        2. 对新回复进行处理：保存到数据库、推送到消费者队列、生成评论扫描任务
        3. 对旧回复进行处理：检查是否有更新并生成增量扫描任务
        4. 处理所有回复附带的楼中楼
        5. 判断是否还有新回复需要继续扫描。

        Args:
            pn: 页码
            last_time: 上次扫描的最后回复时间戳
            last_floor: 上次扫描的最后楼层
            posts_page: 该页面的回复数据

        Returns:
            bool: 是否还有新回复需要继续扫描
        """
        posts = [p for p in posts_page.objs if p.floor != 1]
        all_pids_on_page = [p.pid for p in posts]
        new_pids = await self.datastore.filter_new_ids("post", all_pids_on_page)
        old_pids = set(all_pids_on_page) - new_pids

        tid = posts_page.objs[0].tid if posts_page.objs else None
        current_page = posts_page.page.current_page

        if not new_pids:
            if tid is not None:
                self.log.debug(
                    "No new posts on tid={}, pn={}.",
                    tid,
                    current_page,
                )
            return False

        if tid is not None:
            self.log.debug(
                "tid={}, pn={}: Found {} new posts.",
                tid,
                current_page,
                len(new_pids),
            )

        await self._process_new_posts(new_pids, posts, backfill)

        await self._process_old_posts(old_pids, posts, backfill)

        await self._process_attached_comments(posts)

        if any(p.create_time < last_time or p.floor < last_floor for p in posts_page.objs):
            return False

        return True

    async def _process_new_posts(self, new_pids: set[int], posts: list[PostDTO], backfill: bool):
        """处理新发现的回复。

        主要流程：
        1. 转换为数据库模型并保存
        2. 非回溯模式下推送到消费者队列
        3. 如果携带的楼中楼超过10条，生成楼中楼全量扫描任务

        Args:
            new_pids: 新回复的pid集合
            posts_page: 该页面的回复数据
        """
        if not new_pids:
            return

        new_posts = [p for p in posts if p.pid in new_pids]

        await self.ensure_users(new_posts)

        new_post_models = [Post.from_dto(p) for p in new_posts]
        await self.datastore.save_items(new_post_models)
        self.log.debug("Saved {} new posts to DB.", len(new_posts))

        priority = Priority.BACKFILL if backfill else Priority.LOW

        for post in new_posts:
            if not backfill:
                await self.datastore.push_to_id_queue("post", post.pid)
                await self.datastore.push_object_event("post", post)
                self.log.debug("Pushed new post pid={} to ID queue or object stream.", post.pid)

            if post.reply_num > 10 or len(post.comments) != post.reply_num:
                comment_task = FullScanCommentsTask(tid=post.tid, pid=post.pid, backfill=backfill)
                await self.queue.put(Task(priority=priority, content=comment_task))
                self.log.info(
                    "[{}吧] Scheduled FullScanCommentsTask for new pid={} in tid={}",
                    post.fname,
                    post.pid,
                    post.tid,
                )

    async def _process_old_posts(self, old_pids: set[int], posts: list[PostDTO], backfill: bool):
        """处理已存在的回复，检查是否有更新。

        主要流程：
        1. 对比已存储的回复的reply_num，检查楼中楼是否有更新
        2. 如果有更新，生成楼中楼增量扫描任务

        Args:
            old_pids: 已存在的回复的pid集合
            posts_page: 该页面的回复数据
        """
        if not old_pids:
            return

        old_posts = [p for p in posts if p.pid in old_pids]
        stored_posts = await self.datastore.get_posts_by_pids(old_pids)
        stored_posts_map = {p.pid: p for p in stored_posts}

        posts_to_update = []

        priority = Priority.BACKFILL if backfill else Priority.MEDIUM

        for post_data in old_posts:
            stored_post = stored_posts_map.get(post_data.pid)
            if stored_post and post_data.reply_num > stored_post.reply_num:
                self.log.debug(
                    "Post pid={} has updates. Reply count: {} -> {}",
                    post_data.pid,
                    stored_post.reply_num,
                    post_data.reply_num,
                )
                posts_to_update.append(post_data)

                update_task_content = IncrementalScanCommentsTask(
                    tid=post_data.tid,
                    pid=post_data.pid,
                    backfill=backfill,
                )
                await self.queue.put(Task(priority=priority, content=update_task_content))
                self.log.info(
                    "[{}吧] Scheduled IncrementalScanCommentsTask for updated pid={}",
                    post_data.fname,
                    post_data.pid,
                )

        if posts_to_update:
            post_models = [Post.from_dto(p) for p in posts_to_update]
            await self.datastore.save_items(post_models, upsert=True)

    async def _process_attached_comments(self, posts: list[PostDTO]):
        """处理回复附带的楼中楼。

        对每个回复的楼中楼进行去重处理，保存新楼中楼到数据库。

        Args:
            posts: 包含回复数据的Posts对象。
        """
        for post in posts:
            new_comment_ids = await self.datastore.filter_new_ids("comment", [c.cid for c in post.comments])

            if not new_comment_ids:
                self.log.debug("No new comments for post pid={}.", post.pid)
                continue

            self.log.debug("Post pid={}: Found {} new comments.", post.pid, len(new_comment_ids))
            new_comments = [c for c in post.comments if c.cid in new_comment_ids]

            await self.ensure_users(new_comments)

            new_comment_models = [Comment.from_dto(c) for c in new_comments]
            await self.datastore.save_items(new_comment_models)


class FullScanCommentsTaskHandler(TaskHandler):
    """处理全量扫描单个回复下的楼中楼的任务处理器。

    负责处理ScanCommentsTask类型的任务，全量扫描指定回复下的所有楼中楼。
    """

    async def handle(self, task_content: FullScanCommentsTask):
        """处理扫描单个回复下的楼中楼的任务。

        主要流程：
        1. 获取楼中楼的第一页数据以确定总页数
        2. 逐页扫描直至所有楼中楼都被处理完毕

        Args:
            task_content: ScanPostCommentsTask实例，包含帖子ID和回复ID。
        """
        tid = task_content.tid
        pid = task_content.pid
        backfill = task_content.backfill

        try:
            initial_page_data = await self.container.tb_client.get_comments_dto(tid, pid, pn=1)  # type: ignore

            if not initial_page_data or not initial_page_data.objs:
                self.log.debug("Post pid={} in tid={} has no comments or has been deleted. Task finished.", pid, tid)
                return

            self.log.debug(
                "Scanning comments for pid={}, tid={}. Total pages: {}.", pid, tid, initial_page_data.page.total_page
            )

            await self._scan_comment_pages(tid, pid, initial_page_data, backfill)

            self.log.debug("Finished comment scan for pid={} in tid={}.", pid, tid)

        except Exception as e:
            self.log.exception("Failed to process ScanPostCommentsTask for pid={}, tid={}: {}", pid, tid, e)

    async def _scan_comment_pages(self, tid: int, pid: int, initial_page_data: CommentsDTO, backfill: bool):
        """扫描楼中楼的所有页面。

        Args:
            tid: 主题贴tid
            pid: 回复pid
            total_pages: 楼中楼总页数
            initial_page_data: 第一页楼中楼数据
        """
        total_pages = initial_page_data.page.total_page
        if total_pages < 1:
            total_pages = 1

        processed_num = 0

        for pn in range(1, total_pages + 1):
            self.log.debug("Scanning comments for pid={}, page {}/{}.", pid, pn, total_pages)

            if pn == 1:
                comments_page = initial_page_data
            else:
                comments_page = await self.container.tb_client.get_comments_dto(tid, pid, pn)  # type: ignore

            if not comments_page or not comments_page.objs:
                self.log.warning("No comments found on pid={}, pn={}. Stopping scan.", pid, pn)
                break

            await self._process_comments_on_page(pid, pn, comments_page, backfill)

            processed_num += len(comments_page.objs)

            if processed_num >= self.batch_size:
                await asyncio.sleep(0)
                processed_num = 0

    async def _process_comments_on_page(self, pid: int, pn: int, comments_page: CommentsDTO, backfill: bool):
        """处理单个页面上的所有楼中楼。

        主要流程：
        1. 对页面上的楼中楼进行去重处理，保存新楼中楼到数据库
        2. 非回溯模式下推送到消费者队列

        Args:
            pid: 回复ID。
            pn: 页码。
            comments_page: 该页面的评论数据。
        """
        all_cids_on_page = [c.cid for c in comments_page.objs]
        new_cids = await self.datastore.filter_new_ids("comment", all_cids_on_page)

        if not new_cids:
            self.log.debug("No new comments on pid={}, pn={}.", pid, pn)
            return

        self.log.debug("pid={}, pn={}: Found {} new comments.", pid, pn, len(new_cids))

        new_comments_data = [c for c in comments_page.objs if c.cid in new_cids]

        await self.ensure_users(new_comments_data)

        new_comment_models = [Comment.from_dto(c) for c in new_comments_data]
        await self.datastore.save_items(new_comment_models)

        for comment in new_comments_data:
            if not backfill:
                await self.datastore.push_to_id_queue("comment", comment.cid)
                await self.datastore.push_object_event("comment", comment)
                self.log.debug("Pushed comment cid={} to ID queue or object stream.", comment.cid)


class IncrementalScanCommentsTaskHandler(TaskHandler):
    """处理增量扫描单个回复下的楼中楼的任务处理器。

    负责处理IncrementalScanCommentsTask类型的任务，增量扫描指定回复下的楼中楼。
    """

    async def handle(self, task_content: IncrementalScanCommentsTask):
        """处理增量扫描楼中楼的任务。

        主要流程：
        1. 获取指定回复的最新一页数据以确定总页数
        2. 从最新一页的总页数开始，逐页倒序扫描回复下的所有楼中楼

        Args:
            task_content: IncrementalScanCommentsTask实例，包含主题贴tid和回复pid。
        """
        tid = task_content.tid
        pid = task_content.pid
        backfill = task_content.backfill

        try:
            comments_page = await self.container.tb_client.get_comments_dto(tid, pid)  # type: ignore

            if not comments_page or not comments_page.objs:
                self.log.debug("No comments found for pid={} in tid={}. Task finished.", pid, tid)
                return

            total_pages = comments_page.page.total_page

            await self._scan_comment_pages(tid, pid, total_pages, comments_page, backfill)

        except Exception as e:
            self.log.exception("Failed to process IncrementalScanCommentsTask for pid={}, tid={}: {}", pid, tid, e)

    async def _scan_comment_pages(
        self,
        tid: int,
        pid: int,
        total_pages: int,
        initial_page_data: CommentsDTO,
        backfill: bool,
    ):
        """扫描楼中楼的所有页面。

        从最后一页开始，逐页倒序扫描指定回复下的所有楼中楼。

        Args:
            tid: 主题贴tid
            pid: 回复pid
            total_pages: 楼中楼总页数
            initial_page_data: 第一页的楼中楼数据
        """
        for pn in range(total_pages, 0, -1):
            self.log.debug("Scanning comments for pid={}, page {}/{}.", pid, pn, total_pages)

            if pn == 1:
                comments_page = initial_page_data
            else:
                comments_page = await self.container.tb_client.get_comments_dto(tid, pid, pn)  # type: ignore

            if not comments_page or not comments_page.objs:
                self.log.warning("No comments found on pid={}, pn={}. Stopping scan.", pid, pn)
                break

            has_more = await self._process_comments_on_page(pid, pn, comments_page, backfill)

            if not has_more:
                self.log.debug("No new comments found on pid={}, pn={}. Stopping scan.", pid, pn)
                break

    async def _process_comments_on_page(self, pid: int, pn: int, comments_page: CommentsDTO, backfill: bool) -> bool:
        """处理单个页面上的所有楼中楼。

        主要流程：
        1. 对页面上的楼中楼进行去重处理，保存新楼中楼到数据库
        2. 非回溯模式下推送到消费者队列

        Args:
            pid: 回复ID。
            pn: 页码。
            comments_page: 该页面的评论数据。
        """
        all_cids_on_page = [c.cid for c in comments_page.objs]
        new_cids = await self.datastore.filter_new_ids("comment", all_cids_on_page)
        old_cids = set(all_cids_on_page) - new_cids

        if not new_cids:
            self.log.debug("No new comments on pid={}, pn={}.", pid, pn)
            return False

        self.log.debug("pid={}, pn={}: Found {} new comments.", pid, pn, len(new_cids))

        new_comments_data = [c for c in comments_page.objs if c.cid in new_cids]

        await self.ensure_users(new_comments_data)

        new_comment_models = [Comment.from_dto(c) for c in new_comments_data]
        await self.datastore.save_items(new_comment_models)

        for comment in new_comments_data:
            if not backfill:
                await self.datastore.push_to_id_queue("comment", comment.cid)
                await self.datastore.push_object_event("comment", comment)
                self.log.debug("Pushed comment cid={} to ID queue.", comment.cid)

        if old_cids:
            return False

        return True


class Worker:
    """工作器类，负责处理任务队列中的任务。

    工作器是爬虫系统的核心执行单元，负责：
    1. 从任务队列中获取任务
    2. 根据任务类型选择相应的处理器
    3. 执行具体的爬取和数据处理逻辑
    4. 处理异常和错误恢复

    每个工作器实例都包含所有类型的任务处理器，可以处理任何类型的任务。

    Attributes:
        worker_id: 工作器的唯一标识ID。
        queue: 任务优先队列。
        container: 依赖注入容器。
        datastore: 数据存储层实例。
        log: 日志记录器。
        handlers: 任务类型到处理器的映射字典。
    """

    _datastore: ClassVar[DataStore | None] = None
    _memory_locks: ClassVar[dict[str, float]] = {}

    def __init__(self, worker_id: int, queue: PriorityQueue, container: Container):
        self.worker_id = worker_id
        self.queue = queue
        self.container = container
        if Worker._datastore is None:
            Worker._datastore = DataStore(container)
        self.datastore = Worker._datastore
        self.log = logger.bind(worker_id=worker_id)

        # 初始化任务处理器
        self.handlers = {
            ScanThreadsTask: ThreadsTaskHandler(worker_id, container, self.datastore, queue),
            FullScanPostsTask: FullScanPostsTaskHandler(worker_id, container, self.datastore, queue),
            IncrementalScanPostsTask: IncrementalScanPostsTaskHandler(worker_id, container, self.datastore, queue),
            FullScanCommentsTask: FullScanCommentsTaskHandler(worker_id, container, self.datastore, queue),
            IncrementalScanCommentsTask: IncrementalScanCommentsTaskHandler(
                worker_id, container, self.datastore, queue
            ),
        }

    @classmethod
    async def close_datastore(cls) -> None:
        """关闭共享 DataStore（用于应用退出时优雅清理）。"""
        if cls._datastore is not None:
            try:
                await cls._datastore.close()
            except Exception:
                pass
            cls._datastore = None

    async def run(self):
        """工作器主循环，持续从队列中获取并处理任务。

        工作器会持续运行，直到收到取消信号。在处理过程中会：
        1. 从优先队列获取任务（阻塞等待）
        2. 根据任务类型调用相应的处理器
        3. 处理异常并记录错误日志
        4. 确保调用task_done()标记任务完成

        该方法支持优雅的取消机制，收到CancelledError时会正常退出。
        """
        self.log.info("Starting...")

        while True:
            try:
                task: Task = await self.queue.get()
                self.log.debug("Got task: {} with priority {}.", task.content.__class__.__name__, task.priority.name)

                await self._process_task(task)

            except asyncio.CancelledError:
                self.log.info("Cancelled. Exiting.")
                break
            except Exception as e:
                self.log.exception("An unexpected error occurred in worker loop: {}", e)
                await asyncio.sleep(1)
            finally:
                if "task" in locals():
                    self.queue.task_done()

    async def _process_task(self, task: Task):
        """处理单个任务。

        根据任务内容的类型选择相应的处理器进行处理。
        如果遇到未知的任务类型，会记录警告日志。

        Args:
            task: 待处理的任务对象。
        """
        content = task.content
        handler = self.handlers.get(type(content))

        if handler:
            await handler.handle(content)
        else:
            self.log.warning("Unknown task type: {}", type(content))
