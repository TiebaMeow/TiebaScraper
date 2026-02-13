"""数据存储层模块。

该模块提供了统一的数据访问接口，封装了与PostgreSQL数据库和Redis缓存的交互逻辑。
主要功能包括数据去重、批量保存、状态追踪和消息队列管理。
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, ClassVar, Literal

from cashews import Cache, add_prefix
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from tiebameow.models.orm import Comment, Forum, Post, Thread, User
from tiebameow.utils.logger import logger

from .metrics import CACHE_HITS, CACHE_MISSES, DB_OPERATIONS
from .models import PendingCommentScan, PendingThreadScan
from .publisher import EventEnvelope, NoopPublisher, RedisStreamsPublisher, WebSocketPublisher, build_envelope

if TYPE_CHECKING:
    from datetime import datetime

    from tiebameow.models.dto import CommentDTO, PostDTO, ThreadDTO

    from .container import Container

    type ItemType = Literal["thread", "post", "comment"]
    type DTOType = ThreadDTO | PostDTO | CommentDTO
    type PendingCommentTaskKind = Literal["full", "incremental"]


PRIMARY_KEY_MAP = {
    Thread: {"tid", "create_time"},
    Post: {"pid", "create_time"},
    Comment: {"cid", "create_time"},
    Forum: {"fid"},
    User: {"user_id"},
}


class DataStore:
    """数据存储层，负责与数据库和缓存交互。

    提供统一的数据访问接口，包括：
    - 数据去重检查(基于Redis集合)
    - 批量数据保存(PostgreSQL)
    - 处理状态标记
    - 消息队列推送

    Attributes:
        container (Container): 依赖注入容器
        lock (asyncio.Lock): 异步锁，用于确保数据一致性
        redis: Redis异步客户端实例
        async_sessionmaker: 异步数据库会话工厂
        cache: Cashews Redis缓存实例
    """

    _cache: ClassVar[Cache | None] = None

    def __init__(self, container: Container):
        """初始化数据存储层。

        Args:
            container: 依赖注入容器，提供Redis客户端和数据库会话工厂。
        """
        self.container = container
        self.redis = container.redis_client
        self.async_sessionmaker = container.async_sessionmaker

        if DataStore._cache is None:
            DataStore._cache = Cache()
            if self.container.config.cache_backend == "memory":
                DataStore._cache.setup(f"mem://?size={self.container.config.cache_max_size}")
            else:
                DataStore._cache.setup(
                    self.container.config.redis_url,
                    middlewares=(add_prefix("processed:"),),
                    client_side=True,
                )
        self.cache = DataStore._cache
        self.consumer_transport = self.container.config.consumer_transport
        if self.consumer_transport == "redis":
            self.publisher = RedisStreamsPublisher(
                self.redis,  # type: ignore
                consumer_config=self.container.config.consumer_config,
            )
        elif self.consumer_transport == "websocket":
            self.publisher = WebSocketPublisher(
                self.container.config.websocket_url,
                server=self.container.ws_server,
                consumer_config=self.container.config.consumer_config,
            )
        else:
            self.publisher = NoopPublisher()

    async def close(self) -> None:
        """优雅关闭：释放发布器等资源（幂等）。"""
        try:
            if self.publisher is not None:
                await self.publisher.close()
        except Exception as e:
            logger.warning("Failed to close publisher: {}", e)

    @asynccontextmanager
    async def get_session(self):
        """用于获取数据库会话的异步上下文管理器。

        提供自动的事务管理，包括异常处理时的回滚操作。

        Yields:
            AsyncSession: 异步数据库会话对象。

        Raises:
            Exception: 如果在会话中发生错误，将回滚事务并重新抛出异常。
        """
        async with self.async_sessionmaker() as session:
            try:
                yield session
            except Exception as e:
                logger.exception("Error in database session: {}", e)
                await session.rollback()
                raise

    async def filter_new_ids(self, item_type: ItemType | Literal["user"], ids: list[int]) -> set[int]:
        """使用客户端缓存过滤新ID。

        过滤策略如下：
        1. 内存缓存 / Redis缓存检查（自动使用客户端本地缓存）
        2. 数据库查重过滤（最终的真实性检查）

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'、'comment'或'user'。
            ids: 需要检查的ID列表。

        Returns:
            set[int]: 不存在于数据库中的新ID集合。
        """
        if not ids:
            return set()

        # cashews 的 redis client-side caching 后端在 get_many 入参包含重复 key 时
        # 可能抛出 KeyError（内部对 missed_keys 做 set.remove）。
        # 这里通过对 ids 先进行保序去重来规避该问题。
        unique_ids = list(dict.fromkeys(ids))

        # 第一层：内存缓存 / Redis缓存(With Client-Side Caching)过滤
        ids_after_cache = await self._filter_by_cache(item_type, unique_ids)
        if not ids_after_cache:
            return set()

        # 第二层：数据库查重过滤（在事务中执行，确保一致性）
        try:
            ids_after_db = await self._filter_by_database(item_type, list(ids_after_cache))
        except Exception as e:
            # 如果数据库查询失败，不标记缓存，避免缓存和数据库状态不一致
            logger.exception("Database filter failed for {}: {}", item_type, e)
            raise

        if ids_after_db is not None:
            await self._mark_as_processed(item_type, ids_after_db)

        return ids_after_db

    async def _filter_by_cache(self, item_type: ItemType | Literal["user"], ids: list[int]) -> set[int]:
        """通过内存缓存 / Redis缓存（With Client-Side Caching）过滤。

        Redis Client-Side Caching会自动：
        1. 首先检查本地缓存
        2. 本地缓存未命中时才查询Redis服务器
        3. 自动维护缓存一致性

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'、'comment'或'user'。
            ids: 需要检查的ID列表。

        Returns:
            set[int]: 未缓存的ID集合。
        """
        cache_keys = [f"{item_type}:{item_id}" for item_id in ids]
        cached_results = await self.cache.get_many(*cache_keys)

        new_ids = {ids[i] for i, val in enumerate(cached_results) if val is None}

        hits = len(ids) - len(new_ids)
        if hits > 0:
            CACHE_HITS.labels(type=item_type).inc(hits)

        logger.debug("Cache filter: {} -> {} for {}", len(ids), len(new_ids), item_type)
        return new_ids

    async def _filter_by_database(self, item_type: ItemType | Literal["user"], ids: list[int]) -> set[int]:
        """数据库查重过滤

        根据item_type从数据库中查询已存在的ID，并返回未存在的ID集合。

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'、'comment'或'user'。
            ids: 需要检查的ID列表。

        Returns:
            set[int]: 不存在于数据库中的新ID集合。
        """
        if not ids:
            return set()

        async with self.get_session() as session:
            model_map = {
                "thread": Thread.tid,
                "post": Post.pid,
                "comment": Comment.cid,
                "user": User.user_id,
            }
            id_column = model_map.get(item_type)
            if not id_column:
                raise ValueError(f"Unknown item_type: {item_type}")

            statement = select(id_column).where(id_column.in_(ids))
            result = await session.execute(statement)
            existing_ids = {row[0] for row in result.fetchall()}

        if existing_ids:
            CACHE_MISSES.labels(type=item_type).inc(len(existing_ids))
            logger.debug("Database filter: Found {} existing IDs for {}", len(existing_ids), item_type)
            # 标记缓存由 filter_new_ids 统一处理

        new_ids = set(ids) - existing_ids
        logger.debug(
            "Database filter: {} -> {} for {} (found {} existing)",
            len(ids),
            len(new_ids),
            item_type,
            len(existing_ids),
        )
        return new_ids

    async def _mark_as_processed(self, item_type: ItemType | Literal["user"], new_ids: set[int]):
        """标记ID为已处理。"""
        if not new_ids:
            return

        updates = {f"{item_type}:{item_id}": True for item_id in new_ids}
        await self.cache.set_many(updates, expire=self.container.config.cache_ttl_seconds)

        logger.debug("Marked {} new {} IDs as processed", len(new_ids), item_type)

    async def save_items[T: (Comment, Forum, Post, Thread, User)](self, items: list[T], upsert: bool = False):
        """将SQLAlchemy对象批量保存到数据库。

        使用PostgreSQL的 "INSERT ... ON CONFLICT DO NOTHING"
        防止极端情况下的主键冲突，
        或使用 "INSERT ... ON CONFLICT DO UPDATE" 实现UPSERT功能。

        Args:
            items: 需要保存的SQLAlchemy模型实例列表。支持Comment、Forum、
                   Post、Thread、User等类型。
            upsert: 是否启用UPSERT功能。如果为True，则在主键冲突时更新现有记录。
                    默认为False，表示仅插入新记录，冲突时忽略。
        """
        if not items:
            return

        async with self.get_session() as session:
            table_name = "unknown"
            op = "upsert" if upsert else "insert"
            try:
                items_as_dicts = [item.to_dict() for item in items]
                model_class = type(items[0])
                table_name = model_class.__tablename__

                statement = insert(model_class).values(items_as_dicts)

                if upsert:
                    primary_key = PRIMARY_KEY_MAP.get(model_class)
                    update_dict = {
                        col.name: statement.excluded[col.name]
                        for col in model_class.__table__.columns
                        if col.name not in primary_key
                    }
                    statement = statement.on_conflict_do_update(index_elements=list(primary_key), set_=update_dict)  # type: ignore
                else:
                    statement = statement.on_conflict_do_nothing()

                await session.execute(statement)
                await session.commit()
                DB_OPERATIONS.labels(operation=op, table=table_name, status="success").inc(len(items))
                logger.debug("Successfully saved/ignored {} items using ON CONFLICT.", len(items))

            except IntegrityError as e:
                DB_OPERATIONS.labels(operation=op, table=table_name, status="integrity_error").inc()
                logger.error("An unexpected integrity error occurred: {}", e)
                raise
            except Exception as e:
                DB_OPERATIONS.labels(operation=op, table=table_name, status="error").inc()
                logger.error("Failed to save items: {}", e)
                raise

    async def get_threads_by_tids(self, tids: list[int] | set[int]) -> list[Thread]:
        """根据一组tid从数据库获取主题贴信息。

        Args:
            tids: 主题贴tid列表

        Returns:
            list[Thread]: 主题贴对象列表，如果不存在则返回空列表。
        """
        async with self.get_session() as session:
            statement = select(Thread).where(Thread.tid.in_(tids))
            result = await session.execute(statement)
            return list(result.scalars().all())

    async def get_posts_by_pids(self, pids: list[int] | set[int]) -> list[Post]:
        """根据一组pid从数据库获取回复信息。

        Args:
            pids: 回复pid列表

        Returns:
            list[Post]: 回复对象列表，如果不存在则返回空列表。
        """
        async with self.get_session() as session:
            statement = select(Post).where(Post.pid.in_(pids))
            result = await session.execute(statement)
            return list(result.scalars().all())

    async def push_object_event(
        self,
        item_type: ItemType,
        obj: DTOType,
        *,
        backfill: bool = False,
        event_type: str = "upsert",
    ) -> None:
        """将完整对象以事件形式推送到对象消费者（Redis Streams 或 WebSocket）。"""
        envelope: EventEnvelope = build_envelope(item_type, obj, event_type=event_type, backfill=backfill)
        await self.publisher.publish_object(envelope)

    async def update_thread_metadata(self, tid: int, last_time: datetime, reply_num: int) -> None:
        """更新主题贴的元数据（最后回复时间和回复数）。"""
        from sqlalchemy import update

        async with self.get_session() as session:
            stmt = update(Thread).where(Thread.tid == tid).values(last_time=last_time, reply_num=reply_num)
            await session.execute(stmt)
            await session.commit()

    async def save_threads_and_pending_scans(
        self,
        thread_models: list[Thread],
        pending_scans: list[PendingThreadScan],
    ) -> None:
        """原子保存 thread 元数据与 pending_scan 标记。"""
        if not thread_models and not pending_scans:
            return

        async with self.get_session() as session:
            if thread_models:
                thread_values = [item.to_dict() for item in thread_models]
                thread_stmt = insert(Thread).values(thread_values).on_conflict_do_nothing()
                await session.execute(thread_stmt)

            if pending_scans:
                pending_values = [item.to_dict() for item in pending_scans]
                pending_stmt = insert(PendingThreadScan).values(pending_values).on_conflict_do_nothing()
                await session.execute(pending_stmt)

            await session.commit()

    async def remove_pending_thread_scan(self, tid: int) -> None:
        """移除一条待扫描的主题贴记录。"""
        async with self.get_session() as session:
            stmt = delete(PendingThreadScan).where(PendingThreadScan.tid == tid)
            await session.execute(stmt)
            await session.commit()

    async def get_all_pending_thread_scans(self) -> list[PendingThreadScan]:
        """获取所有待扫描的主题贴记录（用于重启恢复）。"""
        async with self.get_session() as session:
            result = await session.execute(select(PendingThreadScan))
            return list(result.scalars().all())

    async def add_pending_comment_scan(
        self,
        tid: int,
        pid: int,
        *,
        backfill: bool,
        task_kind: PendingCommentTaskKind,
    ) -> None:
        """添加一条楼中楼待扫描记录。"""
        async with self.get_session() as session:
            pending = PendingCommentScan(tid=tid, pid=pid, backfill=backfill, task_kind=task_kind)
            stmt = insert(PendingCommentScan).values(pending.to_dict()).on_conflict_do_nothing()
            await session.execute(stmt)
            await session.commit()

    async def remove_pending_comment_scan(self, tid: int, pid: int) -> None:
        """移除一条楼中楼待扫描记录。"""
        async with self.get_session() as session:
            stmt = delete(PendingCommentScan).where((PendingCommentScan.tid == tid) & (PendingCommentScan.pid == pid))
            await session.execute(stmt)
            await session.commit()

    async def get_all_pending_comment_scans(self) -> list[PendingCommentScan]:
        """获取所有楼中楼待扫描记录（用于重启恢复）。"""
        async with self.get_session() as session:
            result = await session.execute(select(PendingCommentScan))
            return list(result.scalars().all())
