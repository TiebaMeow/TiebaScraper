"""数据存储层模块。

该模块提供了统一的数据访问接口，封装了与PostgreSQL数据库和Redis缓存的交互逻辑。
主要功能包括数据去重、批量保存、状态追踪和消息队列管理。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from time import perf_counter
from typing import TYPE_CHECKING, ClassVar, Literal

import aiotieba.typing as aiotieba
from cashews import Cache, add_prefix
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from ..models import Comment, Forum, Post, Thread, User
from .publisher import EventEnvelope, NoopPublisher, RedisStreamsPublisher, WebSocketPublisher, build_envelope

if TYPE_CHECKING:
    from .container import Container

ItemType = Literal["thread", "post", "comment"]
AiotiebaType = aiotieba.Thread | aiotieba.Post | aiotieba.Comment

log = logging.getLogger("datastore")

CONSUMER_QUEUE_KEY = "consumer:queue"
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

        # 发布通道与模式
        self.consumer_transport = self.container.config.consumer_transport
        self.consumer_mode = self.container.config.consumer_mode

        if self.consumer_transport == "redis":
            self.publisher = RedisStreamsPublisher(
                self.redis,  # type: ignore
                consumer_config=self.container.config.consumer_config,
            )
        elif self.consumer_transport == "websocket" and self.container.config.websocket_enabled:
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
            log.warning(f"Failed to close publisher: {e}")

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
                log.exception(f"Error in database session: {e}")
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

        # 第一层：内存缓存 / Redis缓存(With Client-Side Caching)过滤
        ids_after_cache = await self._filter_by_cache(item_type, ids)
        if not ids_after_cache:
            return set()

        # 第二层：数据库查重过滤
        ids_after_db = await self._filter_by_database(item_type, list(ids_after_cache))

        # 将新ID标记为已处理
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

        new_ids = {ids[i] for i, key in enumerate(cache_keys) if key not in cached_results}

        log.debug(f"Cache filter: {len(ids)} -> {len(new_ids)} for {item_type}")
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
            log.debug(f"Database filter: Found {len(existing_ids)} existing IDs for {item_type}")
            await self._mark_as_processed(item_type, existing_ids)

        new_ids = set(ids) - existing_ids
        log.debug(f"Database filter: {len(ids)} -> {len(new_ids)} for {item_type} (found {len(existing_ids)} existing)")
        return new_ids

    async def _mark_as_processed(self, item_type: ItemType | Literal["user"], new_ids: set[int]):
        """标记ID为已处理。"""
        if not new_ids:
            return

        updates = {f"{item_type}:{item_id}": True for item_id in new_ids}
        await self.cache.set_many(updates, expire=self.container.config.cache_ttl_seconds)

        log.debug(f"Marked {len(new_ids)} new {item_type} IDs as processed")

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
            try:
                items_as_dicts = [item.to_dict() for item in items]
                model_class = type(items[0])

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
                log.debug(f"Successfully saved/ignored {len(items)} items using ON CONFLICT.")

            except IntegrityError as e:
                log.error(f"An unexpected integrity error occurred: {e}")
                raise
            except Exception as e:
                log.error(f"Failed to save items: {e}")
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

    async def push_to_id_queue(self, item_type: ItemType, item_id: int):
        """将新处理的数据信息推送到ID队列。

        将处理完成的数据项信息以JSON格式推送到Redis列表，
        供下游消费者服务处理。

        Args:
            item_type: 数据项类型，可选值为'thread'、'post'或'comment'。
            item_id: 数据项的ID。
        """
        if self.consumer_mode != "id":
            return

        await self.publisher.publish_id(item_type, item_id)

    async def push_object_event(
        self,
        item_type: ItemType,
        obj: AiotiebaType,
        *,
        backfill: bool = False,
        event_type: str = "upsert",
    ) -> None:
        """将完整对象以事件形式推送到对象消费者（Redis Streams）。

        在 config.mode != 'object' 时为 no-op。
        """
        if self.consumer_mode != "object":
            return

        envelope: EventEnvelope = build_envelope(item_type, obj, event_type=event_type, backfill=backfill)
        await self.publisher.publish_object(envelope)

    async def run_partition_maintenance(self) -> None:
        """执行 pg_partman 的默认分区数据回填与维护过程。

        为避免 partition_data_proc 的临时表字段冲突，每个表的回填使用独立连接。
        """
        if not self.container.config.partition_enabled:
            log.debug("Partition disabled; skip run_partition_maintenance().")
            return
        start = perf_counter()

        try:
            tables = ["thread", "post", "comment"]

            for table in tables:
                async with self.container.db_engine.connect() as conn:  # type: ignore
                    ac_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
                    await ac_conn.execute(text(f"CALL partman.partition_data_proc('public.{table}');"))
                    await ac_conn.execute(text(f"VACUUM ANALYZE {table};"))

            async with self.container.db_engine.connect() as conn:  # type: ignore
                ac_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
                await ac_conn.execute(text("CALL partman.run_maintenance_proc();"))

        except Exception as e:
            log.error(f"Partition maintenance failed: {e}")
            raise

        elapsed = perf_counter() - start
        log.info(f"Partition maintenance finished in {elapsed:.2f}s")
