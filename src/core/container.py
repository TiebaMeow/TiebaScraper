"""依赖注入容器模块。

该模块实现了应用程序的依赖注入容器，负责统一管理和初始化
各种外部资源和服务，包括数据库连接、Redis客户端、aiotieba客户端等。
"""

from __future__ import annotations

import itertools
from asyncio import Semaphore
from typing import TYPE_CHECKING

import redis.asyncio as redis
from aiolimiter import AsyncLimiter
from aiotieba.config import ProxyConfig as AiotiebaProxyConfig
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from tiebameow.client import Client
from tiebameow.models.orm import Forum
from tiebameow.utils.logger import logger

from .ws_server import WebSocketServer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .config import Config


class Container:
    """依赖注入容器。

    负责管理应用程序的所有外部依赖，包括数据库连接、Redis客户端、
    aiotieba客户端、aiolimiter限流器等。提供统一的资源初始化和清理接口。

    Attributes:
        config (Config): 应用程序配置对象
        limiter (AsyncLimiter): 漏桶算法实现的异步限流器（全局，向后兼容）
        threads_limiter (AsyncLimiter): get_threads 接口专用限流器
        posts_limiter (AsyncLimiter): get_posts 接口专用限流器
        comments_limiter (AsyncLimiter): get_comments 接口专用限流器
        db_engine (AsyncEngine): SQLAlchemy异步数据库引擎
        redis_client (redis.Redis): Redis异步客户端
        tb_client (Client): tiebameow异步贴吧客户端（主客户端，向后兼容）
        tb_clients (list[Client]): 多代理模式下的所有客户端列表
        forums (list[Forum]): 贴吧信息缓存
        async_sessionmaker: 异步数据库会话工厂
    """

    def __init__(self, config: Config):
        """初始化容器。

        Args:
            config: 应用程序的配置对象，包含数据库、Redis、贴吧等配置信息。
        """
        self.config = config

        self.limiter: AsyncLimiter | None = None
        self.semaphore: Semaphore | None = None
        # 分接口限流器（多代理模式下，每个代理有独立的限流器组）
        self.threads_limiter: AsyncLimiter | None = None
        self.posts_limiter: AsyncLimiter | None = None
        self.comments_limiter: AsyncLimiter | None = None
        # 多代理支持：每个代理有独立的限流器组
        self._threads_limiters: list[AsyncLimiter] = []
        self._posts_limiters: list[AsyncLimiter] = []
        self._comments_limiters: list[AsyncLimiter] = []
        self._client_cycle: Iterator[int] | None = None
        self.db_engine: AsyncEngine | None = None
        self.redis_client: redis.Redis | None = None
        self.tb_client: Client | None = None
        self.tb_clients: list[Client] = []
        self.forums: list[Forum] | None = None
        self.ws_server: WebSocketServer | None = None

    def get_next_client_index(self) -> int:
        """获取下一个客户端索引（轮询）。

        Returns:
            int: 下一个要使用的客户端索引。
        """
        if self._client_cycle is None:
            return 0
        return next(self._client_cycle)

    def get_client(self, index: int | None = None) -> Client:
        """获取指定索引或轮询获取的客户端。

        Args:
            index: 客户端索引，None 则轮询获取。

        Returns:
            Client: tiebameow 客户端实例。
        """
        if not self.tb_clients:
            assert self.tb_client is not None
            return self.tb_client

        idx = index if index is not None else self.get_next_client_index()
        return self.tb_clients[idx % len(self.tb_clients)]

    def get_limiters(self, index: int | None = None) -> tuple[AsyncLimiter, AsyncLimiter, AsyncLimiter]:
        """获取指定索引或轮询获取的限流器组。

        Args:
            index: 限流器组索引，None 则使用全局限流器。

        Returns:
            tuple: (threads_limiter, posts_limiter, comments_limiter)
        """
        if index is not None and self._threads_limiters:
            idx = index % len(self._threads_limiters)
            return (
                self._threads_limiters[idx],
                self._posts_limiters[idx],
                self._comments_limiters[idx],
            )
        # 使用全局限流器
        assert self.threads_limiter is not None
        assert self.posts_limiter is not None
        assert self.comments_limiter is not None
        return (self.threads_limiter, self.posts_limiter, self.comments_limiter)

    async def setup(self):
        """异步初始化容器资源。

        依次初始化以下资源：
        1. AsyncLimiter - 用于aiotieba请求限流
        2. Semaphore - 用于控制并发请求数
        3. Client - 带速率与并发限制的异步贴吧客户端
        4. PostgreSQL异步数据库引擎和会话工厂
        5. Redis异步客户端连接

        如果任何步骤失败，会自动调用teardown()清理已初始化的资源。

        Raises:
            Exception: 当资源初始化失败时抛出异常。
        """
        logger.info("Initializing container resources...")
        try:
            # 全局限流器（向后兼容，用于 tiebameow.Client 内部限流）
            self.limiter = AsyncLimiter(1, time_period=1 / self.config.rps_limit)
            self.semaphore = Semaphore(self.config.concurrency_limit)
            logger.info("Global limiter initialized with a rate of {} RPS.", self.config.rps_limit)

            # 分接口限流器（用于 worker 层精细控制）
            self.threads_limiter = AsyncLimiter(1, time_period=1 / self.config.threads_rps)
            self.posts_limiter = AsyncLimiter(1, time_period=1 / self.config.posts_rps)
            self.comments_limiter = AsyncLimiter(1, time_period=1 / self.config.comments_rps)
            logger.info(
                "Per-API limiters initialized: threads={} RPS, posts={} RPS, comments={} RPS.",
                self.config.threads_rps,
                self.config.posts_rps,
                self.config.comments_rps,
            )

            # 多代理模式：创建多个客户端和限流器
            proxy_urls = self.config.proxy_urls
            if len(proxy_urls) > 1:
                logger.info("Multi-proxy mode enabled with {} proxies.", len(proxy_urls))
                for i, url in enumerate(proxy_urls):
                    # 每个代理有独立的限流器组
                    self._threads_limiters.append(AsyncLimiter(1, time_period=1 / self.config.threads_rps))
                    self._posts_limiters.append(AsyncLimiter(1, time_period=1 / self.config.posts_rps))
                    self._comments_limiters.append(AsyncLimiter(1, time_period=1 / self.config.comments_rps))

                    proxy_config = AiotiebaProxyConfig(url=url)
                    client = await Client(
                        proxy=proxy_config,
                        limiter=self.limiter,
                        semaphore=self.semaphore,
                        cooldown_429=self.config.cooldown_seconds_429,
                        retry_attempts=5,
                    ).__aenter__()
                    self.tb_clients.append(client)
                    logger.info("Tieba Client #{} started with proxy: {}", i, url.split("@")[-1])

                # 设置主客户端为第一个（向后兼容）
                self.tb_client = self.tb_clients[0]
                # 设置轮询迭代器
                self._client_cycle = itertools.cycle(range(len(self.tb_clients)))
                logger.info(
                    "Multi-proxy clients initialized. Total throughput: threads={} RPS, posts={} RPS, comments={} RPS.",
                    self.config.threads_rps * len(self.tb_clients),
                    self.config.posts_rps * len(self.tb_clients),
                    self.config.comments_rps * len(self.tb_clients),
                )
            else:
                # 单代理或无代理模式
                proxy_url = self.config.proxy_url
                proxy_config: AiotiebaProxyConfig | bool = False
                if proxy_url:
                    proxy_config = AiotiebaProxyConfig(url=proxy_url)
                    logger.info("Proxy enabled: {}", proxy_url.split("@")[-1])

                self.tb_client = await Client(
                    proxy=proxy_config,
                    limiter=self.limiter,
                    semaphore=self.semaphore,
                    cooldown_429=self.config.cooldown_seconds_429,
                    retry_attempts=5,
                ).__aenter__()
                logger.info("Tieba Client started.")

            self.db_engine = create_async_engine(self.config.database_url, echo=False)
            self.async_sessionmaker = async_sessionmaker(
                bind=self.db_engine, class_=AsyncSession, expire_on_commit=False
            )
            logger.info("PostgreSQL AsyncEngine created.")

            if self.config.consumer_transport == "redis":
                self.redis_client = await redis.from_url(self.config.redis_url, decode_responses=True)
                await self.redis_client.ping()  # type: ignore
                logger.info("Redis client connected successfully.")
            elif self.config.consumer_transport == "websocket" and self.config.websocket_enabled:
                # 启动内置 WS 服务
                self.ws_server = WebSocketServer(self.config.websocket_url, token=self.config.websocket_token)

                async def _add_forum(fname: str) -> bool:
                    assert self.tb_client is not None
                    assert self.async_sessionmaker is not None
                    fid = await self.tb_client.get_fid(fname)
                    if not fid:
                        return False

                    async with self.async_sessionmaker() as session:
                        try:
                            # 是否已存在
                            res = await session.execute(select(Forum).where(Forum.fname == fname))
                            exist = res.scalar_one_or_none()
                            if exist is None:
                                session.add(Forum(fid=fid, fname=fname))
                                await session.commit()
                                logger.info("Added forum via WS: [{}](fid={})", fname, fid)
                            # 更新容器缓存
                            current = self.forums or []
                            if all(f.fname != fname for f in current):
                                current.append(Forum(fid=fid, fname=fname))
                                self.forums = current
                            return True
                        except Exception:
                            await session.rollback()
                            raise

                async def _remove_forum(fname: str) -> bool:
                    current = self.forums or []
                    self.forums = [f for f in current if f.fname != fname]
                    return True

                if self.config.mode == "periodic":
                    self.ws_server.on_add_forum(_add_forum)
                    self.ws_server.on_remove_forum(_remove_forum)
                else:
                    logger.warning("WebSocket dynamic forum management is only supported in 'periodic' mode.")
                await self.ws_server.start()
                logger.info("WebSocket server started: %s", self.ws_server.get_ws_url())

            elif self.config.consumer_transport == "websocket":
                logger.warning("WebSocket consumer transport selected but WebSocket server is disabled in config.")
                logger.warning("Nothing will be sent to consumers.")

            logger.info("Container resources initialized successfully.")

        except Exception as e:
            logger.exception("Failed to initialize container resources: {}", e)
            await self.teardown()
            raise

    async def teardown(self):
        """异步关闭并清理所有资源。

        按相反顺序安全关闭所有已初始化的资源：
        1. Redis客户端连接
        2. PostgreSQL数据库引擎
        3. tiebameow.client.Client客户端

        该方法是幂等的，可以安全地多次调用。
        """
        logger.info("Tearing down container resources...")

        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis client closed.")
        if self.db_engine:
            await self.db_engine.dispose()
            logger.info("PostgreSQL AsyncEngine disposed.")

        # 关闭所有 tieba 客户端
        if self.tb_clients:
            for i, client in enumerate(self.tb_clients):
                await client.__aexit__()
                logger.info("Tieba Client #{} closed.", i)
        elif self.tb_client:
            await self.tb_client.__aexit__()
            logger.info("Tieba Client closed.")
        if self.ws_server:
            try:
                await self.ws_server.stop()
            except Exception:
                pass
            self.ws_server = None

        logger.info("Container resources torn down successfully.")
