"""依赖注入容器模块。

该模块实现了应用程序的依赖注入容器，负责统一管理和初始化
各种外部资源和服务，包括数据库连接、Redis客户端、aiotieba客户端等。
"""

from __future__ import annotations

from asyncio import Semaphore
from typing import TYPE_CHECKING

import redis.asyncio as redis
from aiolimiter import AsyncLimiter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from tiebameow.client import Client
from tiebameow.models.orm import Forum
from tiebameow.utils.logger import logger

from .ws_server import WebSocketServer

if TYPE_CHECKING:
    from .config import Config


class Container:
    """依赖注入容器。

    负责管理应用程序的所有外部依赖，包括数据库连接、Redis客户端、
    aiotieba客户端、aiolimiter限流器等。提供统一的资源初始化和清理接口。

    Attributes:
        config (Config): 应用程序配置对象
        limiter (AsyncLimiter): 漏桶算法实现的异步限流器
        db_engine (AsyncEngine): SQLAlchemy异步数据库引擎
        redis_client (redis.Redis): Redis异步客户端
        tb_client (Client): tiebameow异步贴吧客户端
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
        self.db_engine: AsyncEngine | None = None
        self.redis_client: redis.Redis | None = None
        self.tb_client: Client | None = None
        self.forums: list[Forum] | None = None
        self.ws_server: WebSocketServer | None = None

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
            self.limiter = AsyncLimiter(1, time_period=1 / self.config.rps_limit)
            self.semaphore = Semaphore(self.config.concurrency_limit)
            logger.info("AioLimiter initialized with a rate of {} RPS.", self.config.rps_limit)
            self.tb_client = await Client(
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
            logger.exception(f"Failed to initialize container resources: {e}")
            await self.teardown()
            raise

    async def teardown(self):
        """异步关闭并清理所有资源。

        按相反顺序安全关闭所有已初始化的资源：
        1. Redis客户端连接
        2. PostgreSQL数据库引擎
        3. aiotieba客户端

        该方法是幂等的，可以安全地多次调用。
        """
        logger.info("Tearing down container resources...")

        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis client closed.")
        if self.db_engine:
            await self.db_engine.dispose()
            logger.info("PostgreSQL AsyncEngine disposed.")

        if self.tb_client:
            await self.tb_client.__aexit__()
            logger.info("Tieba Client closed.")
        if self.ws_server:
            try:
                await self.ws_server.stop()
            except Exception:
                pass
            self.ws_server = None

        logger.info("Container resources torn down successfully.")
