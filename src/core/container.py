"""依赖注入容器模块。

该模块实现了应用程序的依赖注入容器，负责统一管理和初始化
各种外部资源和服务，包括数据库连接、Redis客户端、aiotieba客户端等。
"""

from __future__ import annotations

import logging
from asyncio import Semaphore
from typing import TYPE_CHECKING

import redis.asyncio as redis
from aiolimiter import AsyncLimiter
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from .client import Client

if TYPE_CHECKING:
    from ..config import Config
    from ..models import Forum


log = logging.getLogger("container")


class Container:
    """依赖注入容器。

    负责管理应用程序的所有外部依赖，包括数据库连接、Redis客户端、
    aiotieba客户端、aiolimiter限流器等。提供统一的资源初始化和清理接口。

    Attributes:
        config (Config): 应用程序配置对象
        limiter (AsyncLimiter): 漏桶算法实现的异步限流器
        db_engine (AsyncEngine): SQLAlchemy异步数据库引擎
        redis_client (redis.Redis): Redis异步客户端
        tb_client (aiotieba.Client): aiotieba异步客户端
        fids (list[int]): fid列表
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
        log.info("Initializing container resources...")
        try:
            self.limiter = AsyncLimiter(self.config.rps_limit, time_period=1.0)
            self.semaphore = Semaphore(self.config.concurrency_limit)
            log.info(f"AioLimiter initialized with a rate of {self.config.rps_limit} RPS.")

            self.tb_client = await Client(
                self.config.BDUSS, limiter=self.limiter, semaphore=self.semaphore
            ).__aenter__()
            log.info("Tieba Client started.")

            self.db_engine = create_async_engine(self.config.database_url, echo=False)
            self.async_sessionmaker = async_sessionmaker(
                bind=self.db_engine, class_=AsyncSession, expire_on_commit=False
            )
            log.info("PostgreSQL AsyncEngine created.")

            self.redis_client = await redis.from_url(self.config.redis_url, decode_responses=True)
            await self.redis_client.ping()
            log.info("Redis client connected successfully.")

            log.info("Container resources initialized successfully.")

        except Exception as e:
            log.exception(f"Failed to initialize container resources: {e}")
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
        log.info("Tearing down container resources...")

        if self.redis_client:
            await self.redis_client.close()
            log.info("Redis client closed.")

        if self.db_engine:
            await self.db_engine.dispose()
            log.info("PostgreSQL AsyncEngine disposed.")

        if self.tb_client:
            await self.tb_client.__aexit__()
            log.info("Tieba Client closed.")

        log.info("Container resources torn down successfully.")
