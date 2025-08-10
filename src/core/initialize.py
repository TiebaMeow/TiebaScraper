"""项目初始化模块。

该模块包含应用程序启动时需要执行的初始化任务，
例如初始化数据库中的监控贴吧列表等。
"""

import asyncio
import logging
from typing import Literal

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..config import Config
from ..models import Base, Forum
from .container import Container

log = logging.getLogger(__name__)


async def initialize_application(
    mode: Literal["periodic", "backfill", "hybrid"],
) -> tuple[Container, asyncio.PriorityQueue]:
    """初始化整个应用程序。

    该函数封装了应用启动所需的所有核心初始化步骤：
    1. 根据运行模式加载配置。
    2. 创建并设置依赖注入容器。
    3. 创建数据库表并注册分区。
    4. 初始化数据库中的贴吧列表。
    5. 创建主任务队列。

    Args:
    mode: 应用程序的运行模式 ("periodic"、"backfill" 或 "hybrid")。

    Returns:
        一个元组，包含初始化完成的容器实例和任务队列。
    """
    log.info(f"Initializing application in '{mode}' mode...")

    app_config = Config(mode=mode)

    container = Container(config=app_config)
    await container.setup()

    await create_tables(container)

    await initialize_forums(container)

    task_queue = asyncio.PriorityQueue(maxsize=10000)

    log.info("Application initialized successfully.")
    return container, task_queue


async def _partman_create_parent(
    conn: AsyncConnection, parent_table: str, interval: str = "1 month", premake: int = 4
) -> None:
    res = await conn.execute(
        text("SELECT 1 FROM partman.part_config WHERE parent_table = :pt LIMIT 1"),
        {"pt": parent_table},
    )
    already_managed = res.first() is not None
    if already_managed:
        return
    try:
        result = await conn.execute(
            text(
                """
                SELECT partman.create_parent(
                    p_parent_table := :p_parent_table,
                    p_control      := 'create_time',
                    p_interval     := :p_interval,
                    p_premake      := :p_premake
                );
                """
            ),
            {"p_parent_table": parent_table, "p_interval": interval, "p_premake": premake},
        )
        if result.rowcount == 0:
            raise RuntimeError(f"Failed to register {parent_table} to pg_partman.")

    except Exception as e:
        log.error(f"Error creating partition for {parent_table}: {e}")
        raise RuntimeError(f"Failed to register {parent_table} to pg_partman.") from e

    log.info(f"Registered {parent_table} to pg_partman (interval={interval}, premake={premake}).")


async def create_tables(container: Container) -> None:
    """创建数据库表。

    使用SQLAlchemy的Base.metadata.create_all方法在数据库中创建所有定义的模型表。

    Args:
        container: 依赖注入容器实例，提供数据库会话工厂。
    """
    log.info("Initializing database tables...")

    if not container.tb_client or not container.async_sessionmaker:
        raise RuntimeError("Container is not set up properly.")

    try:
        async with container.db_engine.begin() as conn:  # type: ignore
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("CREATE SCHEMA IF NOT EXISTS partman;"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA partman;"))
            for table in ("thread", "post", "comment"):
                await _partman_create_parent(
                    conn,
                    f"public.{table}",
                    interval=container.config.p_interval,
                    premake=container.config.p_premake,
                )

    except Exception as e:
        log.error(f"Failed to create database tables: {e}")
        raise

    log.info("Database tables created successfully.")


async def initialize_forums(container: Container) -> None:
    """初始化或同步数据库中的贴吧列表。

    从配置文件中读取要监控的贴吧名称，使用aiotieba客户端获取它们的fid，
    然后将这些信息存入数据库。同时，将获取到的fid列表更新到容器中。

    Args:
        container: 依赖注入容器实例。
    """
    log.info("Initializing forum list...")
    if not container.tb_client or not container.async_sessionmaker:
        raise RuntimeError("Container is not set up properly.")

    forum_list: list[Forum] = []
    try:
        async with container.async_sessionmaker() as session:
            try:
                existing_forums = await session.execute(select(Forum))
                existing_forums = existing_forums.scalars().all()

                for forum_name in container.config.forums:
                    if forum := next((f for f in existing_forums if f.fname == forum_name), None):
                        forum_list.append(forum)
                        continue

                    fid = await container.tb_client.get_fid(forum_name)
                    if not fid:
                        log.warning(f"Forum [{forum_name}吧] not found, skipping.")
                        continue
                    new_forum = Forum(fid=fid, fname=forum_name)
                    session.add(new_forum)
                    forum_list.append(new_forum)
                    log.info(f"Added new forum: {forum_name} (fid={fid})")

                await session.commit()

            except Exception:
                await session.rollback()
                raise

    except Exception as e:
        log.error(f"Failed to initialize forums: {e}")
        raise

    container.forums = forum_list
    log.info(f"Forums initialized: {[f.fname for f in forum_list]}")
