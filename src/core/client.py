from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from functools import wraps
from typing import TYPE_CHECKING

import aiotieba as tb
from aiotieba.exception import HTTPStatusError, TiebaServerError
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    wait_fixed,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from aiolimiter import AsyncLimiter
    from tenacity import RetryCallState

logger = logging.getLogger(__name__)


def _wait_after_error(retry_state: RetryCallState) -> float:
    """
    tenacity的等待回调函数。

    将429错误交由全局冷却逻辑处理，其他错误使用指数退避等待策略。
    """
    outcome = retry_state.outcome
    if outcome is None:
        return wait_exponential_jitter(initial=0.5, max=5.0)(retry_state)

    exc = outcome.exception()
    if isinstance(exc, HTTPStatusError) and exc.code == 429:
        return wait_fixed(0)(retry_state)

    return wait_exponential_jitter(initial=0.5, max=5.0)(retry_state)


def with_limit_and_retry(func):
    """为客户端方法应用限流与重试，并包含全局冷却逻辑。"""

    @wraps(func)
    async def wrapper(self: Client, *args, **kwargs):
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=_wait_after_error,
            retry=retry_if_exception_type((
                asyncio.TimeoutError,
                ConnectionError,
                OSError,
                HTTPStatusError,
                TiebaServerError,
            )),
            reraise=True,
        ):
            with attempt:
                try:
                    async with self.rate_limiter():
                        ret = await func(self, *args, **kwargs)
                        err = getattr(ret, "err", None)
                        if err:
                            raise err
                        return ret
                except HTTPStatusError as e:
                    if e.code == 429:
                        wait_seconds = self._cooldown_seconds_429
                        logger.warning(
                            "Received HTTP 429. Activating global cooldown for %.1f seconds.",
                            wait_seconds,
                        )
                        await self.set_cooldown(wait_seconds)
                        await asyncio.sleep(wait_seconds)

                    raise

    return wrapper


class Client(tb.Client):
    """扩展的aiotieba客户端，添加了自定义的请求限流和并发控制功能。

    该客户端继承自aiotieba.Client，并在其基础上实现了速率限制和并发控制。
    通过装饰器和上下文管理器的方式，为所有API调用提供统一的速率限制和并发控制。

    Attributes:
        limiter (AsyncLimiter): 用于控制每秒请求数的限流器。
        semaphore (asyncio.Semaphore): 用于控制最大并发数的信号量。
    """

    def __init__(
        self,
        *args,
        limiter: AsyncLimiter,
        semaphore: asyncio.Semaphore,
        cooldown_seconds_429: float,
        **kwargs,
    ):
        """初始化扩展的aiotieba客户端。

        Args:
            *args: 传递给父类构造函数的参数。
            limiter: 速率限制器，用于控制每秒请求数。
            semaphore: 信号量，用于控制最大并发数。
            cooldown_seconds: 触发429时的全局冷却秒数。
            **kwargs: 传递给父类构造函数的关键字参数。
        """
        super().__init__(*args, **kwargs)
        self._limiter = limiter
        self._semaphore = semaphore
        self._cooldown_seconds_429 = cooldown_seconds_429
        self._cooldown_until: float = 0.0
        self._cooldown_lock = asyncio.Lock()

    async def set_cooldown(self, duration: float):
        """设置全局冷却时间，防止多个任务同时设置。"""
        async with self._cooldown_lock:
            cooldown_end_time = time.monotonic() + duration
            self._cooldown_until = max(self._cooldown_until, cooldown_end_time)

    @property
    def limiter(self) -> AsyncLimiter:
        """获取速率限制器。"""
        return self._limiter

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """获取信号量。"""
        return self._semaphore

    @asynccontextmanager
    async def rate_limiter(self) -> AsyncGenerator[None, None]:
        """获取速率限制和并发控制的上下文管理器，并处理全局冷却。"""
        now = time.monotonic()
        if now < self._cooldown_until:
            wait_time = self._cooldown_until - now
            logger.debug("Global cooldown active. Waiting for %.1f seconds.", wait_time)
            await asyncio.sleep(wait_time)

        async with self.limiter:
            async with self.semaphore:
                yield

    @with_limit_and_retry
    async def get_threads(self, *args, **kwargs):
        return await super().get_threads(*args, **kwargs)

    @with_limit_and_retry
    async def get_posts(self, *args, **kwargs):
        return await super().get_posts(*args, **kwargs)

    @with_limit_and_retry
    async def get_comments(self, *args, **kwargs):
        return await super().get_comments(*args, **kwargs)

    @with_limit_and_retry
    async def get_fid(self, *args, **kwargs):
        return await super().get_fid(*args, **kwargs)

    async def __aenter__(self) -> Client:
        await super().__aenter__()
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        await super().__aexit__(exc_type, exc_val, exc_tb)
