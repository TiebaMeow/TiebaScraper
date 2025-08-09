import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import wraps
from inspect import iscoroutinefunction as awaitable
from typing import Any

import aiotieba as tb
from aiolimiter import AsyncLimiter
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt
from tenacity.wait import wait_exponential_jitter


class Client(tb.Client):
    """扩展的aiotieba客户端，添加了自定义的请求限流和并发控制功能。

    该客户端继承自aiotieba.Client，并在其基础上实现了速率限制和并发控制。
    通过装饰器和上下文管理器的方式，为所有API调用提供统一的速率限制和并发控制。

    Attributes:
        RATE_LIMITED_METHODS (set): 需要速率限制的API方法集合。
        limiter (AsyncLimiter): 用于控制每秒请求数的限流器。
        semaphore (asyncio.Semaphore): 用于控制最大并发数的信号量。
    """

    RATE_LIMITED_METHODS = {
        "get_threads",
        "get_posts",
        "get_comments",
        "get_fid",
    }

    def __init__(self, *args, limiter: AsyncLimiter, semaphore: asyncio.Semaphore, **kwargs):
        """初始化扩展的aiotieba客户端。

        Args:
            *args: 传递给父类构造函数的参数。
            limiter: 速率限制器，用于控制每秒请求数。
            semaphore: 信号量，用于控制最大并发数。
            **kwargs: 传递给父类构造函数的关键字参数。
        """
        super().__init__(*args, **kwargs)
        self._limiter = limiter
        self._semaphore = semaphore

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
        """获取速率限制和并发控制的上下文管理器。

        该方法确保所有aiotieba请求都受到速率限制和并发控制的约束。
        首先获取速率限制器的许可，然后获取信号量的许可。

        Yields:
            None: 上下文管理器
        """
        async with self.limiter:
            async with self.semaphore:
                yield

    def __getattribute__(self, name: str) -> Any:
        """拦截方法调用，为指定的API方法自动添加速率限制。

        Args:
            name (str): 方法名。

        Returns:
            Any: 返回原方法或装饰后的速率限制方法。
        """
        attr = super().__getattribute__(name)

        rate_limited_methods = object.__getattribute__(self, "RATE_LIMITED_METHODS")

        if name in rate_limited_methods and awaitable(attr):

            @wraps(attr)
            async def rate_limited_wrapper(*args, **kwargs) -> Any:
                async with self.rate_limiter():
                    async for attempt in AsyncRetrying(
                        stop=stop_after_attempt(3),
                        wait=wait_exponential_jitter(initial=0.2, max=2.0),
                        retry=retry_if_exception_type((asyncio.TimeoutError, ConnectionError, OSError)),
                        reraise=True,
                    ):
                        with attempt:
                            ret = await attr(*args, **kwargs)
                            err = getattr(ret, "err", None)
                            if err:
                                raise err
                            return ret

            return rate_limited_wrapper

        return attr

    async def __aenter__(self) -> "Client":
        await super().__aenter__()
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        await super().__aexit__(exc_type, exc_val, exc_tb)
