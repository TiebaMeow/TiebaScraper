"""应用程序配置管理模块。

该模块负责从TOML配置文件中加载应用程序的各项配置，
包括数据库连接、Redis连接、贴吧认证信息、爬虫限制等。
支持周期性和回溯两种运行模式的配置。
"""

import tomllib
from pathlib import Path
from typing import ClassVar, Literal
from urllib.parse import quote_plus

from pydantic import (
    BaseModel,
    Field,
    PostgresDsn,
    RedisDsn,
    ValidationError,
    WebsocketUrl,
    computed_field,
)


class DatabaseConfig(BaseModel):
    """数据库配置模型"""

    host: str = "localhost"
    port: int = 5432
    username: str = "admin"
    password: str = "123456"
    db_name: str = "tieba_data"
    partition_enabled: bool = False
    p_interval: str = "1 month"
    p_premake: int = 4


class RedisConfig(BaseModel):
    """Redis配置模型"""

    host: str = "localhost"
    port: int = 6379
    username: str = ""
    password: str = ""
    db: int = 0


class TiebaConfig(BaseModel):
    """贴吧相关配置模型"""

    forums: list[str] = Field(..., min_length=1)
    max_backfill_pages: int = Field(100, gt=0)


class RateLimitConfig(BaseModel):
    """请求频率限制配置模型"""

    rps: int = Field(10, gt=0)
    concurrency: int = Field(8, gt=0)
    cooldown_seconds_429: float = Field(5.0, gt=0)


class CacheConfig(BaseModel):
    """缓存配置模型"""

    backend: Literal["memory", "redis"] = "memory"
    max_size: int = Field(100000, gt=0)
    ttl_seconds: int = Field(86400, gt=0)


class SchedulerConfig(BaseModel):
    """调度器配置模型"""

    interval_seconds: int = Field(60, gt=0)
    good_page_every_n_ticks: int = Field(10, gt=0)
    maintenance_every_n_ticks: int = Field(10, gt=0)
    maintenance_enabled: bool = True


class WebSocketConfig(BaseModel):
    """WebSocket 模式配置"""

    enabled: bool = True
    host: str = "localhost"
    port: int = 8000
    path: str = "/ws"
    token: str | None = None


class ConsumerConfig(BaseModel):
    """消费者推送配置"""

    transport: Literal["redis", "websocket", "none"] = "websocket"
    mode: Literal["id", "object"] = "id"
    max_len: int = Field(10000, gt=0)
    id_queue_key: str = "scraper:tieba:queue"
    stream_prefix: str = "scraper:tieba:events"
    timeout_ms: int = Field(2000, gt=0)
    max_retries: int = Field(5, gt=0)
    retry_backoff_ms: int = Field(200, gt=0)


class PydanticConfig(BaseModel):
    """Pydantic总配置模型"""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    tieba: TiebaConfig
    rate_limit: RateLimitConfig = Field(
        default_factory=lambda: RateLimitConfig(rps=10, concurrency=8, cooldown_seconds_429=5.0)
    )
    cache: CacheConfig = Field(
        default_factory=lambda: CacheConfig(backend="memory", max_size=100000, ttl_seconds=86400)
    )
    scheduler: SchedulerConfig = Field(
        default_factory=lambda: SchedulerConfig(
            interval_seconds=60, good_page_every_n_ticks=10, maintenance_every_n_ticks=10
        )
    )
    websocket: WebSocketConfig = Field(
        default_factory=lambda: WebSocketConfig(enabled=True, host="localhost", port=8000)
    )
    consumer: ConsumerConfig = Field(
        default_factory=lambda: ConsumerConfig(max_len=10000, timeout_ms=2000, max_retries=5, retry_backoff_ms=200)
    )

    @computed_field
    @property
    def database_url(self) -> PostgresDsn:
        """生成PostgreSQL数据库连接URL"""
        return PostgresDsn(
            f"postgresql+asyncpg://{quote_plus(self.database.username)}:{quote_plus(self.database.password)}"
            f"@{self.database.host}:{self.database.port}/{self.database.db_name}"
        )

    @computed_field
    @property
    def redis_url(self) -> RedisDsn:
        """生成Redis连接URL"""
        if self.redis.username and self.redis.password:
            return RedisDsn(
                f"redis://{quote_plus(self.redis.username)}:{quote_plus(self.redis.password)}"
                f"@{self.redis.host}:{self.redis.port}/{self.redis.db}"
            )
        if self.redis.password:
            return RedisDsn(
                f"redis://:{quote_plus(self.redis.password)}@{self.redis.host}:{self.redis.port}/{self.redis.db}"
            )
        return RedisDsn(f"redis://{self.redis.host}:{self.redis.port}/{self.redis.db}")

    @computed_field
    @property
    def websocket_url(self) -> WebsocketUrl:
        """生成WebSocket连接URL"""
        return WebsocketUrl(f"ws://{self.websocket.host}:{self.websocket.port}{self.websocket.path}")


class Config:
    """应用程序配置类。

    负责加载和管理应用程序的所有配置项，包括：
    - 数据库连接配置
    - Redis缓存配置
    - 贴吧API认证配置
    - 爬虫限制和调度配置

    Attributes:
        BASE_DIR (Path): 项目根目录路径
        CONFIG_FILE (Path): 配置文件路径
        pydantic_config (PydanticConfig): Pydantic应用配置模型
        mode (str): 运行模式('periodic'或'backfill')
    """

    BASE_DIR: ClassVar[Path] = Path(__file__).resolve().parent.parent
    CONFIG_FILE: ClassVar[Path] = BASE_DIR / "config.toml"

    pydantic_config: PydanticConfig
    mode: Literal["periodic", "backfill", "hybrid"]

    def __init__(self, mode: Literal["periodic", "backfill", "hybrid"] = "periodic"):
        """初始化配置对象。

        从配置文件中加载各项配置，构建数据库和Redis连接URL，
        并设置爬虫相关的限制参数。

        Args:
            mode: 运行模式，可选值为'periodic'(周期性)、'backfill'(回溯)或'hybrid'(混合)。
                默认为'periodic'。
        """
        config_data = self._load_toml()

        try:
            self.pydantic_config = PydanticConfig.model_validate(config_data)
        except ValidationError as e:
            raise ValueError(f"配置文件验证失败: {e}") from e

        self.mode = mode

    @classmethod
    def _load_toml(cls) -> dict:
        """从配置文件加载配置数据。

        Returns:
            dict: 配置数据字典，如果加载失败则引发异常。
        """
        if not cls.CONFIG_FILE.exists():
            raise FileNotFoundError(f"错误: 配置文件 {cls.CONFIG_FILE} 未找到。")
        with cls.CONFIG_FILE.open("rb") as f:
            return tomllib.load(f)

    @property
    def database_url(self) -> str:
        return str(self.pydantic_config.database_url)

    @property
    def p_interval(self) -> str:
        return self.pydantic_config.database.p_interval

    @property
    def p_premake(self) -> int:
        return self.pydantic_config.database.p_premake

    @property
    def partition_enabled(self) -> bool:
        return self.pydantic_config.database.partition_enabled

    @property
    def redis_url(self) -> str:
        return str(self.pydantic_config.redis_url)

    @property
    def forums(self) -> list[str]:
        return self.pydantic_config.tieba.forums

    @property
    def max_backfill_pages(self) -> int:
        return self.pydantic_config.tieba.max_backfill_pages

    @property
    def rps_limit(self) -> int:
        return self.pydantic_config.rate_limit.rps

    @property
    def concurrency_limit(self) -> int:
        return self.pydantic_config.rate_limit.concurrency

    @property
    def cooldown_seconds_429(self) -> float:
        return self.pydantic_config.rate_limit.cooldown_seconds_429

    @property
    def cache_backend(self) -> Literal["memory", "redis"]:
        return self.pydantic_config.cache.backend

    @property
    def cache_max_size(self) -> int:
        return self.pydantic_config.cache.max_size

    @property
    def cache_ttl_seconds(self) -> int:
        return self.pydantic_config.cache.ttl_seconds

    @property
    def scheduler_interval_seconds(self) -> int:
        return self.pydantic_config.scheduler.interval_seconds

    @property
    def good_page_every_ticks(self) -> int:
        return self.pydantic_config.scheduler.good_page_every_n_ticks

    @property
    def maintenance_every_ticks(self) -> int:
        return self.pydantic_config.scheduler.maintenance_every_n_ticks

    @property
    def maintenance_enabled(self) -> bool:
        return self.pydantic_config.scheduler.maintenance_enabled

    @property
    def websocket_url(self) -> WebsocketUrl:
        return self.pydantic_config.websocket_url

    @property
    def websocket_enabled(self) -> bool:
        return self.pydantic_config.websocket.enabled

    @property
    def websocket_token(self) -> str | None:
        return self.pydantic_config.websocket.token

    @property
    def consumer_transport(self) -> Literal["redis", "websocket", "none"]:
        return self.pydantic_config.consumer.transport

    @property
    def consumer_mode(self) -> Literal["id", "object"]:
        return self.pydantic_config.consumer.mode

    @property
    def consumer_config(self) -> ConsumerConfig:
        return self.pydantic_config.consumer
