"""应用程序配置管理模块。

该模块负责从TOML配置文件中加载应用程序的各项配置，
包括数据库连接、Redis连接、贴吧认证信息、爬虫限制等。
支持周期性和回溯两种运行模式的配置。
支持通过环境变量覆盖配置（例如 DATABASE__HOST）。
"""

import tomllib
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote_plus, urlparse, urlunparse

from pydantic import (
    BaseModel,
    Field,
    PostgresDsn,
    RedisDsn,
    ValidationError,
    WebsocketUrl,
    computed_field,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class DatabaseConfig(BaseModel):
    """数据库配置模型"""

    host: str = "localhost"
    port: int = 5432
    username: str = "admin"
    password: str = "123456"
    db_name: str = "tieba_data"
    partition_enabled: bool = False
    p_interval: str = "1 month"


class RedisConfig(BaseModel):
    """Redis配置模型"""

    host: str = "localhost"
    port: int = 6379
    username: str = ""
    password: str = ""
    db: int = 0
    stream_request: str = "scraper:tieba:cmd:req"
    stream_response: str = "scraper:tieba:cmd:res"


class ForumGroup(BaseModel):
    """贴吧分组配置"""

    name: str
    forums: list[str]
    interval_seconds: int | None = None


class TiebaConfig(BaseModel):
    """贴吧相关配置模型"""

    forums: list[str] = []
    groups: list[ForumGroup] = []
    max_backfill_pages: int = Field(100, gt=0)
    backfill_force_scan: bool = False

    @model_validator(mode="after")
    def check_forums_exist(self) -> "TiebaConfig":
        if not self.forums and not self.groups:
            pass
        return self


class RateLimitConfig(BaseModel):
    """请求频率限制配置模型"""

    # 全局配置（向后兼容）
    rps: int = Field(default=10, gt=0)
    concurrency: int = Field(default=8, gt=0)
    cooldown_seconds_429: float = Field(default=5.0, gt=0)

    # 分接口 RPS 配置（可选，未设置则使用默认值）
    # 基于测试结果：posts 令牌桶容量≈57, 恢复速率≈2.2/s
    threads_rps: float = Field(default=2.0, gt=0, description="get_threads 接口的 RPS 限制")
    posts_rps: float = Field(default=2.0, gt=0, description="get_posts 接口的 RPS 限制")
    comments_rps: float = Field(default=5.0, gt=0, description="get_comments 接口的 RPS 限制")


class CacheConfig(BaseModel):
    """缓存配置模型"""

    backend: Literal["memory", "redis"] = "memory"
    max_size: int = Field(100000, gt=0)
    ttl_seconds: int = Field(86400, gt=0)


class SchedulerConfig(BaseModel):
    """调度器配置模型"""

    interval_seconds: int = Field(default=60, gt=0)
    good_page_every_n_ticks: int = Field(default=10, gt=0)
    # 队列感知调度：队列深度超过阈值时跳过本轮调度
    queue_depth_threshold: int = Field(default=0, ge=0, description="队列深度阈值，0 表示禁用")
    # 跳过调度时的等待时间（秒），避免频繁检查
    skip_wait_seconds: int = Field(default=5, gt=0, description="跳过调度后的等待时间")


class DeepScanConfig(BaseModel):
    """DeepScan 深度扫描配置模型"""

    enabled: bool = False
    depth: int = Field(3, gt=0, description="扫描前 n 页 + 后 n 页")


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


class ProxyConfig(BaseModel):
    """代理配置模型

    支持 HTTP/HTTPS/SOCKS4/SOCKS5 代理。
    支持单个代理或多代理轮换模式。
    """

    enabled: bool = False
    url: str = ""
    username: str = ""
    password: str = ""
    # 多代理支持：配置多个代理 URL 以实现轮换，可线性提升吞吐量
    urls: list[str] = []

    @property
    def proxy_url(self) -> str | None:
        """生成完整的代理 URL（包含认证信息）"""
        if not self.enabled or not self.url:
            return None

        if self.username and self.password:
            parsed = urlparse(self.url)
            netloc = f"{quote_plus(self.username)}:{quote_plus(self.password)}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return urlunparse((parsed.scheme, netloc, parsed.path, "", "", ""))

        return self.url

    @property
    def proxy_urls(self) -> list[str]:
        """获取所有代理 URL 列表（用于轮换）

        如果配置了 urls 列表则使用列表，否则使用单个 url。
        """
        if not self.enabled:
            return []

        if self.urls:
            return self.urls

        single_url = self.proxy_url
        return [single_url] if single_url else []

    @property
    def is_multi_proxy(self) -> bool:
        """是否配置了多代理"""
        return self.enabled and len(self.urls) > 1


class PydanticConfig(BaseSettings):
    """Pydantic总配置模型"""

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    tieba: TiebaConfig = Field(default_factory=lambda: TiebaConfig(max_backfill_pages=100))
    rate_limit: RateLimitConfig = Field(default_factory=lambda: RateLimitConfig())
    cache: CacheConfig = Field(
        default_factory=lambda: CacheConfig(backend="memory", max_size=100000, ttl_seconds=86400)
    )
    scheduler: SchedulerConfig = Field(
        default_factory=lambda: SchedulerConfig(interval_seconds=60, good_page_every_n_ticks=10)
    )
    deep_scan: DeepScanConfig = Field(default_factory=lambda: DeepScanConfig(enabled=False, depth=3))
    websocket: WebSocketConfig = Field(
        default_factory=lambda: WebSocketConfig(enabled=True, host="localhost", port=8000)
    )
    consumer: ConsumerConfig = Field(
        default_factory=lambda: ConsumerConfig(max_len=10000, timeout_ms=2000, max_retries=5, retry_backoff_ms=200)
    )
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            TomlConfigSettingsSource(settings_cls),
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


class TomlConfigSettingsSource(PydanticBaseSettingsSource):
    """TOML 配置文件加载源"""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        raise NotImplementedError

    def __call__(self) -> dict[str, Any]:
        config_file = Path(__file__).resolve().parent.parent.parent / "config.toml"
        if not config_file.exists():
            return {}
        try:
            with config_file.open("rb") as f:
                return tomllib.load(f)
        except Exception:
            return {}


class Config:
    """应用程序配置类。

    负责加载和管理应用程序的所有配置项，包括：
    - 数据库连接配置
    - Redis缓存配置
    - 贴吧API认证配置
    - 爬虫限制和调度配置

    Attributes:
        pydantic_config (PydanticConfig): Pydantic应用配置模型
        mode (str): 运行模式('periodic'或'backfill')
    """

    pydantic_config: PydanticConfig
    mode: Literal["periodic", "backfill", "hybrid"]

    def __init__(self, mode: Literal["periodic", "backfill", "hybrid"] = "periodic"):
        """初始化配置对象。

        配置加载优先级：
        1. 环境变量 (例如 DATABASE__HOST)
        2. config.toml 配置文件

        Args:
            mode: 运行模式，可选值为'periodic'(周期性)、'backfill'(回溯)或'hybrid'(混合)。
                默认为'periodic'。
        """
        try:
            self.pydantic_config = PydanticConfig()
        except ValidationError as e:
            raise ValueError(f"配置验证失败: {e}") from e

        self.mode = mode

    @property
    def database_url(self) -> str:
        """获取数据库连接URL。"""
        return str(self.pydantic_config.database_url)

    @property
    def p_interval(self) -> str:
        """获取数据库分区间隔。"""
        return self.pydantic_config.database.p_interval

    @property
    def partition_enabled(self) -> bool:
        """获取是否启用数据库分区。"""
        return self.pydantic_config.database.partition_enabled

    @property
    def redis_url(self) -> str:
        """获取Redis连接URL。"""
        return str(self.pydantic_config.redis_url)

    @property
    def redis_stream_request(self) -> str:
        """获取 Redis 指令请求与 Stream 名称。"""
        return self.pydantic_config.redis.stream_request

    @property
    def redis_stream_response(self) -> str:
        """获取 Redis 指令响应 Stream 名称。"""
        return self.pydantic_config.redis.stream_response

    @property
    def forums(self) -> list[str]:
        """获取所有需要初始化的贴吧列表（去重）。"""
        s = set(self.pydantic_config.tieba.forums)
        for group in self.pydantic_config.tieba.groups:
            s.update(group.forums)
        return list(s)

    @property
    def default_forums(self) -> list[str]:
        """获取未分组的默认贴吧列表。"""
        return self.pydantic_config.tieba.forums

    @property
    def groups(self) -> list[ForumGroup]:
        """获取贴吧分组配置。"""
        return self.pydantic_config.tieba.groups

    @property
    def max_backfill_pages(self) -> int:
        """获取最大回溯深度配置。"""
        return self.pydantic_config.tieba.max_backfill_pages

    @property
    def backfill_force_scan(self) -> bool:
        """获取回溯模式是否强制扫描配置。"""
        return self.pydantic_config.tieba.backfill_force_scan

    @property
    def rps_limit(self) -> int:
        """获取每秒请求限制。"""
        return self.pydantic_config.rate_limit.rps

    @property
    def concurrency_limit(self) -> int:
        """获取并发限制。"""
        return self.pydantic_config.rate_limit.concurrency

    @property
    def cooldown_seconds_429(self) -> float:
        """获取429响应后的冷却时间（秒）。"""
        return self.pydantic_config.rate_limit.cooldown_seconds_429

    @property
    def threads_rps(self) -> float:
        """获取 get_threads 接口的 RPS 限制。"""
        return self.pydantic_config.rate_limit.threads_rps

    @property
    def posts_rps(self) -> float:
        """获取 get_posts 接口的 RPS 限制。"""
        return self.pydantic_config.rate_limit.posts_rps

    @property
    def comments_rps(self) -> float:
        """获取 get_comments 接口的 RPS 限制。"""
        return self.pydantic_config.rate_limit.comments_rps

    @property
    def cache_backend(self) -> Literal["memory", "redis"]:
        """获取缓存后端类型。"""
        return self.pydantic_config.cache.backend

    @property
    def cache_max_size(self) -> int:
        """获取缓存最大条目数。"""
        return self.pydantic_config.cache.max_size

    @property
    def cache_ttl_seconds(self) -> int:
        """获取缓存条目过期时间（秒）。"""
        return self.pydantic_config.cache.ttl_seconds

    @property
    def scheduler_interval_seconds(self) -> int:
        """获取调度器运行间隔时间（秒）。"""
        return self.pydantic_config.scheduler.interval_seconds

    @property
    def good_page_every_ticks(self) -> int:
        """获取调度器调度优质页的频率。"""
        return self.pydantic_config.scheduler.good_page_every_n_ticks

    @property
    def queue_depth_threshold(self) -> int:
        """获取队列深度阈值，超过此值时跳过调度。0 表示禁用。"""
        return self.pydantic_config.scheduler.queue_depth_threshold

    @property
    def skip_wait_seconds(self) -> int:
        """获取跳过调度后的等待时间（秒）。"""
        return self.pydantic_config.scheduler.skip_wait_seconds

    @property
    def deep_scan_enabled(self) -> bool:
        """获取是否启用 DeepScan 深度扫描。"""
        return self.pydantic_config.deep_scan.enabled

    @property
    def deep_scan_depth(self) -> int:
        """获取 DeepScan 深度扫描的页数深度。"""
        return self.pydantic_config.deep_scan.depth

    @property
    def websocket_url(self) -> WebsocketUrl:
        """获取WebSocket连接URL。"""
        return self.pydantic_config.websocket_url

    @property
    def websocket_enabled(self) -> bool:
        """获取是否启用WebSocket。"""
        return self.pydantic_config.websocket.enabled

    @property
    def websocket_token(self) -> str | None:
        """获取WebSocket认证令牌。"""
        return self.pydantic_config.websocket.token

    @property
    def consumer_transport(self) -> Literal["redis", "websocket", "none"]:
        """获取内容推送方式。"""
        return self.pydantic_config.consumer.transport

    @property
    def consumer_mode(self) -> Literal["id", "object"]:
        """获取内容推送模式。"""
        return self.pydantic_config.consumer.mode

    @property
    def consumer_config(self) -> ConsumerConfig:
        """获取内容推送配置对象。"""
        return self.pydantic_config.consumer

    @property
    def proxy_enabled(self) -> bool:
        """获取是否启用代理。"""
        return self.pydantic_config.proxy.enabled

    @property
    def proxy_url(self) -> str | None:
        """获取完整的代理 URL。"""
        return self.pydantic_config.proxy.proxy_url

    @property
    def proxy_urls(self) -> list[str]:
        """获取所有代理 URL 列表（用于轮换）。"""
        return self.pydantic_config.proxy.proxy_urls

    @property
    def is_multi_proxy(self) -> bool:
        """是否配置了多代理。"""
        return self.pydantic_config.proxy.is_multi_proxy
