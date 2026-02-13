from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# 爬取条目统计
SCRAPED_ITEMS = Counter(
    "tiebascraper_scraped_items_total",
    "Total number of items scraped",
    ["type", "forum"],  # type: thread, post, comment
)

# 任务处理耗时分布
TASK_DURATION = Histogram(
    "tiebascraper_task_duration_seconds",
    "Time spent processing a specific task",
    ["type"],  # type: ScanThreadsTask, FullScanPostsTask, IncrementalScanCommentsTask, etc.
)

# API 请求耗时分布
API_REQUEST_DURATION = Histogram(
    "tiebascraper_api_request_duration_seconds",
    "Time spent on actual API network requests",
    # method: get_threads, etc.
    # status: 'success', 'error_deleted', 'error_<code>', or 'unexpected_error'
    ["method", "status"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)

# 限流等待耗时分布
RATELIMIT_WAIT_DURATION = Histogram(
    "tiebascraper_ratelimit_wait_seconds",
    "Time spent waiting for rate limiter acquisition",
    ["limiter_type"],  # threads, posts, comments
)

# 队列大小（背压）
QUEUE_SIZE = Gauge(
    "tiebascraper_queue_size",
    "Current number of tasks in the queue",
    ["lane"],  # lane: threads, posts, comments
)

# 活跃 Worker 数量
ACTIVE_WORKERS = Gauge(
    "tiebascraper_active_workers",
    "Number of currently active workers",
)

# Worker 错误统计
WORKER_ERRORS = Counter(
    "tiebascraper_worker_errors_total",
    "Total number of unhandled worker errors",
    ["handler_type"],
)

# 数据库操作统计
DB_OPERATIONS = Counter(
    "tiebascraper_db_operations_total",
    "Total number of database operations",
    ["operation", "table", "status"],  # operation: insert, upsert; status: success, error, integrity_error
)

# 缓存命中统计
CACHE_HITS = Counter(
    "tiebascraper_cache_hits_total",
    "Total number of cache hits",
    ["type"],  # type: thread, post, comment
)

# 缓存未命中统计（在数据库中找到但不在缓存中）
CACHE_MISSES = Counter(
    "tiebascraper_cache_misses_total",
    "Total number of cache misses (items found in DB but not in cache)",
    ["type"],
)

# Periodic 模式下的贴吧数量
PERIODIC_FORUMS_COUNT = Gauge(
    "tiebascraper_periodic_forums_count",
    "Number of forums currently being monitored in periodic mode",
    ["group"],
)

# Backfill 模式下的贴吧数量
BACKFILL_FORUMS_COUNT = Gauge(
    "tiebascraper_backfill_forums_count",
    "Number of forums currently being processed in backfill mode",
)

# 事件循环延迟
EVENT_LOOP_LAG = Histogram(
    "tiebascraper_event_loop_lag_seconds",
    "Event loop latency (lag) time",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
)

# 数据库连接池状态
DB_POOL_STATS = Gauge(
    "tiebascraper_db_pool_connections",
    "Connection pool statistics",
    ["state"],  # capacity, available, acquired, overflow
)
