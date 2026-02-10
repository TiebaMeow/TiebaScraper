from prometheus_client import Counter, Gauge, Histogram

# 爬取条目统计
SCRAPED_ITEMS = Counter(
    "tieba_scraped_items_total",
    "Total number of items scraped",
    ["type", "forum", "status"],  # type: thread, post, comment; status: success, error
)

# 任务处理耗时分布
TASK_DURATION = Histogram(
    "tieba_task_duration_seconds",
    "Time spent processing a specific task",
    ["type"],  # type: ScanThreadsTask, FullScanPostsTask, IncrementalScanCommentsTask, etc.
)

# API 请求耗时分布
API_REQUEST_DURATION = Histogram(
    "tieba_api_request_duration_seconds",
    "Time spent on actual API network requests",
    ["method", "status"],  # method: get_threads, etc.; status: 'error_deleted', 'error_<code>', or 'unexpected_error'
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)

# 限流等待耗时分布
RATELIMIT_WAIT_DURATION = Histogram(
    "tieba_ratelimit_wait_seconds",
    "Time spent waiting for rate limiter acquisition",
    ["limiter_type"],  # threads, posts, comments
)

# 队列大小（背压）
QUEUE_SIZE = Gauge(
    "tieba_queue_size",
    "Current number of tasks in the queue",
    ["priority"],
)

# 活跃 Worker 数量
ACTIVE_WORKERS = Gauge(
    "tieba_active_workers",
    "Number of currently active workers",
)

# Worker 错误统计
WORKER_ERRORS = Counter(
    "tieba_worker_errors_total",
    "Total number of unhandled worker errors",
    ["handler_type"],
)

# 数据库操作统计
DB_OPERATIONS = Counter(
    "tieba_db_operations_total",
    "Total number of database operations",
    ["operation", "table", "status"],  # operation: insert, upsert; status: success, error, integrity_error
)

# 缓存命中统计
CACHE_HITS = Counter(
    "tieba_cache_hits_total",
    "Total number of cache hits",
    ["type"],  # type: thread, post, comment
)

# 缓存未命中统计（在数据库中找到但不在缓存中）
CACHE_MISSES = Counter(
    "tieba_cache_misses_total",
    "Total number of cache misses (items found in DB but not in cache)",
    ["type"],
)

# Periodic 模式下的贴吧数量
PERIODIC_FORUMS_COUNT = Gauge(
    "tieba_periodic_forums_count",
    "Number of forums currently being monitored in periodic mode",
    ["group"],
)

# Backfill 模式下的贴吧数量
BACKFILL_FORUMS_COUNT = Gauge(
    "tieba_backfill_forums_count",
    "Number of forums currently being processed in backfill mode",
)
