<div align="center">

# TiebaScraper

_基于 aiotieba 的高性能百度贴吧异步爬虫工具，支持实时监控和历史数据回溯。_

</div>

## 功能特性

- **实时监控模式**: 持续监控指定贴吧首页的最新内容并推送到 Redis 队列
- **历史数据回溯**: 抓取指定贴吧的历史数据
- **异步高性能**: 基于 asyncio 的全异步架构
- **数据持久化**: 支持 PostgreSQL 数据库存储
- **缓存支持**: 使用 Redis 进行缓存优化
- **限流保护**: 内置基于漏桶算法的限流器

## 环境要求

- Python 3.12+
- uv
- PostgreSQL
- Redis 6.0+

## 安装依赖

```shell
uv sync
```

## 配置

步骤 1：复制配置文件模板

```bash
cp config.example.toml config.toml
```

步骤 2：编辑 `config.toml` 文件，填入以下信息：

- `BDUSS`: 百度贴吧的身份认证信息
- `forums`: 要监控的论坛列表
- `database`: PostgreSQL 数据库连接信息
- `redis`: Redis 缓存服务器信息

## 使用方法

### 实时监控模式

```bash
python main.py
```

### 历史数据回溯模式

```bash
python main.py --mode backfill
```

## 项目结构

```text
src/
├── core/           # 核心模块（依赖注入、数据存储等）
├── models/         # 数据模型定义
├── scraper/        # 爬虫核心（调度器、任务、工作器）
└── utils/          # 工具类
```
