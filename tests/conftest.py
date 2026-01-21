"""Pytest 配置和共享 fixtures。"""
# ruff: noqa: E402

import sys
from pathlib import Path

# Add the project root to sys.path so `from src...` works (src is a package)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from tiebameow.models.dto import (
    BaseForumDTO,
    BaseThreadDTO,
    CommentDTO,
    CommentsDTO,
    CommentUserDTO,
    PageInfoDTO,
    PostDTO,
    PostsDTO,
    PostUserDTO,
    ThreadDTO,
    ThreadsDTO,
    ThreadUserDTO,
)
from tiebameow.schemas.fragments import Fragment, FragTextModel

# ==================== Dummy 配置和容器类 ====================


@dataclass
class DummyConfig:
    """测试用配置模拟类"""

    scheduler_interval_seconds: int = 0
    good_page_every_ticks: int = 2
    mode: str = "periodic"
    max_backfill_pages: int = 5
    backfill_force_scan: bool = False
    deep_scan_enabled: bool = False
    deep_scan_depth: int = 3
    default_forums: list[str] = field(default_factory=lambda: ["bar", "baz"])
    groups: list = field(default_factory=list)


@dataclass
class DummyForum:
    """测试用贴吧模拟类"""

    fid: int
    fname: str


@dataclass
class DummyContainer:
    """测试用容器模拟类"""

    config: DummyConfig
    forums: list[DummyForum]
    tb_client: Any = None
    redis_client: Any = None


# ==================== DTO 工厂函数 ====================


def make_user(dto_class, **overrides):
    """创建测试用的用户 DTO"""
    defaults = {
        "user_id": 1,
        "portrait": "p",
        "user_name": "u",
        "nick_name_new": "n",
        "level": 1,
        "glevel": 1,
        "gender": "UNKNOWN",
        "icons": [],
        "is_bawu": False,
        "is_vip": False,
        "is_god": False,
        "priv_like": "PUBLIC",
        "priv_reply": "ALL",
    }
    if dto_class is PostUserDTO:
        defaults["ip"] = "127.0.0.1"
    if dto_class is CommentUserDTO:
        defaults["ip"] = "127.0.0.1"

    defaults.update(overrides)
    return dto_class(**defaults)


def make_contents() -> list[Fragment]:
    """创建测试用的内容片段"""
    return [FragTextModel(text="sample text")]


def make_thread(
    *,
    tid: int = 1,
    fid: int = 10,
    fname: str = "bar",
    last_time: int | datetime = 1,
    reply_num: int = 1,
    user: ThreadUserDTO | None = None,
    is_livepost: bool = False,
) -> ThreadDTO:
    """创建测试用的 ThreadDTO"""
    user = user or make_user(ThreadUserDTO)
    if isinstance(last_time, int):
        last_time = datetime.fromtimestamp(last_time)

    return ThreadDTO(
        tid=tid,
        fid=fid,
        fname=fname,
        pid=tid,
        author_id=user.user_id,
        author=user,
        title="t",
        contents=make_contents(),
        is_good=False,
        is_top=False,
        is_share=False,
        is_hide=False,
        is_livepost=is_livepost,
        is_help=False,
        agree_num=0,
        disagree_num=0,
        reply_num=reply_num,
        view_num=0,
        share_num=0,
        create_time=datetime.fromtimestamp(1),
        last_time=last_time,
        thread_type=0,
        tab_id=0,
        share_origin=BaseThreadDTO(pid=0, tid=0, fid=0, fname="", author_id=0, title="", contents=[]),
    )


def make_post(
    *,
    pid: int = 100,
    tid: int = 1,
    fid: int = 10,
    fname: str = "bar",
    floor: int = 2,
    reply_num: int = 0,
    user: PostUserDTO | None = None,
    comments: list[CommentDTO] | None = None,
    create_time: datetime | None = None,
) -> PostDTO:
    """创建测试用的 PostDTO"""
    user = user or make_user(PostUserDTO)
    return PostDTO(
        pid=pid,
        tid=tid,
        fid=fid,
        fname=fname,
        author_id=user.user_id,
        author=user,
        contents=make_contents(),
        sign="",
        comments=comments or [],
        is_aimeme=False,
        is_thread_author=False,
        agree_num=0,
        disagree_num=0,
        reply_num=reply_num,
        create_time=create_time or datetime.fromtimestamp(1),
        floor=floor,
    )


def make_comment(
    *,
    cid: int = 1000,
    pid: int = 100,
    tid: int = 1,
    fid: int = 10,
    fname: str = "bar",
    user: CommentUserDTO | None = None,
    reply_to_id: int = 0,
    floor: int = 1,
) -> CommentDTO:
    """创建测试用的 CommentDTO"""
    user = user or make_user(CommentUserDTO)
    return CommentDTO(
        cid=cid,
        pid=pid,
        tid=tid,
        fid=fid,
        fname=fname,
        author_id=user.user_id,
        author=user,
        contents=make_contents(),
        reply_to_id=reply_to_id,
        is_thread_author=False,
        agree_num=0,
        disagree_num=0,
        create_time=datetime.fromtimestamp(1),
        floor=floor,
    )


def make_threads_response(
    threads: list[ThreadDTO],
    *,
    has_more: bool = False,
    current_page: int = 1,
    total_page: int = 1,
) -> ThreadsDTO:
    """创建测试用的 ThreadsDTO 响应"""
    page_info = PageInfoDTO(
        page_size=30,
        current_page=current_page,
        total_page=total_page,
        total_count=len(threads),
        has_more=has_more,
        has_prev=current_page > 1,
    )
    forum = BaseForumDTO(fid=threads[0].fid if threads else 10, fname=threads[0].fname if threads else "bar")
    return ThreadsDTO(objs=threads, page=page_info, forum=forum)


def make_posts_response(
    posts: list[PostDTO],
    *,
    has_more: bool = False,
    current_page: int = 1,
    total_page: int = 1,
    thread: ThreadDTO | None = None,
) -> PostsDTO:
    """创建测试用的 PostsDTO 响应"""
    page_info = PageInfoDTO(
        page_size=30,
        current_page=current_page,
        total_page=total_page,
        total_count=len(posts),
        has_more=has_more,
        has_prev=current_page > 1,
    )
    forum = BaseForumDTO(fid=posts[0].fid if posts else 10, fname=posts[0].fname if posts else "bar")
    return PostsDTO(objs=posts, page=page_info, forum=forum)


def make_comments_response(
    comments: list[CommentDTO],
    *,
    has_more: bool = False,
    current_page: int = 1,
    total_page: int = 1,
) -> CommentsDTO:
    """创建测试用的 CommentsDTO 响应"""
    page_info = PageInfoDTO(
        page_size=30,
        current_page=current_page,
        total_page=total_page,
        total_count=len(comments),
        has_more=has_more,
        has_prev=current_page > 1,
    )
    forum = BaseForumDTO(fid=comments[0].fid if comments else 10, fname=comments[0].fname if comments else "bar")
    return CommentsDTO(objs=comments, page=page_info, forum=forum)


# ==================== Fixtures ====================


@pytest.fixture
def dummy_config():
    """返回一个测试用配置"""
    return DummyConfig()


@pytest.fixture
def dummy_forum():
    """返回一个测试用贴吧"""
    return DummyForum(fid=1, fname="bar")


@pytest.fixture
def dummy_container(dummy_config, dummy_forum):
    """返回一个测试用容器"""
    return DummyContainer(config=dummy_config, forums=[dummy_forum])


@pytest.fixture
def mock_datastore():
    """返回一个带有常用方法的 mock DataStore"""
    from types import SimpleNamespace

    return SimpleNamespace(
        filter_new_ids=AsyncMock(return_value=set()),
        save_items=AsyncMock(),
        get_threads_by_tids=AsyncMock(return_value=[]),
        get_posts_by_pids=AsyncMock(return_value=[]),
        push_to_id_queue=AsyncMock(),
        push_object_event=AsyncMock(),
        update_thread_metadata=AsyncMock(),
    )


@pytest.fixture
def mock_tb_client():
    """返回一个带有常用方法的 mock TiebaClient"""
    from types import SimpleNamespace

    return SimpleNamespace(
        get_threads_dto=AsyncMock(),
        get_posts_dto=AsyncMock(),
        get_comments_dto=AsyncMock(),
    )
