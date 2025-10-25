"""数据模型定义模块。

该模块定义了所有与贴吧数据相关的SQLAlchemy ORM模型和Pydantic验证模型，
包括论坛、用户、主题贴、回复、楼中楼等实体，以及各种内容片段的数据模型。
"""

from __future__ import annotations

import dataclasses
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy import BIGINT, DateTime, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, foreign, mapped_column, relationship

from ..schemas import FRAG_MAP, Fragment, FragUnknownModel

if TYPE_CHECKING:
    import aiotieba.api.get_posts._classdef as aiotieba_posts
    import aiotieba.typing as aiotieba

    AiotiebaType = aiotieba.Thread | aiotieba.Post | aiotieba.Comment


__all__ = [
    "Forum",
    "User",
    "Thread",
    "Post",
    "Comment",
    "Fragment",
]


log = logging.getLogger("models")


class Base(DeclarativeBase):
    pass


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def now_with_tz():
    """返回带时区的当前时间。

    Returns:
        datetime: 上海时区的当前时间。
    """
    return datetime.now(SHANGHAI_TZ)


def _partition_enabled() -> bool:
    """从 config.toml 或环境变量读取是否启用分区，默认不启用。"""

    env_val = os.getenv("PARTITION_ENABLED")
    if env_val is not None:
        return str(env_val).strip().lower() in {"1", "true", "yes", "on"}

    try:
        root = Path(__file__).resolve().parents[2]
        cfg = root / "config.toml"
        if cfg.exists():
            import tomllib

            with cfg.open("rb") as f:
                data = tomllib.load(f)
            db = data.get("database") or {}
            val = db.get("partition_enabled")
            if isinstance(val, bool):
                return val
    except Exception:
        pass

    return False


_PARTITION_ENABLED = _partition_enabled()


def _with_partition(*indexes: Any):
    """根据开关拼装 __table_args__。

    当启用分区时附加 {"postgresql_partition_by": "RANGE (create_time)"}。
    """
    if _PARTITION_ENABLED:
        return (*indexes, {"postgresql_partition_by": "RANGE (create_time)"})
    return indexes


class MixinBase(Base):
    """为SQLAlchemy模型提供通用方法的混入类。"""

    __abstract__ = True

    def to_dict(self) -> dict[str, Any]:
        """将模型实例的列数据转换为字典。

        此方法包含直接映射到数据库表的列，用于批量插入操作。

        Returns:
            dict: 包含模型列名和对应值的字典。
        """
        result = {}
        for c in self.__table__.columns:
            value = getattr(self, c.name)
            if c.name == "contents" and value is not None:
                result[c.name] = [frag.model_dump() for frag in value]
            else:
                result[c.name] = value
        return result


class AiotiebaConvertible:
    """
    为可以从aiotieba对象转换的模型定义一个通用接口的抽象基类。
    """

    @staticmethod
    def _convert_fragment(obj: AiotiebaType) -> Fragment:
        """将单个爬取到的fragment对象转换为其对应的Pydantic模型实例。

        此方法会自动处理_t, _p, _c等后缀，
        通过反射动态构建目标模型名称并在当前模块的全局命名空间中选择相应的模型类。
        如果遇到不支持的类型，会使用FragTextModel作为默认类型。

        Args:
            obj: aiotieba返回的对象。

        Returns:
            models.Fragment: 转换后的Pydantic模型实例。
        """
        source_type_name = type(obj).__name__
        target_model_name = source_type_name.rsplit("_", 1)[0]

        target_model = FRAG_MAP.get(target_model_name)

        if target_model is None:
            log.warning(
                f"Unsupported fragment base type: '{target_model_name}' (from '{source_type_name}'), "
                f"using FragUnknownModel as default type."
            )
            return FragUnknownModel()

        data_dict = dataclasses.asdict(obj)
        return target_model(**data_dict)

    @classmethod
    def convert_content_list(cls, contents: list[Any]) -> list[Fragment]:
        """将爬取到的fragment对象列表转换为Pydantic模型实例列表。

        批量转换内容片段列表，每个片段都会调用convert_fragment函数进行转换。

        Args:
            contents: 从aiotieba库获取的内容片段对象列表。

        Returns:
            list[models.Fragment]: 转换后的Pydantic模型实例列表。
        """
        if not contents:
            return []
        return [cls._convert_fragment(frag) for frag in contents]


class Forum(MixinBase):
    """贴吧信息数据模型。

    Attributes:
        fid: 论坛ID，主键。
        fname: 论坛名称，建立索引用于快速查询。
        threads: 该论坛下的所有帖子，与Thread模型的反向关系。
    """

    __tablename__ = "forum"

    fid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    fname: Mapped[str] = mapped_column(String(255), index=True)

    threads: Mapped[list[Thread]] = relationship(
        "Thread",
        back_populates="forum",
        primaryjoin=lambda: Forum.fid == foreign(Thread.fid),
    )


class User(MixinBase):
    """用户数据模型。

    Attributes:
        user_id: 用户user_id，主键。
        portrait: 用户portrait。
        user_name: 用户名。
        nick_name: 用户昵称。
        threads: 该用户发布的所有帖子，与Thread模型的反向关系。
        posts: 该用户发布的所有回复，与Post模型的反向关系。
        comments: 该用户发布的所有评论，与Comment模型的反向关系。
    """

    __tablename__ = "user"

    user_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    portrait: Mapped[str] = mapped_column(String(255), nullable=True, index=True)
    user_name: Mapped[str] = mapped_column(String(255), nullable=True, index=True)
    nick_name: Mapped[str] = mapped_column(String(255), nullable=True, index=True)

    threads: Mapped[list[Thread]] = relationship(
        "Thread",
        back_populates="author",
        primaryjoin=lambda: User.user_id == foreign(Thread.author_id),
    )
    posts: Mapped[list[Post]] = relationship(
        "Post",
        back_populates="author",
        primaryjoin=lambda: User.user_id == foreign(Post.author_id),
    )
    comments: Mapped[list[Comment]] = relationship(
        "Comment",
        back_populates="author",
        primaryjoin=lambda: User.user_id == foreign(Comment.author_id),
    )

    @classmethod
    def from_aiotieba(cls, user: aiotieba.UserInfo) -> User:
        """从aiotieba.User对象创建User模型实例。

        Args:
            user: aiotieba返回的User对象。

        Returns:
            User: 转换后的User模型实例。
        """
        return cls(
            user_id=user.user_id,
            portrait=user.portrait,
            user_name=user.user_name,
            nick_name=user.nick_name,
        )


class Thread(MixinBase, AiotiebaConvertible):
    """主题贴数据模型。

    Attributes:
        tid: 主题贴tid，与create_time组成复合主键。
        create_time: 主题贴创建时间，带时区信息，与tid组成复合主键。
        title: 主题贴标题内容。
        text: 主题贴的纯文本内容。
        contents: 正文内容碎片列表，以JSONB格式存储。
        last_time: 最后回复时间，以秒为单位的10位时间戳。
        reply_num: 回复数。
        author_level: 作者在主题贴所在吧的等级。
        scrape_time: 数据抓取时间。
        fid: 所属贴吧fid，外键关联到Forum表。
        author_id: 作者user_id，外键关联到User表。
        forum: 所属贴吧对象，与Forum模型的关系。
        author: 作者用户对象，与User模型的关系。
        posts: 该贴子下的所有回复，与Post模型的反向关系。
    """

    __tablename__ = "thread"
    __table_args__ = _with_partition(
        Index("idx_thread_forum_ctime", "fid", "create_time"),
        Index("idx_thread_forum_ltime", "fid", "last_time"),
        Index("idx_thread_author_time", "author_id", "create_time"),
        Index("idx_thread_author_forum_time", "author_id", "fid", "create_time"),
    )

    tid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    create_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    title: Mapped[str] = mapped_column(String(255))
    text: Mapped[str] = mapped_column(Text)
    contents: Mapped[list[Fragment] | None] = mapped_column(JSONB, nullable=True)
    last_time: Mapped[int] = mapped_column(BIGINT)
    reply_num: Mapped[int] = mapped_column(Integer)
    author_level: Mapped[int] = mapped_column(Integer)
    scrape_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_with_tz)

    fid: Mapped[int] = mapped_column(BIGINT, index=True)
    author_id: Mapped[int] = mapped_column(BIGINT, index=True)

    forum: Mapped[Forum] = relationship(
        "Forum",
        back_populates="threads",
        primaryjoin=lambda: foreign(Thread.fid) == Forum.fid,
    )
    author: Mapped[User] = relationship(
        "User",
        back_populates="threads",
        primaryjoin=lambda: foreign(Thread.author_id) == User.user_id,
    )
    posts: Mapped[list[Post]] = relationship(
        "Post",
        back_populates="thread",
        primaryjoin=lambda: Thread.tid == foreign(Post.tid),
    )

    @classmethod
    def from_aiotieba(cls, thread: aiotieba.Thread) -> Thread:
        """从aiotieba.Thread对象创建Thread模型实例。

        Args:
            thread: aiotieba返回的Thread对象。

        Returns:
            Thread: 转换后的Thread模型实例。
        """
        return cls(
            tid=thread.tid,
            create_time=datetime.fromtimestamp(thread.create_time, tz=SHANGHAI_TZ),
            title=thread.title,
            text=thread.contents.text,
            contents=cls.convert_content_list(thread.contents.objs),
            last_time=thread.last_time,
            reply_num=thread.reply_num,
            author_level=thread.user.level,
            scrape_time=now_with_tz(),
            fid=thread.fid,
            author_id=thread.user.user_id,
        )


class Post(MixinBase, AiotiebaConvertible):
    """回复数据模型。

    Attributes:
        pid: 回复pid，与create_time组成复合主键。
        create_time: 回复创建时间，带时区信息，与pid组成复合主键。
        text: 回复的纯文本内容。
        contents: 回复的正文内容碎片列表，以JSONB格式存储。
        floor: 楼层号。
        reply_num: 该回复下的楼中楼数量。
        author_level: 作者在主题贴所在吧的等级。
        scrape_time: 数据抓取时间。
        tid: 所属贴子tid，外键关联到Thread表。
        author_id: 作者user_id，外键关联到User表。
        thread: 所属主题贴对象，与Thread模型的关系。
        author: 作者用户对象，与User模型的关系。
        comments: 该回复下的所有楼中楼，与Comment模型的反向关系。
    """

    __tablename__ = "post"
    __table_args__ = _with_partition(
        Index("idx_post_thread_time", "tid", "create_time"),
        Index("idx_post_author_time", "author_id", "create_time"),
    )

    pid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    create_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    text: Mapped[str] = mapped_column(Text)
    contents: Mapped[list[Fragment] | None] = mapped_column(JSONB, nullable=True)
    floor: Mapped[int] = mapped_column(Integer)
    reply_num: Mapped[int] = mapped_column(Integer)
    author_level: Mapped[int] = mapped_column(Integer)
    scrape_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_with_tz)

    tid: Mapped[int] = mapped_column(BIGINT, index=True)
    author_id: Mapped[int] = mapped_column(BIGINT, index=True)

    thread: Mapped[Thread] = relationship(
        "Thread",
        back_populates="posts",
        primaryjoin=lambda: foreign(Post.tid) == Thread.tid,
    )
    author: Mapped[User] = relationship(
        "User",
        back_populates="posts",
        primaryjoin=lambda: foreign(Post.author_id) == User.user_id,
    )
    comments: Mapped[list[Comment]] = relationship(
        "Comment",
        back_populates="post",
        primaryjoin=lambda: Post.pid == foreign(Comment.pid),
    )

    @classmethod
    def from_aiotieba(cls, post: aiotieba.Post) -> Post:
        """从aiotieba.Post对象创建Post模型实例。

        Args:
            post: aiotieba返回的Post对象。

        Returns:
            Post: 转换后的Post模型实例。
        """
        return cls(
            pid=post.pid,
            create_time=datetime.fromtimestamp(post.create_time, tz=SHANGHAI_TZ),
            text=post.text,
            contents=cls.convert_content_list(post.contents.objs),
            floor=post.floor,
            reply_num=post.reply_num,
            author_level=post.user.level,
            scrape_time=now_with_tz(),
            tid=post.tid,
            author_id=post.user.user_id,
        )


class Comment(MixinBase, AiotiebaConvertible):
    """楼中楼数据模型。

    Attributes:
        cid: 楼中楼pid，存储为cid以区分，与create_time组成复合主键。
        create_time: 楼中楼创建时间，带时区信息，与cid组成复合主键。
        text: 楼中楼的纯文本内容。
        contents: 楼中楼的正文内容碎片列表，以JSONB格式存储。
        author_level: 作者在主题贴所在吧的等级。
        reply_to_id: 被回复者的user_id，可为空。
        scrape_time: 数据抓取时间。
        pid: 所属回复ID，外键关联到Post表。
        author_id: 作者user_id，外键关联到User表。
        post: 所属回复对象，与Post模型的关系。
        author: 作者用户对象，与User模型的关系。
    """

    __tablename__ = "comment"
    __table_args__ = _with_partition(
        Index("idx_comment_post_time", "pid", "create_time"),
        Index("idx_comment_author_time", "author_id", "create_time"),
    )

    cid: Mapped[int] = mapped_column(BIGINT, primary_key=True)
    create_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    text: Mapped[str] = mapped_column(Text)
    contents: Mapped[list[Fragment] | None] = mapped_column(JSONB, nullable=True)
    author_level: Mapped[int] = mapped_column(Integer)
    reply_to_id: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    scrape_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_with_tz)

    pid: Mapped[int] = mapped_column(BIGINT, index=True)
    author_id: Mapped[int] = mapped_column(BIGINT, index=True)

    post: Mapped[Post] = relationship(
        "Post",
        back_populates="comments",
        primaryjoin=lambda: foreign(Comment.pid) == Post.pid,
    )
    author: Mapped[User] = relationship(
        "User",
        back_populates="comments",
        primaryjoin=lambda: foreign(Comment.author_id) == User.user_id,
    )

    @classmethod
    def from_aiotieba(cls, comment: aiotieba.Comment | aiotieba_posts.Comment_p) -> Comment:
        """从aiotieba.Comment对象创建Comment模型实例。

        Args:
            comment: aiotieba返回的Comment对象。

        Returns:
            Comment: 转换后的Comment模型实例。
        """
        return cls(
            cid=comment.pid,
            create_time=datetime.fromtimestamp(comment.create_time, tz=SHANGHAI_TZ),
            text=comment.text,
            contents=cls.convert_content_list(comment.contents.objs),
            author_level=comment.user.level,
            reply_to_id=comment.reply_to_id or None,
            scrape_time=now_with_tz(),
            pid=comment.ppid,
            author_id=comment.user.user_id,
        )
