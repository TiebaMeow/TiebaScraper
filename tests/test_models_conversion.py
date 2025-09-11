import dataclasses
from datetime import UTC, datetime

from src.models.models import Comment, Post, Thread, User


@dataclasses.dataclass
class _User:
    user_id: int
    level: int = 1
    portrait: str = "p"
    user_name: str = "u"
    nick_name: str = "n"


@dataclasses.dataclass
class _FragText:
    text: str


@dataclasses.dataclass
class _Contents:
    objs: list[object]


@dataclasses.dataclass
class _Thread:
    tid: int
    fid: int
    fname: str
    title: str
    text: str
    contents: _Contents
    last_time: int
    reply_num: int
    user: _User
    create_time: int


@dataclasses.dataclass
class _Post:
    pid: int
    tid: int
    fid: int
    fname: str
    text: str
    contents: _Contents
    floor: int
    reply_num: int
    user: _User
    create_time: int
    comments: list[object]


@dataclasses.dataclass
class _Comment:
    pid: int
    ppid: int
    tid: int
    fid: int
    fname: str
    text: str
    contents: _Contents
    user: _User
    create_time: int
    reply_to_id: int | None = None


def test_user_from_aiotieba():
    u = _User(user_id=7)
    model = User.from_aiotieba(u)  # type: ignore[arg-type]
    assert model.user_id == 7


def test_thread_post_comment_from_aiotieba_and_to_dict():
    u = _User(user_id=1, level=3)
    now = int(datetime.now(tz=UTC).timestamp())
    contents = _Contents(objs=[_FragText(text="hello")])

    t = _Thread(
        tid=10,
        fid=100,
        fname="bar",
        title="T",
        text="Tx",
        contents=contents,
        last_time=now,
        reply_num=2,
        user=u,
        create_time=now,
    )
    tm = Thread.from_aiotieba(t)  # type: ignore[arg-type]
    d = tm.to_dict()
    assert d["tid"] == 10
    assert d["fid"] == 100
    assert isinstance(d["contents"], list)

    p = _Post(
        pid=20,
        tid=10,
        fid=100,
        fname="bar",
        text="Px",
        contents=contents,
        floor=1,
        reply_num=1,
        user=u,
        create_time=now,
        comments=[],
    )
    pm = Post.from_aiotieba(p)  # type: ignore[arg-type]
    pd = pm.to_dict()
    assert pd["pid"] == 20
    assert pd["tid"] == 10
    assert isinstance(pd["contents"], list)

    c = _Comment(
        pid=30,
        ppid=20,
        tid=10,
        fid=100,
        fname="bar",
        text="Cx",
        contents=contents,
        user=u,
        create_time=now,
        reply_to_id=None,
    )
    cm = Comment.from_aiotieba(c)  # type: ignore[arg-type]
    cd = cm.to_dict()
    assert cd["cid"] == 30
    assert cd["pid"] == 20
