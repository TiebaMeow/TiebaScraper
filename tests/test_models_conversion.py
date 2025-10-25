import dataclasses
from datetime import UTC, datetime
from types import SimpleNamespace

from src.models.models import Comment, Post, Thread, User


def make_user(**overrides):
    defaults = {"user_id": 1, "level": 1, "portrait": "p", "user_name": "u", "nick_name": "n"}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


@dataclasses.dataclass
class _FragText:
    text: str


def test_user_from_aiotieba():
    u = make_user(user_id=7)
    model = User.from_aiotieba(u)  # type: ignore[arg-type]
    assert model.user_id == 7


def test_thread_post_comment_from_aiotieba_and_to_dict():
    u = make_user(user_id=1, level=3)
    now = int(datetime.now(tz=UTC).timestamp())
    contents = SimpleNamespace(text="Tx", objs=[_FragText(text="hello")])

    t = SimpleNamespace(
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

    p = SimpleNamespace(
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

    c = SimpleNamespace(
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
