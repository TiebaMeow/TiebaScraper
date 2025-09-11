import dataclasses
from types import SimpleNamespace

from src.utils.serialization import to_jsonable


@dataclasses.dataclass
class FragText:
    text: str


@dataclasses.dataclass
class FragEmoji:
    id: str
    desc: str


def test_to_jsonable_with_dataclass_and_contents_list():
    contents = SimpleNamespace(objs=[FragText(text="hello"), FragEmoji(id="e1", desc="smile")])

    @dataclasses.dataclass
    class ThreadLike:
        tid: int
        contents: object

    obj = ThreadLike(tid=123, contents=contents)
    out = to_jsonable(obj)

    assert out["tid"] == 123
    assert isinstance(out["contents"], dict)
    assert len(out["contents"]["objs"]) == 2
    types = {f["type"] for f in out["contents"]["objs"]}
    assert types == {"text", "emoji"}


def test_to_jsonable_nested_collections_and_dict():
    data = {
        "a": [1, 2, {"b": {"c": 3}}],
        "contents": SimpleNamespace(objs=[FragText(text="x")]),
    }
    out = to_jsonable(data)
    assert out["a"][2]["b"]["c"] == 3
    assert out["contents"]["objs"][0]["type"] == "text"
