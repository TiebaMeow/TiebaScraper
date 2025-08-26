from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, cast

import aiotieba.api._classdef as bmod
import aiotieba.api.get_comments._classdef as cmod
import aiotieba.api.get_posts._classdef as pmod
import aiotieba.api.get_threads._classdef as tmod

if TYPE_CHECKING:
    from aiotieba.typing import Comment, Post, Thread

__all__ = [
    "deserialize_thread",
    "deserialize_post",
    "deserialize_comment",
    "deserialize",
]

_FRAG_NAME_MAP: dict[str, str] = {
    "text": "FragText",
    "at": "FragAt",
    "emoji": "FragEmoji",
    "image": "FragImage",
    "item": "FragItem",
    "link": "FragLink",
    "tieba_plus": "FragTiebaPlus",
    "video": "FragVideo",
    "voice": "FragVoice",
    "unknown": "FragUnknown",
}


def _pick_dataclass_fields(cls: type, data: Mapping[str, Any]) -> dict[str, Any]:
    if not is_dataclass(cls):
        return dict(data)
    allowed = {f.name for f in fields(cls)}
    return {k: v for k, v in data.items() if k in allowed}


def _build_user(scope: Literal["t", "p", "c"], data: Mapping[str, Any]):
    cls = {"t": tmod.UserInfo_t, "p": pmod.UserInfo_p, "c": cmod.UserInfo_c}[scope]
    kwargs = _pick_dataclass_fields(cls, data)
    try:
        return cls(**kwargs)
    except Exception:
        return SimpleNamespace(**dict(data))


def _frag_class(scope: Literal["t", "p", "c", "st", "pc"], frag_type: str):
    base = _FRAG_NAME_MAP.get(frag_type.lower())
    if base is None:
        base = "FragUnknown"
    suffix = f"_{scope}"
    name = f"{base}{suffix}"
    mod = {"t": tmod, "p": pmod, "c": cmod, "st": tmod, "pc": pmod}[scope]
    return getattr(mod, name, None)


def _build_fragment(scope: Literal["t", "p", "c", "st", "pc"], frag: Mapping[str, Any]) -> Any:
    ftype = str(frag.get("type", "text")).lower()
    cls = _frag_class(scope, ftype)
    if cls is None:
        return dict(frag)
    kwargs = _pick_dataclass_fields(cls, frag)
    try:
        return cls(**kwargs)
    except Exception:
        return dict(frag)


def _build_contents(scope: Literal["t", "p", "c", "st", "pc"], contents: Any):
    cls_map = {
        "t": tmod.Contents_t,
        "p": pmod.Contents_p,
        "c": cmod.Contents_c,
        "st": tmod.Contents_st,
        "pc": pmod.Contents_pc,
    }
    cls = cls_map[scope]

    if isinstance(contents, Mapping):
        kw: dict[str, Any] = {}
        # objs
        objs_in = cast("list[Any]", contents.get("objs", []) or [])
        objs: list[Any] = []
        for frag in objs_in:
            if isinstance(frag, Mapping):
                objs.append(_build_fragment(scope, frag))
            else:
                objs.append(frag)
        if objs:
            kw["objs"] = objs

        for key in ("texts", "emojis", "imgs", "ats", "links", "tiebapluses"):
            arr = contents.get(key)
            if isinstance(arr, list) and arr:
                kw[key] = [_build_fragment(scope, frag) if isinstance(frag, Mapping) else frag for frag in arr]

        for key in ("video", "voice"):
            frag = contents.get(key)
            if isinstance(frag, Mapping) and frag:
                kw[key] = _build_fragment(scope, frag)

        try:
            return cls(**_pick_dataclass_fields(cls, kw) | kw)
        except Exception:
            try:
                return cls(objs=kw.get("objs", []))
            except Exception:
                return SimpleNamespace(**kw)

    objs: list[Any] = []
    try:
        for frag in cast("list[Any]", contents or []):
            if isinstance(frag, Mapping):
                objs.append(_build_fragment(scope, frag))
            else:
                objs.append(frag)
    except Exception:
        objs = []

    try:
        return cls(objs=objs)
    except Exception:
        return SimpleNamespace(objs=objs)


def _build_thread_like(
    scope: Literal["t", "p", "c", "st", "pc"],
    target_cls: type | None,
    data: Mapping[str, Any],
    *,
    contents_scope: Literal["t", "p", "c", "st", "pc"] | None = None,
):
    user_raw = cast("Mapping[str, Any] | None", data.get("user"))
    user_built = (
        _build_user(
            cast("Literal['t','p','c']", scope if scope in {"t", "p", "c"} else "p"),
            user_raw or {},
        )
        if user_raw is not None
        else None
    )
    cscope = contents_scope or scope
    contents_built = _build_contents(cscope, data.get("contents"))

    kwargs = dict(data)
    kwargs["contents"] = contents_built
    if user_built is not None:
        kwargs["user"] = user_built
    if target_cls is not None:
        kwargs = _pick_dataclass_fields(target_cls, kwargs)
        try:
            return target_cls(**kwargs)
        except Exception:
            pass

    ns = SimpleNamespace(**dict(data))
    ns.contents = contents_built
    if user_built is not None:
        ns.user = user_built
    return ns


def _build_vote_info(data: Mapping[str, Any] | None):
    if not isinstance(data, Mapping):
        return None
    cls = bmod.VoteInfo
    kwargs = _pick_dataclass_fields(cls, data)
    try:
        return cls(**kwargs)
    except Exception:
        return SimpleNamespace(**dict(data))


def _build_share_thread(data: Mapping[str, Any] | None):
    if not isinstance(data, Mapping):
        return None
    cls = getattr(tmod, "ShareThread", None)
    if cls is None:
        return SimpleNamespace(**dict(data))
    kwargs = dict(data)
    kwargs["contents"] = _build_contents("st", data.get("contents"))
    if isinstance(data.get("vote_info"), Mapping):
        kwargs["vote_info"] = _build_vote_info(cast("Mapping[str, Any]", data["vote_info"]))
    kwargs = _pick_dataclass_fields(cls, kwargs)
    try:
        return cls(**kwargs)
    except Exception:
        ns = SimpleNamespace(**dict(data))
        ns.contents = _build_contents("st", data.get("contents"))
        return ns


def deserialize_thread(data: Mapping[str, Any]) -> Thread:
    """将 JSON/dict 反序列化为 aiotieba Thread 对象。"""
    target = getattr(tmod, "Thread", None)
    enriched = dict(data)
    if isinstance(data.get("vote_info"), Mapping):
        enriched["vote_info"] = _build_vote_info(cast("Mapping[str, Any]", data["vote_info"]))
    if isinstance(data.get("share_origin"), Mapping):
        enriched["share_origin"] = _build_share_thread(cast("Mapping[str, Any]", data["share_origin"]))
    return cast("Thread", _build_thread_like("t", target, enriched))


def deserialize_post(data: Mapping[str, Any]) -> Post:
    """将 JSON/dict 反序列化为 aiotieba Post 对象。"""
    target = getattr(pmod, "Post", None)
    enriched = dict(data)
    comments_raw = cast("list[Any] | None", data.get("comments"))
    if isinstance(comments_raw, list):
        built: list[Any] = []
        for c in comments_raw:
            if isinstance(c, Mapping):
                built.append(
                    _build_thread_like(
                        "p",
                        getattr(pmod, "Comment_p", None),
                        c,
                        contents_scope="pc",
                    )
                )
            else:
                built.append(c)
        enriched["comments"] = built
    return cast("Post", _build_thread_like("p", target, enriched))


def deserialize_comment(data: Mapping[str, Any]) -> Comment:
    """将 JSON/dict 反序列化为 aiotieba Comment 对象。"""
    target = getattr(cmod, "Comment", None)
    return cast("Comment", _build_thread_like("c", target, data))


def deserialize(item_type: Literal["thread", "post", "comment"], data: Mapping[str, Any]) -> Thread | Post | Comment:
    """根据类型进行通用反序列化。"""
    if item_type == "thread":
        return deserialize_thread(data)
    if item_type == "post":
        return deserialize_post(data)
    if item_type == "comment":
        return deserialize_comment(data)
    raise ValueError(f"Unsupported item_type: {item_type}")
