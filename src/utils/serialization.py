from dataclasses import fields, is_dataclass
from typing import Any


def _is_jsonable(obj: Any) -> bool:
    return is_dataclass(obj) or isinstance(obj, (list, tuple, set, frozenset, dict))


def _frag_type_name(obj: Any) -> str:
    name = type(obj).__name__
    if "Text" in name:
        return "text"
    if "Emoji" in name:
        return "emoji"
    if "At" in name and name.startswith("Frag"):
        return "at"
    if "Link" in name:
        return "link"
    if "TiebaPlus" in name:
        return "tieba_plus"
    if "Image" in name or "Img" in name:
        return "image"
    if "Video" in name:
        return "video"
    if "Voice" in name:
        return "voice"
    return "unknown"


def _serialize_fragment(obj: Any) -> dict[str, Any] | Any:
    data: dict[str, Any]
    if is_dataclass(obj):
        data = {f.name: to_jsonable(getattr(obj, f.name)) for f in fields(obj)}
    else:
        data = {k: to_jsonable(v) for k, v in vars(obj).items() if not callable(v) and not k.startswith("_")}
    data["type"] = _frag_type_name(obj)
    return data


def _serialize_contents(value: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}

    fragments = getattr(value, "objs", None)
    if fragments is not None:
        result["objs"] = [_serialize_fragment(f) for f in (fragments or [])]

    for key in ("texts", "emojis", "imgs", "ats", "links", "tiebapluses"):
        lst = getattr(value, key, None)
        if lst:
            result[key] = [_serialize_fragment(f) for f in lst]

    for key in ("video", "voice"):
        frag = getattr(value, key, None)
        if frag:
            result[key] = _serialize_fragment(frag)

    return result


def to_jsonable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if is_dataclass(obj):
        result: dict[str, Any] = {}
        for f in fields(obj):
            v = getattr(obj, f.name)
            if f.name == "contents":
                result[f.name] = _serialize_contents(v)
            else:
                result[f.name] = to_jsonable(v) if _is_jsonable(v) else v
        return result

    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            sk = str(k)
            if sk == "contents":
                out[sk] = _serialize_contents(v)
            else:
                out[sk] = to_jsonable(v) if _is_jsonable(v) else v
        return out

    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_jsonable(v) if _is_jsonable(v) else v for v in obj]

    try:
        return str(obj)
    except Exception:
        return repr(obj)
