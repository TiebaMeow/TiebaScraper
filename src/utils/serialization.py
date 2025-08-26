from dataclasses import fields, is_dataclass
from typing import Any


def _json_key(k: Any) -> str:
    try:
        return str(k)
    except Exception:
        return repr(k)


def _frag_type_name(obj: Any) -> str:
    try:
        name = type(obj).__name__
    except Exception:
        return "unknown"
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


def _serialize_fragment(obj: Any, _seen: set[int]) -> dict[str, Any] | Any:
    try:
        data: dict[str, Any]
        if is_dataclass(obj):
            data = {f.name: to_jsonable(getattr(obj, f.name), _seen) for f in fields(obj)}
        else:
            data = {}
            for k, v in vars(obj).items():
                if not callable(v) and not k.startswith("_"):
                    data[k] = to_jsonable(v, _seen)
        data["type"] = _frag_type_name(obj)
        return data
    except Exception:
        try:
            return {"type": _frag_type_name(obj), "value": str(obj)}
        except Exception:
            return str(obj)


def _serialize_contents(value: Any, _seen: set[int]) -> dict[str, Any]:
    result: dict[str, Any] = {}

    try:
        if hasattr(value, "objs"):
            fragments = value.objs
        else:
            fragments = None
        if fragments is not None:
            result["objs"] = [_serialize_fragment(f, _seen) for f in (fragments or [])]
    except Exception:
        result["objs"] = []

    for key in ("texts", "emojis", "imgs", "ats", "links", "tiebapluses"):
        try:
            lst = getattr(value, key)
        except Exception:
            lst = None
        if lst:
            result[key] = [_serialize_fragment(f, _seen) for f in lst]

    for key in ("video", "voice"):
        try:
            frag = getattr(value, key)
        except Exception:
            frag = None
        try:
            if frag:
                result[key] = _serialize_fragment(frag, _seen)
        except Exception:
            pass

    if not result:
        try:
            result = {"objs": [to_jsonable(v, _seen) for v in (value or [])]}
        except Exception:
            result = {"objs": []}
    return result


def _inject_properties(obj: Any, out: dict[str, Any], _seen: set[int]) -> None:
    try:
        for cls in type(obj).__mro__:
            for name, descriptor in cls.__dict__.items():
                is_cached_prop = False
                try:
                    is_cached_prop = descriptor.__class__.__name__ == "cached_property"
                except Exception:
                    is_cached_prop = False
                if not (isinstance(descriptor, property) or is_cached_prop):
                    continue
                if name in out or name.startswith("_"):
                    continue
                try:
                    val = getattr(obj, name)
                except Exception:
                    continue
                if callable(val):
                    continue
                try:
                    if name == "contents":
                        out[name] = _serialize_contents(val, _seen)
                    else:
                        out[name] = to_jsonable(val, _seen)
                except Exception:
                    continue
    except Exception:
        return


def to_jsonable(obj: Any, _seen: set[int] | None = None) -> Any:
    if _seen is None:
        _seen = set()
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    oid = id(obj)
    if oid in _seen:
        return "<recursion>"
    _seen.add(oid)

    if is_dataclass(obj):
        try:
            result: dict[str, Any] = {}
            for f in fields(obj):
                v = getattr(obj, f.name)
                if f.name == "contents":
                    result[f.name] = _serialize_contents(v, _seen)
                else:
                    result[f.name] = to_jsonable(v, _seen)
            # _inject_properties(obj, result, _seen)
            return result
        except Exception:
            try:
                result: dict[str, Any] = {}
                for k, v in vars(obj).items():
                    if callable(v):
                        continue
                    if k == "contents":
                        result[k] = _serialize_contents(v, _seen)
                    else:
                        result[k] = to_jsonable(v, _seen)
                # _inject_properties(obj, result, _seen)
                return result
            except Exception:
                return str(obj)

    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            sk = _json_key(k)
            if sk == "contents":
                out[sk] = _serialize_contents(v, _seen)
            else:
                out[sk] = to_jsonable(v, _seen)
        return out

    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_jsonable(v, _seen) for v in obj]

    try:
        return str(obj)
    except Exception:
        return repr(obj)
