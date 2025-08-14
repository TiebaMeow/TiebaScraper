from dataclasses import fields, is_dataclass
from typing import Any

from ..models import AiotiebaConvertible


def _json_key(k: Any) -> str:
    try:
        return str(k)
    except Exception:
        return repr(k)


def _serialize_contents(value: Any, _seen: set[int]) -> list[dict]:
    try:
        if hasattr(value, "objs"):
            fragments = value.objs
        else:
            fragments = None
        if fragments is not None:
            converted = AiotiebaConvertible.convert_content_list(fragments)
            return [frag.model_dump() for frag in converted]
    except Exception:
        pass

    try:
        return [frag.model_dump() for frag in (value or [])]
    except Exception:
        pass

    try:
        return [to_jsonable(v, _seen) for v in value]
    except Exception:
        return []


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
            _inject_properties(obj, result, _seen)
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
                _inject_properties(obj, result, _seen)
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
