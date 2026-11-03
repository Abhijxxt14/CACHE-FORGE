"""Cache value serializer.

Converts Python values to/from JSON strings with a type-tag envelope so that
values survive the round-trip without losing their original type.

Supported types: dict, list, str, int, float, bool, NoneType.

Wire format (JSON string):
    {"t": "<type_tag>", "v": <json_value>}

Type tags:
    "d"  -> dict
    "l"  -> list
    "s"  -> str
    "i"  -> int
    "f"  -> float
    "b"  -> bool
    "n"  -> None
"""
from __future__ import annotations

import json
from typing import Any

# Map Python type -> single-char tag
_TYPE_TAGS: dict[type, str] = {
    dict: "d",
    list: "l",
    str: "s",
    int: "i",
    float: "f",
    bool: "b",
    type(None): "n",
}

# Reverse map
_TAG_TYPES: dict[str, type] = {v: k for k, v in _TYPE_TAGS.items()}

CacheValue = dict | list | str | int | float | bool | None


def serialize(value: CacheValue) -> str:
    """Serialize a Python value to a JSON wire-format string.

    Args:
        value: The value to serialize. Must be one of the supported types.

    Returns:
        A JSON string with a ``{"t": ..., "v": ...}`` envelope.

    Raises:
        TypeError: If the value's type is not supported.
    """
    t = type(value)
    # bool is a subclass of int — must be tested first
    if isinstance(value, bool):
        tag = "b"
    elif t in _TYPE_TAGS:
        tag = _TYPE_TAGS[t]
    else:
        raise TypeError(
            f"Cannot serialize value of type {t.__name__!r}. "
            f"Supported: {list(_TYPE_TAGS.keys())}"
        )
    return json.dumps({"t": tag, "v": value}, ensure_ascii=False, separators=(",", ":"))


def deserialize(raw: str) -> CacheValue:
    """Deserialize a JSON wire-format string back to its original Python value.

    Args:
        raw: A string produced by :func:`serialize`.

    Returns:
        The original Python value with its original type.

    Raises:
        ValueError: If *raw* is not a valid envelope or the tag is unknown.
    """
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Not valid JSON: {raw!r}") from exc

    if not isinstance(envelope, dict) or "t" not in envelope or "v" not in envelope:
        raise ValueError(f"Invalid cache envelope: {raw!r}")

    tag: str = envelope["t"]
    val: Any = envelope["v"]

    if tag == "n":
        return None
    if tag == "b":
        # json.loads already returns bool for true/false
        return bool(val)
    if tag == "i":
        return int(val)
    if tag == "f":
        return float(val)
    if tag == "s":
        return str(val)
    if tag == "d":
        if not isinstance(val, dict):
            raise ValueError(f"Expected dict for tag 'd', got {type(val).__name__}")
        return val
    if tag == "l":
        if not isinstance(val, list):
            raise ValueError(f"Expected list for tag 'l', got {type(val).__name__}")
        return val

    raise ValueError(f"Unknown type tag: {tag!r}")


def estimate_size(key: str, value: CacheValue) -> int:
    """Estimate the memory footprint of a cache entry in bytes.

    Uses the length of the serialized JSON payload plus a fixed overhead per
    entry to account for Python object headers and dict overhead.

    Args:
        key: The raw cache key string.
        value: The cache value.

    Returns:
        Estimated byte count.
    """
    _OVERHEAD_BYTES = 256  # conservative per-entry overhead
    try:
        payload = serialize(value)
    except TypeError:
        payload = str(value)
    return len(key.encode()) + len(payload.encode()) + _OVERHEAD_BYTES

