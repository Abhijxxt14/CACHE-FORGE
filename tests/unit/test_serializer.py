"""Unit tests for the value serializer."""
from __future__ import annotations

import pytest

from cacheforge.app.cache.serializer import deserialize, estimate_size, serialize


class TestSerializeDeserialize:
    """Round-trip tests for all supported types."""

    @pytest.mark.parametrize(
        "value",
        [
            "hello",
            "",
            "unicode 日本語",
            42,
            0,
            -7,
            3.14,
            -0.001,
            True,
            False,
            None,
            {"key": "value", "nested": {"a": 1}},
            [],
            [1, "two", 3.0, None, True],
            {"list": [1, 2], "nested": {"x": False}},
        ],
    )
    def test_round_trip(self, value):
        """Every supported value survives a serialize → deserialize round-trip."""
        raw = serialize(value)
        result = deserialize(raw)
        assert result == value
        assert type(result) is type(value)

    def test_bool_not_confused_with_int(self):
        """True must round-trip as bool, not int."""
        assert type(deserialize(serialize(True))) is bool
        assert type(deserialize(serialize(False))) is bool

    def test_int_not_confused_with_float(self):
        """Integer 1 must round-trip as int, not float."""
        result = deserialize(serialize(1))
        assert type(result) is int
        assert result == 1

    def test_none_round_trips(self):
        """None must round-trip as None."""
        assert deserialize(serialize(None)) is None

    def test_unsupported_type_raises(self):
        """Serializing an unsupported type must raise TypeError."""
        with pytest.raises(TypeError, match="Cannot serialize"):
            serialize(object())  # type: ignore[arg-type]

    def test_invalid_json_raises(self):
        """Deserializing invalid JSON must raise ValueError."""
        with pytest.raises(ValueError, match="Not valid JSON"):
            deserialize("not-json")

    def test_missing_envelope_fields_raises(self):
        """An envelope without 't' or 'v' raises ValueError."""
        import json

        with pytest.raises(ValueError, match="Invalid cache envelope"):
            deserialize(json.dumps({"only_t": "i"}))

    def test_unknown_tag_raises(self):
        """An unknown type tag raises ValueError."""
        import json

        with pytest.raises(ValueError, match="Unknown type tag"):
            deserialize(json.dumps({"t": "z", "v": 1}))

    def test_serialize_produces_string(self):
        """serialize always returns a str."""
        assert isinstance(serialize(42), str)
        assert isinstance(serialize({"a": 1}), str)


class TestEstimateSize:
    """Tests for the size estimator."""

    def test_returns_positive_int(self):
        assert estimate_size("my_key", "value") > 0

    def test_larger_value_is_larger(self):
        small = estimate_size("k", "x")
        large = estimate_size("k", "x" * 10_000)
        assert large > small

    def test_longer_key_is_larger(self):
        s = estimate_size("k", 42)
        l = estimate_size("k" * 100, 42)
        assert l > s

