"""Type adapters: Decimal<->TEXT (canonical Decimal string), bool<->INTEGER (0/1),
and datetime<->ISO-8601 UTC (Req 14.17, 14.20, 14.18).

SQLite stores a tiny set of column kinds, so every richer Python value the schema
needs is funnelled through one of the three canonical encodings below:

* **Decimal <-> TEXT** — monetary prices are stored as a *canonical* Decimal string
  (e.g. ``"12.99"``) rather than a float, so equality is exact and the ``.js``
  minor-unit path and the feed decimal-string path round-trip to the identical
  stored form (Req 14.17; underpins the Req 21.21 parity test). "Canonical" means
  the string is normalised so that two Decimals that compare equal always encode to
  byte-identical TEXT — trailing-zero and exponent noise (``Decimal("12.990")`` vs
  ``Decimal("1.299E+1")``) collapses to one representation.
* **bool <-> INTEGER** — booleans are the SQLite integers ``0`` / ``1`` (Req schema
  convention), never Python ``True``/``False`` written through sqlite3's implicit
  int coercion, so the stored form is explicit and stable.
* **datetime <-> TEXT** — timestamps are ISO-8601 strings normalised to UTC
  (e.g. ``"2025-01-02T03:04:05.000000+00:00"``), so every stored instant is
  unambiguous and comparable as text (Req 14.18, 14.20).

The functions are deliberately plain conversions (no sqlite3 global registration):
the single-writer executor owns the one connection and calls these explicitly at
the repository boundary, which keeps the encoding visible at each call site and
avoids process-global ``sqlite3.register_adapter`` state leaking across the shared
database file.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

# ---------------------------------------------------------------------------
# Decimal <-> TEXT  (Req 14.17 — monetary prices stored as canonical Decimal text)
# ---------------------------------------------------------------------------


def decimal_to_text(value: Decimal) -> str:
    """Encode a :class:`~decimal.Decimal` as its canonical TEXT form.

    The result is normalised so that any two Decimals that are numerically equal
    produce byte-identical strings — the property the exact price-change rule and
    the minor-unit/decimal-string parity test rely on.

    * A non-finite Decimal (NaN / Infinity) has no canonical price meaning and
      raises :class:`ValueError`.
    * ``Decimal.normalize`` strips trailing-zero/exponent noise, but it renders a
      whole number in scientific notation (``Decimal("100").normalize()`` ->
      ``"1E+2"``); we expand any such positive exponent back to plain digits so the
      stored form is always ordinary decimal text (``"100"``), never ``"1E+2"``.
    * Negative zero is folded to ``"0"``.
    """
    if not value.is_finite():
        raise ValueError(f"cannot store a non-finite Decimal as TEXT: {value!r}")

    normalised = value.normalize()
    # Fold -0 to 0 so the canonical form never carries a stray sign.
    if normalised == 0:
        return "0"

    _sign, _digits, exponent = normalised.as_tuple()
    # normalize() can yield a positive exponent (e.g. 1E+2) for trailing-zero whole
    # numbers; re-materialise those as plain integer text instead of sci-notation.
    if isinstance(exponent, int) and exponent > 0:
        normalised = normalised.quantize(Decimal(1))
    return str(normalised)


def text_to_decimal(text: str) -> Decimal:
    """Decode a canonical TEXT price back into a :class:`~decimal.Decimal`."""
    return Decimal(text)


# ---------------------------------------------------------------------------
# bool <-> INTEGER  (0 / 1)
# ---------------------------------------------------------------------------


def bool_to_int(value: bool) -> int:
    """Encode a boolean as the SQLite integer ``1`` (true) or ``0`` (false)."""
    return 1 if value else 0


def int_to_bool(value: int) -> bool:
    """Decode a stored ``0`` / ``1`` integer back into a boolean.

    Only the two canonical values are accepted; any other integer indicates a
    corrupt/foreign encoding and raises :class:`ValueError` rather than silently
    coercing (``2`` is not ``True``).
    """
    if value == 0:
        return False
    if value == 1:
        return True
    raise ValueError(f"expected a stored boolean 0 or 1, got {value!r}")


# ---------------------------------------------------------------------------
# datetime <-> TEXT  (ISO-8601, normalised to UTC — Req 14.18, 14.20)
# ---------------------------------------------------------------------------


def datetime_to_text(value: datetime) -> str:
    """Encode a timezone-aware :class:`~datetime.datetime` as ISO-8601 UTC text.

    * A naive datetime (no ``tzinfo``) is ambiguous and raises :class:`ValueError`;
      callers must attach a timezone before storing.
    * Any aware datetime is converted to UTC first, so the stored string always
      carries the ``+00:00`` offset (Req 14.20) regardless of the source zone.
    """
    if value.tzinfo is None:
        raise ValueError("cannot store a naive datetime; a timezone is required")
    return value.astimezone(UTC).isoformat()


def text_to_datetime(text: str) -> datetime:
    """Decode an ISO-8601 timestamp back into a UTC-normalised aware datetime.

    The value is parsed with :meth:`datetime.fromisoformat` and converted to UTC.
    A stored timestamp that somehow lacks an offset is rejected rather than guessed,
    keeping the "all timestamps are UTC" invariant explicit (Req 14.20).
    """
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError(f"stored timestamp is not timezone-aware: {text!r}")
    return parsed.astimezone(UTC)
