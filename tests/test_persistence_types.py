"""Task 4.2 — unit + property tests for the persistence type adapters (Req 14.17, 14.18).

These exercise the three canonical encodings used to store richer Python values in
SQLite's narrow column set:

* **Decimal <-> TEXT** round-trips to the *same numeric value* and the stored form is
  canonical (numerically-equal Decimals encode identically) — the invariant the exact
  price-change rule and the minor-unit/decimal-string parity test depend on (Req 14.17).
* **bool <-> INTEGER** round-trips through the stored ``0`` / ``1`` integers.
* **datetime <-> TEXT** round-trips through ISO-8601, always normalised to UTC
  regardless of the source timezone (Req 14.18).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from restockradar.persistence.types import (
    bool_to_int,
    datetime_to_text,
    decimal_to_text,
    int_to_bool,
    text_to_datetime,
    text_to_decimal,
)

# ---------------------------------------------------------------------------
# Decimal <-> TEXT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected_text"),
    [
        (Decimal("12.99"), "12.99"),
        (Decimal("0"), "0"),
        (Decimal("-0"), "0"),
        (Decimal("0.00"), "0"),
        (Decimal("100"), "100"),  # must NOT render as "1E+2"
        (Decimal("100.00"), "100"),
        (Decimal("12.990"), "12.99"),  # trailing zero stripped -> canonical
        (Decimal("1.299E+1"), "12.99"),  # exponent noise collapsed
        (Decimal("-5.50"), "-5.5"),
        (Decimal("0.01"), "0.01"),
    ],
)
def test_decimal_to_text_is_canonical(value: Decimal, expected_text: str) -> None:
    """Numerically-equal Decimals encode to byte-identical ordinary-decimal TEXT."""
    assert decimal_to_text(value) == expected_text


def test_text_to_decimal_round_trips_value() -> None:
    assert text_to_decimal("12.99") == Decimal("12.99")


@pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_decimal_to_text_rejects_non_finite(bad: Decimal) -> None:
    with pytest.raises(ValueError, match="non-finite"):
        decimal_to_text(bad)


@given(
    st.decimals(
        allow_nan=False,
        allow_infinity=False,
        min_value=Decimal("-1000000"),
        max_value=Decimal("1000000"),
        places=4,
    )
)
def test_decimal_text_round_trip_preserves_value(value: Decimal) -> None:
    """Feature: stock-drop-monitor, Property: Decimal<->TEXT round-trips exactly.

    Encoding then decoding yields a Decimal numerically equal to the original.
    """
    assert text_to_decimal(decimal_to_text(value)) == value


@given(
    st.decimals(allow_nan=False, allow_infinity=False, places=2).filter(
        lambda d: abs(d) < Decimal("1e9")
    )
)
def test_decimal_text_encoding_is_canonical(value: Decimal) -> None:
    """Feature: stock-drop-monitor, Property: equal Decimals encode identically.

    Any two Decimals that compare equal (e.g. with differing trailing zeros or
    exponents) must produce the identical TEXT, so stored prices compare by value.
    """
    noisy = value * Decimal("1.000")  # same value, different internal representation
    assert decimal_to_text(value) == decimal_to_text(noisy)


# ---------------------------------------------------------------------------
# bool <-> INTEGER
# ---------------------------------------------------------------------------


def test_bool_encodes_to_zero_and_one() -> None:
    assert bool_to_int(True) == 1
    assert bool_to_int(False) == 0


def test_int_decodes_to_bool() -> None:
    assert int_to_bool(1) is True
    assert int_to_bool(0) is False


@pytest.mark.parametrize("bad", [-1, 2, 100])
def test_int_to_bool_rejects_non_canonical(bad: int) -> None:
    with pytest.raises(ValueError, match="0 or 1"):
        int_to_bool(bad)


@given(st.booleans())
def test_bool_int_round_trip(value: bool) -> None:
    """Feature: stock-drop-monitor, Property: bool<->INTEGER round-trips."""
    assert int_to_bool(bool_to_int(value)) is value


# ---------------------------------------------------------------------------
# datetime <-> TEXT
# ---------------------------------------------------------------------------


def test_datetime_to_text_is_utc_iso() -> None:
    dt = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert datetime_to_text(dt) == "2025-01-02T03:04:05+00:00"


def test_datetime_to_text_normalises_offset_to_utc() -> None:
    """A non-UTC aware datetime is converted to UTC before encoding (Req 14.20)."""
    eastern = timezone(timedelta(hours=-5))
    dt = datetime(2025, 1, 2, 3, 4, 5, tzinfo=eastern)  # 08:04:05 UTC
    assert datetime_to_text(dt) == "2025-01-02T08:04:05+00:00"


def test_datetime_to_text_rejects_naive() -> None:
    with pytest.raises(ValueError, match="naive"):
        datetime_to_text(datetime(2025, 1, 2, 3, 4, 5))  # intentionally naive


def test_text_to_datetime_rejects_naive_string() -> None:
    with pytest.raises(ValueError, match="not timezone-aware"):
        text_to_datetime("2025-01-02T03:04:05")


@given(
    st.datetimes(
        min_value=datetime(2000, 1, 1),
        max_value=datetime(2100, 1, 1),
        timezones=st.timezones(),
        # Spring-forward gaps produce local wall-times that do not exist in the
        # named zone; such imaginary values carry an offset zoneinfo would never
        # re-resolve to, so they are not valid instants to round-trip.
        allow_imaginary=False,
    )
)
def test_datetime_text_round_trip_preserves_instant(dt: datetime) -> None:
    """Feature: stock-drop-monitor, Property: datetime<->TEXT round-trips the instant.

    Any *unambiguous* aware datetime encodes and decodes back to the SAME absolute
    instant, always expressed in UTC. Fall-back transitions make some local
    wall-times ambiguous (they occur twice with different offsets); for those the
    offset the generated object happens to carry need not match what zoneinfo
    re-resolves, so we only assert on instants that resolve unambiguously.
    """
    # An ambiguous local time (a fall-back repeat) resolves to two different
    # offsets depending on ``fold``; skip those so the property only covers
    # instants that have a single, well-defined UTC answer.
    assume(dt.replace(fold=0).utcoffset() == dt.replace(fold=1).utcoffset())
    decoded = text_to_datetime(datetime_to_text(dt))
    assert decoded.tzinfo == UTC
    assert decoded == dt
