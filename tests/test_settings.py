"""Task 2.1 — settings defaults and ``RR_*`` env overrides (Req 20.7).

Confirms that :meth:`Settings.from_env` yields the typed defaults fixed by the
design when nothing is set, and that every ``RR_``-prefixed variable overrides its
default. We always pass an explicit ``env`` mapping with ``load_dotenv_file=False``
so the tests are hermetic — they never read the real process environment or a
``.env`` file on disk.
"""

from __future__ import annotations

import pytest

from restockradar.settings import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_BUSY_TIMEOUT_MS,
    DEFAULT_DB_PATH,
    DEFAULT_DELIVERY_LEASE_S,
    DEFAULT_DELIVERY_REQUEST_BUDGET_S,
    DEFAULT_DISPATCH_CONCURRENCY,
    DEFAULT_ONCE_DRAIN_TIMEOUT_S,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_TICK_S,
    DEFAULT_WATCHDOG_DISPATCH_S,
    DEFAULT_WATCHDOG_SCHEDULER_S,
    DEFAULT_WATCHDOG_STARTUP_GRACE_S,
    Settings,
)


def _from_empty_env() -> Settings:
    """Build settings from an empty environment (pure defaults)."""
    return Settings.from_env({}, load_dotenv_file=False)


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


def test_defaults_match_the_design() -> None:
    """With no env vars set, every field takes its design-mandated default."""
    settings = _from_empty_env()

    assert settings.db_path == DEFAULT_DB_PATH
    assert settings.config_path == DEFAULT_CONFIG_PATH == "config/config.yaml"
    assert settings.db_busy_timeout_ms == DEFAULT_DB_BUSY_TIMEOUT_MS
    assert settings.retention_days == DEFAULT_RETENTION_DAYS == 14
    assert settings.tick_s == DEFAULT_TICK_S == 5.0
    assert settings.delivery_lease_s == DEFAULT_DELIVERY_LEASE_S == 60.0
    assert settings.delivery_request_budget_s == DEFAULT_DELIVERY_REQUEST_BUDGET_S == 45.0
    assert settings.dispatch_concurrency == DEFAULT_DISPATCH_CONCURRENCY == 10
    assert settings.once_drain_timeout_s == DEFAULT_ONCE_DRAIN_TIMEOUT_S == 120.0
    assert settings.watchdog_scheduler_s == DEFAULT_WATCHDOG_SCHEDULER_S == 120.0
    assert settings.watchdog_dispatch_s == DEFAULT_WATCHDOG_DISPATCH_S == 60.0
    assert settings.watchdog_startup_grace_s == DEFAULT_WATCHDOG_STARTUP_GRACE_S == 120.0
    assert settings.dry_run is False
    assert settings.once is False


def test_settings_is_frozen() -> None:
    """The snapshot is immutable so it can be shared across threads safely."""
    settings = _from_empty_env()
    with pytest.raises((AttributeError, TypeError)):
        settings.tick_s = 1.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Env overrides
# ---------------------------------------------------------------------------


def test_rr_env_vars_override_every_default() -> None:
    """Each ``RR_*`` variable overrides its corresponding default."""
    env = {
        "RR_DB_PATH": "/tmp/custom.db",
        "RR_CONFIG_PATH": "/tmp/custom.yaml",
        "RR_DB_BUSY_TIMEOUT_MS": "9000",
        "RR_RETENTION_DAYS": "30",
        "RR_TICK_S": "2.5",
        "RR_DELIVERY_LEASE_S": "90",
        "RR_DELIVERY_REQUEST_BUDGET_S": "30",
        "RR_DISPATCH_CONCURRENCY": "25",
        "RR_ONCE_DRAIN_TIMEOUT_S": "300",
        "RR_WATCHDOG_SCHEDULER_S": "200",
        "RR_WATCHDOG_DISPATCH_S": "45",
        "RR_WATCHDOG_STARTUP_GRACE_S": "60",
        "RR_DRY_RUN": "true",
        "RR_ONCE": "yes",
    }

    settings = Settings.from_env(env, load_dotenv_file=False)

    assert settings.db_path == "/tmp/custom.db"
    assert settings.config_path == "/tmp/custom.yaml"
    assert settings.db_busy_timeout_ms == 9000
    assert settings.retention_days == 30
    assert settings.tick_s == 2.5
    assert settings.delivery_lease_s == 90.0
    assert settings.delivery_request_budget_s == 30.0
    assert settings.dispatch_concurrency == 25
    assert settings.once_drain_timeout_s == 300.0
    assert settings.watchdog_scheduler_s == 200.0
    assert settings.watchdog_dispatch_s == 45.0
    assert settings.watchdog_startup_grace_s == 60.0
    assert settings.dry_run is True
    assert settings.once is True


def test_partial_override_leaves_other_defaults_intact() -> None:
    """Setting one variable overrides only that field; the rest stay default."""
    settings = Settings.from_env({"RR_TICK_S": "1"}, load_dotenv_file=False)

    assert settings.tick_s == 1.0
    # Everything else untouched.
    assert settings.dispatch_concurrency == DEFAULT_DISPATCH_CONCURRENCY
    assert settings.retention_days == DEFAULT_RETENTION_DAYS
    assert settings.dry_run is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("Yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("No", False),
        ("off", False),
        ("", False),
    ],
)
def test_boolean_flag_tokens(raw: str, expected: bool) -> None:
    """Boolean flags accept the usual truthy/falsey spellings, case-insensitively."""
    settings = Settings.from_env({"RR_DRY_RUN": raw}, load_dotenv_file=False)
    assert settings.dry_run is expected


# ---------------------------------------------------------------------------
# Invalid values fail fast
# ---------------------------------------------------------------------------


def test_invalid_integer_raises() -> None:
    """A non-integer value for an int setting is rejected at parse time."""
    with pytest.raises(ValueError, match="RR_DISPATCH_CONCURRENCY"):
        Settings.from_env({"RR_DISPATCH_CONCURRENCY": "lots"}, load_dotenv_file=False)


def test_invalid_float_raises() -> None:
    """A non-numeric value for a float setting is rejected at parse time."""
    with pytest.raises(ValueError, match="RR_TICK_S"):
        Settings.from_env({"RR_TICK_S": "soon"}, load_dotenv_file=False)


def test_invalid_boolean_raises() -> None:
    """An unrecognised boolean token is rejected rather than silently ignored."""
    with pytest.raises(ValueError, match="RR_ONCE"):
        Settings.from_env({"RR_ONCE": "maybe"}, load_dotenv_file=False)


def test_out_of_range_value_raises() -> None:
    """A below-minimum value (concurrency < 1) is rejected."""
    with pytest.raises(ValueError, match="RR_DISPATCH_CONCURRENCY"):
        Settings.from_env({"RR_DISPATCH_CONCURRENCY": "0"}, load_dotenv_file=False)
