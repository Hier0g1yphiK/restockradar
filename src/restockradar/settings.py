"""Process-level settings loaded from the environment: DB path, busy_timeout,
retention, operational modes, ``tick_s``, lease/budget/concurrency, drain timeout,
and watchdog thresholds + startup grace (Req 13.8, 18.1, 19.9, 20.7).

The whole process is configured from the environment (optionally seeded from a
``.env`` file via ``python-dotenv``). Every tunable is read from an ``RR_``-prefixed
variable; where a variable is unset we fall back to the typed default below, which
matches the values fixed by the design's "Decisions not dictated" list and the
requirements it cites:

- ``tick_s`` 5s (Req 1.5), retention 14 days (Req 14.22),
- delivery lease 60s / request budget 45s / dispatch concurrency 10 (Req 10.3-10.7),
- once-mode drain timeout 120s (Req 20.7),
- watchdog scheduler 120s / dispatch 60s / startup grace 120s (Req 19.6, 19.10).

Secrets never live here — only the DB path and operational tunables (Req 18.1); a
Destination's webhook URL is resolved later from its own named env var (Req 18.2).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from dotenv import load_dotenv

__all__ = ["Settings"]


# ---------------------------------------------------------------------------
# Defaults (single source of truth, matched by the unit tests)
# ---------------------------------------------------------------------------

#: Where the SQLite database file lives when ``RR_DB_PATH`` is unset.
DEFAULT_DB_PATH = "data/restockradar.db"
#: Where the YAML configuration file lives when ``RR_CONFIG_PATH`` is unset. The
#: Config_Loader reads this on startup; secrets are never in it (Req 13.1, 18.2).
DEFAULT_CONFIG_PATH = "config/config.yaml"
#: SQLite ``busy_timeout`` in milliseconds, so the shared bot process can wait for
#: the writer lock rather than failing immediately (Req 14.19).
DEFAULT_DB_BUSY_TIMEOUT_MS = 5000
#: Retention sweep horizon in days for terminal outbox/deliveries/history rows
#: (Req 14.22); seen_products / alert_state / admin_scope_state are never pruned.
DEFAULT_RETENTION_DAYS = 14
#: Fixed fine-grained scheduler tick in seconds (Req 1.5).
DEFAULT_TICK_S = 5.0
#: ``sending`` lease duration in seconds (``RR_DELIVERY_LEASE_S``; Req 10.4).
DEFAULT_DELIVERY_LEASE_S = 60.0
#: Per-delivery total in-request time budget in seconds (Req 10.7).
DEFAULT_DELIVERY_REQUEST_BUDGET_S = 45.0
#: Global upper bound on concurrent destination deliveries (Req 10.3).
DEFAULT_DISPATCH_CONCURRENCY = 10
#: Upper bound in seconds on how long ``--once`` drains the outbox (Req 20.7).
DEFAULT_ONCE_DRAIN_TIMEOUT_S = 120.0
#: Scheduler-liveness staleness threshold in seconds (Req 19.6, 19.10).
DEFAULT_WATCHDOG_SCHEDULER_S = 120.0
#: Dispatch-liveness staleness threshold in seconds (Req 19.6, 19.10).
DEFAULT_WATCHDOG_DISPATCH_S = 60.0
#: Startup grace in seconds during which heartbeats are treated as fresh (Req 19.10).
DEFAULT_WATCHDOG_STARTUP_GRACE_S = 120.0


# ---------------------------------------------------------------------------
# Typed env parsers
# ---------------------------------------------------------------------------

#: Values that read as "true" for a boolean flag, compared case-insensitively.
_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
#: Values that read as "false" for a boolean flag, compared case-insensitively.
_FALSE_TOKENS = frozenset({"0", "false", "no", "off", ""})


def _env_str(env: Mapping[str, str], name: str, default: str) -> str:
    """Return ``env[name]`` or ``default`` when unset."""
    return env.get(name, default)


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    """Parse an ``RR_`` boolean flag, accepting the usual truthy/falsey tokens."""
    raw = env.get(name)
    if raw is None:
        return default
    token = raw.strip().casefold()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ValueError(
        f"{name}={raw!r} is not a valid boolean "
        f"(use one of true/false/yes/no/on/off/1/0)"
    )


def _env_int(env: Mapping[str, str], name: str, default: int, *, minimum: int) -> int:
    """Parse a non-negative-bounded integer ``RR_`` setting."""
    raw = env.get(name)
    if raw is None:
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r} is not a valid integer") from exc
    if value < minimum:
        raise ValueError(f"{name}={value} must be >= {minimum}")
    return value


def _env_float(env: Mapping[str, str], name: str, default: float, *, minimum: float) -> float:
    """Parse a bounded float ``RR_`` setting (seconds)."""
    raw = env.get(name)
    if raw is None:
        return default
    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r} is not a valid number") from exc
    if value < minimum:
        raise ValueError(f"{name}={value} must be >= {minimum}")
    return value


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable, fully-typed snapshot of the process-level configuration.

    Build it with :meth:`from_env`, which reads the ``RR_``-prefixed environment
    variables and fills in the typed defaults for any that are unset. Being frozen,
    an instance is safe to share across the event loop, the executor thread, and the
    watchdog daemon thread without risk of mutation.
    """

    #: Filesystem path to the SQLite database file (``RR_DB_PATH``).
    db_path: str = DEFAULT_DB_PATH
    #: Filesystem path to the YAML configuration file (``RR_CONFIG_PATH``).
    config_path: str = DEFAULT_CONFIG_PATH
    #: SQLite ``busy_timeout`` in milliseconds (``RR_DB_BUSY_TIMEOUT_MS``).
    db_busy_timeout_ms: int = DEFAULT_DB_BUSY_TIMEOUT_MS
    #: Retention horizon in days for terminal rows (``RR_RETENTION_DAYS``).
    retention_days: int = DEFAULT_RETENTION_DAYS
    #: Scheduler tick interval in seconds (``RR_TICK_S``).
    tick_s: float = DEFAULT_TICK_S
    #: ``sending`` lease duration in seconds (``RR_DELIVERY_LEASE_S``).
    delivery_lease_s: float = DEFAULT_DELIVERY_LEASE_S
    #: Per-delivery in-request time budget in seconds (``RR_DELIVERY_REQUEST_BUDGET_S``).
    delivery_request_budget_s: float = DEFAULT_DELIVERY_REQUEST_BUDGET_S
    #: Max concurrent destination deliveries (``RR_DISPATCH_CONCURRENCY``).
    dispatch_concurrency: int = DEFAULT_DISPATCH_CONCURRENCY
    #: ``--once`` drain timeout in seconds (``RR_ONCE_DRAIN_TIMEOUT_S``).
    once_drain_timeout_s: float = DEFAULT_ONCE_DRAIN_TIMEOUT_S
    #: Scheduler-liveness watchdog threshold in seconds (``RR_WATCHDOG_SCHEDULER_S``).
    watchdog_scheduler_s: float = DEFAULT_WATCHDOG_SCHEDULER_S
    #: Dispatch-liveness watchdog threshold in seconds (``RR_WATCHDOG_DISPATCH_S``).
    watchdog_dispatch_s: float = DEFAULT_WATCHDOG_DISPATCH_S
    #: Watchdog startup grace in seconds (``RR_WATCHDOG_STARTUP_GRACE_S``).
    watchdog_startup_grace_s: float = DEFAULT_WATCHDOG_STARTUP_GRACE_S
    #: Dry-run mode: render alerts to the log, issue no webhook POST (``RR_DRY_RUN``).
    dry_run: bool = False
    #: Once mode: run one cycle, drain the outbox, exit (``RR_ONCE``).
    once: bool = False

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        load_dotenv_file: bool = True,
    ) -> Settings:
        """Build :class:`Settings` from the environment.

        When ``env`` is omitted the live process environment (``os.environ``) is
        read, after loading a ``.env`` file if present (``python-dotenv``); any
        ``RR_`` variable already set in the real environment wins over the file.
        Pass an explicit ``env`` mapping (and ``load_dotenv_file=False``) to parse a
        fixed set of variables deterministically — the unit tests rely on this.

        Raises:
            ValueError: if any provided ``RR_`` variable fails to parse or violates
                its bound (so a typo surfaces at startup, not silently).
        """
        if env is None:
            if load_dotenv_file:
                load_dotenv()
            env = os.environ

        return cls(
            db_path=_env_str(env, "RR_DB_PATH", DEFAULT_DB_PATH),
            config_path=_env_str(env, "RR_CONFIG_PATH", DEFAULT_CONFIG_PATH),
            db_busy_timeout_ms=_env_int(
                env, "RR_DB_BUSY_TIMEOUT_MS", DEFAULT_DB_BUSY_TIMEOUT_MS, minimum=0
            ),
            retention_days=_env_int(
                env, "RR_RETENTION_DAYS", DEFAULT_RETENTION_DAYS, minimum=0
            ),
            tick_s=_env_float(env, "RR_TICK_S", DEFAULT_TICK_S, minimum=0.0),
            delivery_lease_s=_env_float(
                env, "RR_DELIVERY_LEASE_S", DEFAULT_DELIVERY_LEASE_S, minimum=0.0
            ),
            delivery_request_budget_s=_env_float(
                env,
                "RR_DELIVERY_REQUEST_BUDGET_S",
                DEFAULT_DELIVERY_REQUEST_BUDGET_S,
                minimum=0.0,
            ),
            dispatch_concurrency=_env_int(
                env, "RR_DISPATCH_CONCURRENCY", DEFAULT_DISPATCH_CONCURRENCY, minimum=1
            ),
            once_drain_timeout_s=_env_float(
                env, "RR_ONCE_DRAIN_TIMEOUT_S", DEFAULT_ONCE_DRAIN_TIMEOUT_S, minimum=0.0
            ),
            watchdog_scheduler_s=_env_float(
                env, "RR_WATCHDOG_SCHEDULER_S", DEFAULT_WATCHDOG_SCHEDULER_S, minimum=0.0
            ),
            watchdog_dispatch_s=_env_float(
                env, "RR_WATCHDOG_DISPATCH_S", DEFAULT_WATCHDOG_DISPATCH_S, minimum=0.0
            ),
            watchdog_startup_grace_s=_env_float(
                env,
                "RR_WATCHDOG_STARTUP_GRACE_S",
                DEFAULT_WATCHDOG_STARTUP_GRACE_S,
                minimum=0.0,
            ),
            dry_run=_env_bool(env, "RR_DRY_RUN", False),
            once=_env_bool(env, "RR_ONCE", False),
        )
