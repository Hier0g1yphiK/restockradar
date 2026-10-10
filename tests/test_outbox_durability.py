"""M2 task 14.4 — Property 12: Outbox rows survive a restart.

For any set of committed pending ``alert_deliveries`` rows (and their ``alerts_outbox``
parents), closing the database and reopening it leaves the pending set byte-identical, so
the Notifier resumes delivering exactly those rows after a restart (Req 6.5).

The test uses a temp-FILE database (so the close/reopen is a real restart, not an
in-memory DB that would vanish), migrated with the REAL
:func:`~restockradar.persistence.migrate.migrate` runner. Hypothesis generates a varying
number of alerts, each fanned to a varying subset of destinations; the write-set helper
commits them, the DB is closed and reopened, and the surviving pending rows are compared
exactly. The suite-wide offline guard keeps everything local.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from restockradar.domain.ids import EventType, SubjectKind, listing_subject_key
from restockradar.domain.models import Existence
from restockradar.persistence.db import Database
from restockradar.persistence.migrate import migrate
from restockradar.persistence.repository import (
    AlertStateUpsert,
    DeliveryTarget,
    HistoryRow,
    OutboxRow,
    StateDelta,
    WriteSet,
    write_transition,
)

_RETAILER_ID = "northern-card-gaming"
_GAME_ID = "pokemon"
_PRODUCT_ID = "ncg-pokemon-booster"
_LISTING_ID = 1
_DEST_IDS = (1, 2, 3)
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _migrated_db(path: str | Path) -> Database:
    db = Database(path)
    asyncio.run(migrate(db))
    return db


def _seed_catalogue(db: Database) -> None:
    """Insert the game/retailer/product/listing and three product destinations."""

    def _run(conn: Any) -> None:
        conn.execute("INSERT INTO games (id, name) VALUES (?, ?)", (_GAME_ID, "Pokemon"))
        conn.execute(
            "INSERT INTO retailers (id, name, adapter_type, base_url) VALUES (?, ?, ?, ?)",
            (_RETAILER_ID, "Northern Card Gaming", "shopify", "https://example.test"),
        )
        conn.execute(
            "INSERT INTO products (id, game_id, name) VALUES (?, ?, ?)",
            (_PRODUCT_ID, _GAME_ID, "A Pokemon product"),
        )
        conn.execute(
            "INSERT INTO listings "
            "  (id, config_key, product_id, retailer_id, url, handle, existence) "
            "VALUES (?, ?, ?, ?, ?, ?, 1)",
            (
                _LISTING_ID,
                "listing-config-1",
                _PRODUCT_ID,
                _RETAILER_ID,
                "https://example.test/products/a-pokemon-product",
                "a-pokemon-product",
            ),
        )
        for dest_id in _DEST_IDS:
            conn.execute(
                "INSERT INTO destinations "
                "  (id, config_key, server_id, game_id, webhook_env_var) "
                "VALUES (?, ?, ?, ?, ?)",
                (dest_id, f"dest-{dest_id}", "server-1", _GAME_ID, f"RR_WEBHOOK_{dest_id}"),
            )

    asyncio.run(db.run(_run))


def _run_on_db(db: Database, func: Any) -> Any:
    return asyncio.run(db.run(func))


def _read_pending(db: Database) -> list[tuple[Any, ...]]:
    """Return every pending delivery joined to its outbox parent, deterministically ordered."""
    rows = _run_on_db(
        db,
        lambda conn: conn.execute(
            "SELECT d.alert_id, d.destination_id, d.status, d.attempts, "
            "       d.next_attempt_at, o.event_type, o.payload, o.created_at "
            "FROM alert_deliveries d JOIN alerts_outbox o ON o.id = d.alert_id "
            "WHERE d.status = 'pending' "
            "ORDER BY d.alert_id, d.destination_id"
        ).fetchall(),
    )
    return [tuple(r) for r in rows]


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    data=st.data(),
    alert_count=st.integers(min_value=1, max_value=5),
)
def test_property_12_outbox_rows_survive_a_restart(
    tmp_path_factory: Any, data: st.DataObject, alert_count: int
) -> None:
    """Feature: stock-drop-monitor, Property 12: Outbox rows survive a restart.

    Validates: Requirements 6.5

    For any committed set of pending deliveries, closing and reopening the database
    leaves the pending set identical, so the Notifier resumes delivering exactly those
    rows after a restart.
    """
    db_path = tmp_path_factory.mktemp("outbox") / "restockradar.db"
    db = _migrated_db(db_path)
    try:
        _seed_catalogue(db)
        subject_key = listing_subject_key(_LISTING_ID)

        for n in range(alert_count):
            # Each alert fans out to a non-empty subset of the destinations.
            targets = data.draw(
                st.lists(
                    st.sampled_from(_DEST_IDS),
                    min_size=1,
                    max_size=len(_DEST_IDS),
                    unique=True,
                ),
                label=f"targets_{n}",
            )
            price = data.draw(
                st.decimals(
                    min_value=Decimal("0"),
                    max_value=Decimal("100000"),
                    allow_nan=False,
                    allow_infinity=False,
                    places=2,
                ),
                label=f"price_{n}",
            )
            write_set = WriteSet(
                state=StateDelta(
                    listing_id=_LISTING_ID,
                    in_stock=True,
                    set_in_stock=True,
                    price=price,
                    set_price=True,
                    existence=Existence.EXISTING,
                    set_existence=True,
                ),
                alert_state=(
                    AlertStateUpsert(
                        subject_key=subject_key,
                        event_type=EventType.PRICE_CHANGE,
                        subject_kind=SubjectKind.LISTING,
                        listing_id=_LISTING_ID,
                        last_alert_at=_NOW,
                        set_last_alert_at=True,
                        last_alerted_value=price,
                        set_last_alerted_value=True,
                    ),
                ),
                outbox=OutboxRow(
                    alert_kind="product",
                    event_type=EventType.PRICE_CHANGE.value,
                    game_id=_GAME_ID,
                    subject_kind=SubjectKind.LISTING.value,
                    listing_id=_LISTING_ID,
                    feed_id=None,
                    external_id=None,
                    payload=f'{{"price": "{price}"}}',
                    created_at=_NOW,
                ),
                history=HistoryRow(
                    subject_kind=SubjectKind.LISTING.value,
                    listing_id=_LISTING_ID,
                    feed_id=None,
                    external_id=None,
                    event_type=EventType.PRICE_CHANGE.value,
                    detected_at=_NOW,
                    detail=None,
                ),
                deliveries=tuple(
                    DeliveryTarget(destination_id=d, next_attempt_at=_NOW) for d in targets
                ),
            )
            _run_on_db(db, lambda conn, ws=write_set: write_transition(db, conn, ws))

        before = _read_pending(db)
        assert before, "expected at least one pending delivery to be written"
    finally:
        db.close_sync()

    # Simulated restart: reopen the same file. The committed pending set must be identical.
    db2 = Database(db_path)
    try:
        after = _read_pending(db2)
        assert after == before
    finally:
        db2.close_sync()
