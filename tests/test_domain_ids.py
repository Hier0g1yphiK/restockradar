"""Task 6.1 — unit + property tests for the domain key builders and models
(Req 14.10, 14.12, 11.2).

The ``subject_key`` / ``scope_key`` builders are the single source of truth for the
string format of the single-column keys used by ``alert_state``, ``backoff_state``,
and the ``incidents`` open-lookup. They are logic worth testing on their own: the
tests here pin the exact string format (so a stored key stays stable across versions)
and prove each builder round-trips through its parser without ambiguity — including
the awkward cases where an ``external_id`` or a ``path`` contains a colon.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from restockradar.domain.ids import (
    EventType,
    ScopeKind,
    SubjectKind,
    feed_product_subject_key,
    listing_subject_key,
    parse_scope_key,
    parse_subject_key,
    scope_destination_key,
    scope_feed_key,
    scope_listing_key,
    scope_path_key,
    scope_retailer_key,
    scope_system_key,
)
from restockradar.domain.models import (
    Destination,
    DiscoveryFeed,
    Existence,
    Game,
    HealthStatus,
    Listing,
    Product,
    Retailer,
    Source,
)

# ---------------------------------------------------------------------------
# EventType enum
# ---------------------------------------------------------------------------


def test_event_type_values_are_stored_tokens() -> None:
    """The enum's string value is the exact token stored in the DB columns."""
    assert EventType.RESTOCK.value == "restock"
    assert EventType.PRICE_CHANGE.value == "price_change"
    assert EventType.NEW_PRODUCT.value == "new_product"
    assert EventType.PAGE_LIVE.value == "page_live"

    # The StrEnum members serialise to those same tokens (member -> stored token).
    assert str(EventType.RESTOCK) == "restock"
    assert str(EventType.PRICE_CHANGE) == "price_change"
    assert str(EventType.NEW_PRODUCT) == "new_product"
    assert str(EventType.PAGE_LIVE) == "page_live"


def test_only_restock_and_price_change_carry_cooldown() -> None:
    """Exactly the two cooldown-bearing events own an alert_state row (Req 9.6)."""
    assert EventType.RESTOCK.carries_cooldown
    assert EventType.PRICE_CHANGE.carries_cooldown
    assert not EventType.NEW_PRODUCT.carries_cooldown
    assert not EventType.PAGE_LIVE.carries_cooldown


# ---------------------------------------------------------------------------
# subject_key — format + round-trip
# ---------------------------------------------------------------------------


def test_listing_subject_key_format() -> None:
    assert listing_subject_key(12) == "listing:12"


def test_feed_product_subject_key_format() -> None:
    assert feed_product_subject_key(3, "7891234") == "feed:3:ext:7891234"


def test_listing_subject_key_round_trips() -> None:
    kind, id_, external_id = parse_subject_key(listing_subject_key(42))
    assert (kind, id_, external_id) == (SubjectKind.LISTING, 42, None)


def test_feed_product_subject_key_round_trips() -> None:
    kind, id_, external_id = parse_subject_key(feed_product_subject_key(9, "abc123"))
    assert (kind, id_, external_id) == (SubjectKind.FEED_PRODUCT, 9, "abc123")


def test_feed_product_external_id_with_colons_round_trips() -> None:
    """An external_id that itself contains ':' survives the round-trip unambiguously."""
    weird = "gid://shopify/Product/7891234:v2"
    key = feed_product_subject_key(5, weird)
    kind, id_, external_id = parse_subject_key(key)
    assert (kind, id_, external_id) == (SubjectKind.FEED_PRODUCT, 5, weird)


def test_feed_product_subject_key_rejects_empty_external_id() -> None:
    with pytest.raises(ValueError, match="external_id"):
        feed_product_subject_key(1, "")


@pytest.mark.parametrize(
    "bad",
    ["", "listing:", "listing:abc", "feed:3", "feed:x:ext:1", "feed:3:ext:", "bogus:1"],
)
def test_parse_subject_key_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_subject_key(bad)


@given(st.integers(min_value=0, max_value=2**31))
def test_listing_subject_key_round_trip_property(listing_id: int) -> None:
    """Property: every listing id builds a key that parses back to the same id."""
    kind, id_, external_id = parse_subject_key(listing_subject_key(listing_id))
    assert kind is SubjectKind.LISTING
    assert id_ == listing_id
    assert external_id is None


@given(
    st.integers(min_value=0, max_value=2**31),
    st.text(min_size=1).filter(lambda s: s.strip() != ""),
)
def test_feed_product_subject_key_round_trip_property(feed_id: int, external_id: str) -> None:
    """Property: any non-empty external_id round-trips verbatim for any feed id."""
    key = feed_product_subject_key(feed_id, external_id)
    kind, id_, parsed_external = parse_subject_key(key)
    assert kind is SubjectKind.FEED_PRODUCT
    assert id_ == feed_id
    assert parsed_external == external_id


# ---------------------------------------------------------------------------
# scope_key — format + round-trip
# ---------------------------------------------------------------------------


def test_scope_key_formats() -> None:
    assert scope_retailer_key("northern-card-gaming") == "retailer:northern-card-gaming"
    assert scope_feed_key(3) == "feed:3"
    assert scope_listing_key(12) == "listing:12"
    assert scope_path_key("/collections/pokemon/products.json") == (
        "path:/collections/pokemon/products.json"
    )
    assert scope_destination_key(4) == "destination:4"
    assert scope_system_key() == "system"


@pytest.mark.parametrize(
    ("builder_key", "expected_kind", "expected_id"),
    [
        (scope_retailer_key("ncg"), ScopeKind.RETAILER, "ncg"),
        (scope_feed_key(3), ScopeKind.FEED, "3"),
        (scope_listing_key(12), ScopeKind.LISTING, "12"),
        (scope_destination_key(7), ScopeKind.DESTINATION, "7"),
        (scope_path_key("/a/b.json"), ScopeKind.PATH, "/a/b.json"),
        (scope_system_key(), ScopeKind.SYSTEM, None),
    ],
)
def test_scope_key_round_trips(
    builder_key: str, expected_kind: ScopeKind, expected_id: str | None
) -> None:
    kind, raw_id = parse_scope_key(builder_key)
    assert kind is expected_kind
    assert raw_id == expected_id


def test_scope_path_key_with_colon_round_trips() -> None:
    """A path containing ':' keeps the whole remainder as its id."""
    path = "/weird:segment/products.json"
    kind, raw_id = parse_scope_key(scope_path_key(path))
    assert kind is ScopeKind.PATH
    assert raw_id == path


@pytest.mark.parametrize("bad", ["", "retailer:", "feed", "unknown:1", "systemx"])
def test_parse_scope_key_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_scope_key(bad)


def test_scope_key_builders_reject_empty_text_ids() -> None:
    with pytest.raises(ValueError):
        scope_retailer_key("")
    with pytest.raises(ValueError):
        scope_path_key("")


@given(st.integers(min_value=0, max_value=2**31))
def test_scope_feed_listing_destination_round_trip_property(id_: int) -> None:
    """Property: integer-id scope keys round-trip to the same decimal id text."""
    for builder, kind in (
        (scope_feed_key, ScopeKind.FEED),
        (scope_listing_key, ScopeKind.LISTING),
        (scope_destination_key, ScopeKind.DESTINATION),
    ):
        parsed_kind, raw_id = parse_scope_key(builder(id_))
        assert parsed_kind is kind
        assert raw_id == str(id_)


# ---------------------------------------------------------------------------
# Domain models — type + invariant smoke checks
# ---------------------------------------------------------------------------


def test_models_are_frozen() -> None:
    """Dataclasses are immutable: assignment raises (frozen=True)."""
    game = Game(id="pokemon", name="Pokémon TCG")
    with pytest.raises(AttributeError):
        game.name = "changed"  # type: ignore[misc]


def test_listing_carries_decimal_price_and_defaults() -> None:
    listing = Listing(
        id=1,
        config_key="ncg:booster",
        product_id="p1",
        retailer_id="ncg",
        url="https://x/products/booster",
        handle="booster",
        price=Decimal("12.99"),
        existence=Existence.EXISTING,
        in_stock=True,
    )
    assert listing.price == Decimal("12.99")
    assert listing.external_id is None  # unseeded default
    assert listing.seeded_at is None
    assert listing.source is Source.CONFIG
    assert listing.enabled is True


def test_discovery_feed_keywords_are_tuples() -> None:
    feed = DiscoveryFeed(
        id=1,
        config_key="ncg:pokemon",
        retailer_id="ncg",
        game_id="pokemon",
        feed_url="/collections/pokemon/products.json",
        include_keywords=("pokemon",),
        exclude_keywords=("plush", "playmat"),
    )
    assert feed.include_keywords == ("pokemon",)
    assert feed.exclude_keywords == ("plush", "playmat")
    assert feed.max_page_count == 20


def test_admin_destination_may_omit_game() -> None:
    dest = Destination(
        id=1,
        config_key="ncg-admin",
        server_id="123",
        webhook_env_var="DISCORD_WEBHOOK_ADMIN",
        is_admin=True,
    )
    assert dest.game_id is None
    assert dest.health_status is HealthStatus.HEALTHY


def test_non_admin_destination_requires_game() -> None:
    with pytest.raises(ValueError, match="requires a game_id"):
        Destination(
            id=2,
            config_key="ncg-pokemon",
            server_id="123",
            webhook_env_var="DISCORD_WEBHOOK_POKEMON",
            is_admin=False,
            game_id=None,
        )


def test_retailer_defaults_match_schema() -> None:
    retailer = Retailer(
        id="ncg",
        name="Northern Card Gaming",
        adapter_type="shopify",
        base_url="https://www.northerncardgaming.com",
    )
    assert retailer.poll_interval_s == 60
    assert retailer.minor_unit_exponent == 2


def test_product_category_optional() -> None:
    product = Product(id="p1", game_id="pokemon", name="Booster Bundle")
    assert product.category is None
