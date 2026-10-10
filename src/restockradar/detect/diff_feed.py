"""New-product detection vs ``seen_products`` with keyword filtering (Req 3, 12).

Always records ``seen_products`` regardless of include/exclude/alert, and suppresses
the new-product alert when the product matches an enabled Listing on the same retailer
by external_id OR handle (the Listing already covers it).

This module is a **pure** function: :func:`diff_feed` takes this cycle's discovered
products plus the already-seen external-id set, the feed's include/exclude keywords, and
the tracked-listing identities, and RETURNS the new-product events to emit alongside the
full set of :class:`~restockradar.persistence.repository.BaselineProduct` rows to record
in ``seen_products``. It opens no connection and writes nothing — the fan-out/write
happens in task 17 via the existing ``seed_feed`` / ``write_feed_baseline`` path.

Three rules govern the split between *recording* and *alerting*:

* **Record everything** (Req 3.5): every discovered product becomes a
  :class:`BaselineProduct` in :attr:`FeedDiffResult.to_record`, regardless of keyword
  match or tracked-listing coverage. Keyword filtering only gates the ALERT.
* **Keyword filter gates the alert** (Req 3.3/3.4): the alert is emitted only when
  :func:`restockradar.adapters.keywords.matches` accepts the product (exclude precedes
  include; an empty include list means match-all).
* **Tracked-listing suppression** (Req 3.6, 22.4 / Property 23): a product that matches
  an enabled listing on the same retailer by ``external_id`` OR ``handle`` is recorded
  but emits NO new-product alert — the listing already covers it.

Processing is **order-independent** (Property 2): the function iterates the input once
and makes each decision from the product alone plus the (frozen) seen / tracked sets, so
any permutation of ``discovered`` yields the same event set and the same recorded set.
A product whose ``external_id`` is NULL/empty cannot form a dedup key and is skipped
entirely (neither recorded nor alerted); a duplicate ``external_id`` within one batch is
recorded once and alerts at most once.
"""

from __future__ import annotations

from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass

from restockradar.adapters.base import DiscoveredProduct
from restockradar.adapters.keywords import matches
from restockradar.persistence.repository import BaselineProduct

from .events import NewProductEvent

__all__ = ["FeedDiffResult", "diff_feed"]


@dataclass(frozen=True, slots=True, kw_only=True)
class FeedDiffResult:
    """The pure output of :func:`diff_feed` (Req 3.1, 3.5, 3.6).

    ``events`` holds one :class:`NewProductEvent` per genuinely-new, keyword-accepted,
    not-tracked product. ``to_record`` holds a :class:`BaselineProduct` for EVERY
    discovered product that carries a usable ``external_id`` (Req 3.5), ready to hand
    straight to the existing ``seed_feed`` / ``write_feed_baseline`` path (task 17).
    Both are tuples so the result is immutable and cheap to share.
    """

    events: tuple[NewProductEvent, ...]
    to_record: tuple[BaselineProduct, ...]


def diff_feed(
    *,
    retailer_id: str,
    game_id: str,
    feed_id: int,
    discovered: Sequence[DiscoveredProduct],
    seen_external_ids: AbstractSet[str],
    include_keywords: Sequence[str],
    exclude_keywords: Sequence[str],
    tracked_external_ids: AbstractSet[str],
    tracked_handles: AbstractSet[str],
) -> FeedDiffResult:
    """Detect new products on one feed against the already-seen set (Req 3, PURE).

    Args:
        retailer_id: The feed's retailer id (part of the dedup identity).
        game_id: The feed's game id (part of the dedup identity).
        feed_id: The feed's id, carried on each emitted :class:`NewProductEvent`.
        discovered: This cycle's products returned by the adapter's ``discover``.
        seen_external_ids: The ``external_id`` values already recorded in
            ``seen_products`` for THIS retailer+game. A product whose ``external_id`` is
            in this set is not new and emits no alert.
        include_keywords: The feed's include keywords (empty => match-all, Req 3.3).
        exclude_keywords: The feed's exclude keywords (exclude precedes include, Req 3.4).
        tracked_external_ids: ``external_id`` values of enabled listings on this retailer.
        tracked_handles: ``handle`` values of enabled listings on this retailer.

    Returns:
        A :class:`FeedDiffResult` carrying the new-product events and the full set of
        products to record in ``seen_products``.

    The decision per product, in order:

    1. Skip products with no usable ``external_id`` (cannot form a dedup key).
    2. Record the product in ``to_record`` regardless of everything that follows
       (Req 3.5), de-duplicating repeated ``external_id`` values within the batch.
    3. Emit a :class:`NewProductEvent` only when the product is genuinely new (its
       ``external_id`` is NOT in ``seen_external_ids``), passes the keyword filter
       (Req 3.3/3.4), and is NOT covered by a tracked listing (Req 3.6).

    Because every decision depends only on the product plus the frozen seen/tracked
    sets, the result is independent of the order of ``discovered`` (Property 2).
    """
    events: list[NewProductEvent] = []
    to_record: list[BaselineProduct] = []
    recorded_ids: set[str] = set()
    alerted_ids: set[str] = set()

    for product in discovered:
        external_id = product.external_id
        if not external_id:
            # No dedup key => cannot record or alert on this product (Req 14.7).
            continue

        # Rule 1: record every discovered product exactly once (Req 3.5), independent of
        # keyword match or tracked-listing coverage.
        if external_id not in recorded_ids:
            recorded_ids.add(external_id)
            to_record.append(
                BaselineProduct(
                    external_id=external_id,
                    title=product.title,
                    handle=product.handle,
                )
            )

        # Rule 2: a product already seen for this retailer+game is not new (no alert).
        if external_id in seen_external_ids:
            continue

        # Rule 3: tracked-listing suppression — an enabled listing already covers this
        # product by external_id OR handle, so record-only, no alert (Req 3.6 / Prop 23).
        if external_id in tracked_external_ids or product.handle in tracked_handles:
            continue

        # Rule 4: the keyword filter gates ONLY the alert (Req 3.3/3.4). Exclude precedes
        # include; an empty include list means match-all.
        if not matches(product, include_keywords, exclude_keywords):
            continue

        # Guard against a duplicate external_id within one batch alerting twice.
        if external_id in alerted_ids:
            continue
        alerted_ids.add(external_id)

        events.append(
            NewProductEvent(
                retailer_id=retailer_id,
                game_id=game_id,
                feed_id=feed_id,
                external_id=external_id,
                title=product.title,
                handle=product.handle,
                url=product.url,
                image=product.image,
            )
        )

    return FeedDiffResult(events=tuple(events), to_record=tuple(to_record))
