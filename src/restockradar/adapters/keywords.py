"""Keyword matching: Unicode NFKD normalise + strip combining marks (accent-
insensitive) + casefold; substring match over title/product_type/vendor/tags;
exclude precedes include; empty-include means match-all (Req 3.3, 3.4).

Matching is applied by the core, not by any adapter, so it stays retailer-agnostic.
Normalisation decomposes with ``unicodedata.normalize("NFKD", …)``, strips combining
marks (so ``"Pokémon"`` matches ``"pokemon"``), and casefolds (so matching is
case-insensitive). A keyword matches when its normalised form is a substring of any
normalised haystack field (``title``, ``product_type``, ``vendor``, or any ``tag``).

Decision rule (Req 3.3/3.4): an **exclude** match wins outright — if any exclude keyword
matches, the product is rejected regardless of includes. Otherwise the product is
accepted when the include list is empty (match-all) or when any include keyword matches.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable, Sequence

from .base import DiscoveredProduct


def normalise(text: str) -> str:
    """Normalise ``text`` for accent- and case-insensitive substring matching.

    NFKD-decompose, drop combining marks (accent-insensitive), then casefold
    (case-insensitive), e.g. ``"Pokémon"`` and ``"pokemon"`` both become ``"pokemon"``.
    """
    decomposed = unicodedata.normalize("NFKD", text)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return without_marks.casefold()


def _any_keyword_matches(keywords: Iterable[str], haystacks: Sequence[str]) -> bool:
    """Return whether any normalised ``keyword`` is a substring of any normalised haystack."""
    normalised_haystacks = [normalise(h) for h in haystacks]
    return any(
        normalise(keyword) in haystack
        for keyword in keywords
        for haystack in normalised_haystacks
    )


def matches_fields(
    *,
    title: str,
    product_type: str | None,
    vendor: str | None,
    tags: Sequence[str],
    includes: Sequence[str],
    excludes: Sequence[str],
) -> bool:
    """Decide whether a product described by these fields passes the keyword filter.

    Exclude takes precedence over include (Req 3.4); an empty ``includes`` list means
    match-all (Req 3.3). The haystack is the ``title``, ``product_type``, ``vendor``,
    and each ``tag`` (Req 3.3/3.4).
    """
    haystacks: list[str] = [title, product_type or "", vendor or "", *tags]
    if _any_keyword_matches(excludes, haystacks):
        return False  # exclude wins (Req 3.4)
    return not includes or _any_keyword_matches(includes, haystacks)


def matches(
    product: DiscoveredProduct,
    includes: Sequence[str],
    excludes: Sequence[str],
) -> bool:
    """Decide whether a discovered ``product`` passes the keyword filter (Req 3.3/3.4)."""
    return matches_fields(
        title=product.title,
        product_type=product.product_type,
        vendor=product.vendor,
        tags=product.tags,
        includes=includes,
        excludes=excludes,
    )


__all__ = ["matches", "matches_fields", "normalise"]
