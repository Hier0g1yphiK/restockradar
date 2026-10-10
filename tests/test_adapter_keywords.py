"""Tasks 8.3 / 8.4 — keyword-matching tests for ``adapters/keywords.py``.

* **8.3 — Property 3: exclude keywords take precedence over include keywords** (Req 3.3,
  3.4). Over arbitrary Unicode / accent / case variation, if any exclude keyword matches
  the product then no event is emitted; otherwise an event is emitted iff the include
  list is empty or some include keyword matches.
* **8.4 — unit test (Req 21.14): matching is Unicode-normalised, accent-insensitive, and
  case-insensitive** (e.g. ``"Pokémon"`` matches ``"pokemon"``).
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from restockradar.adapters.base import DiscoveredProduct, VariantState
from restockradar.adapters.keywords import matches, matches_fields, normalise


def _product(
    *,
    title: str = "",
    product_type: str | None = None,
    vendor: str | None = None,
    tags: Sequence[str] = (),
) -> DiscoveredProduct:
    """Build a :class:`DiscoveredProduct` carrying only the keyword-match surfaces."""
    return DiscoveredProduct(
        external_id="1",
        title=title,
        product_type=product_type,
        vendor=vendor,
        tags=tuple(tags),
        handle="h",
        url="https://example.test/products/h",
        image=None,
        variants=None,
    )


# ---------------------------------------------------------------------------
# 8.4 — Unicode / accent / case insensitivity (Req 21.14)
# ---------------------------------------------------------------------------


def test_normalise_strips_accents_and_casefolds() -> None:
    """``normalise`` makes an accented, mixed-case string match its plain lowercase form."""
    assert normalise("Pokémon") == normalise("pokemon") == "pokemon"


def test_accented_include_matches_plain_product_title() -> None:
    """An accented include keyword matches a plain (unaccented) product title."""
    product = _product(title="Pokemon Scarlet & Violet Booster Box")
    assert matches(product, includes=["pokémon"], excludes=[]) is True


def test_plain_include_matches_accented_product_title() -> None:
    """A plain include keyword matches an accented product title (symmetry)."""
    product = _product(title="Pokémon Scarlet & Violet Booster Box")
    assert matches(product, includes=["pokemon"], excludes=[]) is True


def test_matching_is_case_insensitive() -> None:
    product = _product(title="POKEMON booster")
    assert matches(product, includes=["Pokemon"], excludes=[]) is True


def test_matches_across_all_haystack_fields() -> None:
    """Include keywords are tested against title, product_type, vendor, and tags."""
    assert matches(_product(product_type="Pokémon TCG"), includes=["pokemon"], excludes=[]) is True
    assert matches(_product(vendor="Pokémon Company"), includes=["pokemon"], excludes=[]) is True
    assert matches(_product(tags=["Préorder"]), includes=["preorder"], excludes=[]) is True


def test_empty_include_list_means_match_all() -> None:
    """An empty include list accepts any product that no exclude keyword rejects (Req 3.3)."""
    assert matches(_product(title="anything at all"), includes=[], excludes=[]) is True


def test_no_include_match_rejects() -> None:
    assert matches(_product(title="plain magic card"), includes=["pokemon"], excludes=[]) is False


def test_exclude_is_also_accent_and_case_insensitive() -> None:
    """Exclude matching uses the same Unicode-normalised comparison (Req 3.4, 21.14)."""
    product = _product(title="Pokémon Plüsh Toy")
    assert matches(product, includes=["pokemon"], excludes=["plush"]) is False


# ---------------------------------------------------------------------------
# 8.3 — Property 3: exclude precedes include (Req 3.3, 3.4)
# ---------------------------------------------------------------------------

# Keep the alphabet small and mixed so Hypothesis explores accent, case, and
# compatibility variation while still producing frequent substring hits.
_KEYWORD_CHARS = "abPÉéK\u00b2\ufb01"  # letters, accents, case + compatibility chars (², ﬁ)
_keywords = st.text(alphabet=_KEYWORD_CHARS, min_size=1, max_size=4)
_free_text = st.text(alphabet=_KEYWORD_CHARS, max_size=8)
_keyword_lists = st.lists(_keywords, max_size=4)


def _expected(
    *,
    title: str,
    product_type: str | None,
    vendor: str | None,
    tags: Sequence[str],
    includes: Sequence[str],
    excludes: Sequence[str],
) -> bool:
    """Independent reference implementation of the Req 3.3/3.4 decision rule."""
    haystacks = [normalise(h) for h in (title, product_type or "", vendor or "", *tags)]

    def hit(keywords: Sequence[str]) -> bool:
        return any(normalise(k) in h for k in keywords for h in haystacks)

    if hit(excludes):
        return False
    return not includes or hit(includes)


@settings(max_examples=200)
@given(
    title=_free_text,
    product_type=st.none() | _free_text,
    vendor=st.none() | _free_text,
    tags=st.lists(_free_text, max_size=3),
    includes=_keyword_lists,
    excludes=_keyword_lists,
)
def test_exclude_takes_precedence_over_include(
    title: str,
    product_type: str | None,
    vendor: str | None,
    tags: list[str],
    includes: list[str],
    excludes: list[str],
) -> None:
    """Property 3 — exclude keywords take precedence over include keywords.

    Feature: stock-drop-monitor, Property 3: Exclude keywords take precedence over
    include keywords.

    Validates: Requirements 3.3, 3.4.

    For all products and include/exclude lists over arbitrary Unicode/accent/case
    variation: any exclude match rejects the product regardless of includes; otherwise
    it is accepted iff the include list is empty or some include keyword matches.
    """
    result = matches_fields(
        title=title,
        product_type=product_type,
        vendor=vendor,
        tags=tags,
        includes=includes,
        excludes=excludes,
    )
    assert result == _expected(
        title=title,
        product_type=product_type,
        vendor=vendor,
        tags=tags,
        includes=includes,
        excludes=excludes,
    )

    # Stronger directional check: if any exclude keyword matches, the result is False
    # no matter what the include list contains — exclude strictly wins (Req 3.4).
    haystacks = [normalise(h) for h in (title, product_type or "", vendor or "", *tags)]
    if any(normalise(x) in h for x in excludes for h in haystacks):
        assert result is False


def test_exclude_wins_even_when_include_also_matches() -> None:
    """A concrete witness for Property 3: the same word in both lists is excluded."""
    product = _product(title="Pokémon plush")
    assert matches(product, includes=["pokemon"], excludes=["pokemon"]) is False


def test_variant_state_import_is_usable() -> None:
    """Guard that the self-contained value types are importable from base (Req 11.2)."""
    variant = VariantState(variant_id="v1", available=True, price=Decimal("9.99"))
    assert variant.available is True
