#!/usr/bin/env python
"""Manual operator tool: capture real Northern Card Gaming fixtures (task 10).

Run **by hand** to hit live Northern Card Gaming, save the raw discovery-feed and
single-listing JSON (plus a reminder about the synthetic soft-block HTML sample) under
``fixtures/ncg/``, and print a candidate table of real listings the operator then uses
to author ``config/config.yaml`` (task 11).

This script is **NOT** part of the pytest suite. It lives under ``scripts/`` (not under
``tests/``), its filename does not match pytest's ``test_*``/``*_test`` discovery
patterns, and ``scripts/`` is deliberately not an importable package, so the offline
suite (guarded by the no-network transport, Req 21.4) never collects or runs it.

It reuses the real M1 HTTP stack rather than opening a second HTTP path:

* :class:`~restockradar.http.client.HttpClient` — the single choke point that already
  enforces robots.txt on the request path (Req 15.5), sends the descriptive User-Agent
  (Req 15.4), applies the 30s per-request timeout, and classifies Blocked / SoftBlock
  challenges (Req 16). It beats the heartbeat per request too.
* :class:`~restockradar.http.host_gate.HostGate` — ≥1s spacing + one in-flight request
  per host (Req 1.4).
* :class:`~restockradar.observability.heartbeat.HeartbeatRegistry` — the per-request
  liveness beat the client expects.

Politeness / safety (Req 1.4, 15.4, 15.5, 22.8):

* A hard cap on the **total** number of HTTP requests (robots.txt + feed pages + ``.js``
  pages) at :data:`MAX_TOTAL_REQUESTS`; the script stops before exceeding it.
* It STOPS IMMEDIATELY and exits non-zero on any :class:`~restockradar.adapters.errors.Blocked`
  (including :class:`~restockradar.adapters.errors.SoftBlock`) or an HTTP 429 — it never
  hammers a retailer that is pushing back.
* robots.txt and host spacing are honoured because every request goes through the shared
  client / host gate above.

Usage (from the repo root, with the project venv)::

    .\\.venv\\Scripts\\python.exe scripts\\capture_ncg_fixtures.py

Nothing is written to ``config/config.yaml`` — the operator approves that separately in
task 11 using the candidate table this script prints.
"""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import httpx

from restockradar.adapters.errors import Blocked, FetchError
from restockradar.http.client import HttpClient
from restockradar.http.host_gate import HostGate
from restockradar.observability.heartbeat import HeartbeatRegistry

# ---------------------------------------------------------------------------
# Capture configuration (MVP targets — Req 22.1/22.3)
# ---------------------------------------------------------------------------

#: Northern Card Gaming origin (Req 22.1).
NCG_BASE_URL: Final = "https://www.northerncardgaming.com"

#: Minor-unit exponent for NCG prices (Req 11.5 / 22.1 — major = minor / 10**exp).
MINOR_UNIT_EXPONENT: Final = 2

#: The two MVP discovery feeds, paged (Req 22.3 / 12.1).
FEEDS: Final[tuple[tuple[str, str], ...]] = (
    ("collections_pokemon", "/collections/pokemon/products.json"),
    ("collections_pre_orders_1", "/collections/pre-orders-1/products.json"),
)

#: Shopify discovery page size (Req 12.5).
PAGE_LIMIT: Final = 250

#: Cap on discovery pages per feed (Req 12.6 default). Real feeds stop far sooner on a
#: short page; this is a safety ceiling so a misbehaving feed cannot loop forever.
MAX_PAGE_COUNT: Final = 20

#: Hard ceiling on the TOTAL number of HTTP requests this run may issue — robots.txt,
#: every feed page, and every ``.js`` product fetch all count. The script stops before
#: exceeding it so a hand-run capture stays polite (Req 1.4 / 22.8).
MAX_TOTAL_REQUESTS: Final = 15

#: How many candidate product handles to fetch as ``.js`` (3-5 real listings).
CANDIDATE_PRODUCT_COUNT: Final = 5

#: Where fixtures are written, relative to the repo root (Req 21.3 / 22.8).
FIXTURES_DIR: Final = Path(__file__).resolve().parent.parent / "fixtures" / "ncg"


class RequestBudget:
    """A simple shared counter enforcing :data:`MAX_TOTAL_REQUESTS` across the run."""

    __slots__ = ("_limit", "_used")

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._used = 0

    @property
    def used(self) -> int:
        """Number of HTTP requests charged so far."""
        return self._used

    @property
    def remaining(self) -> int:
        """How many requests are still permitted."""
        return max(0, self._limit - self._used)

    def can_spend(self) -> bool:
        """``True`` while at least one request is still within budget."""
        return self._used < self._limit

    def charge(self) -> None:
        """Record one more request against the budget."""
        self._used += 1


@dataclass(frozen=True)
class Candidate:
    """A row of the operator-facing candidate table derived from a feed product."""

    handle: str
    title: str
    price: Decimal | None
    in_stock: bool
    product_type: str


class StopCapture(RuntimeError):
    """Raised to abort the capture early (budget exhausted or a block was detected)."""


# ---------------------------------------------------------------------------
# Fixture IO
# ---------------------------------------------------------------------------


def _ensure_fixtures_dir() -> None:
    """Create ``fixtures/ncg/`` if it does not already exist."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)


def _write_raw(filename: str, text: str) -> Path:
    """Write ``text`` verbatim to ``fixtures/ncg/{filename}`` and return its path.

    The body is saved exactly as the retailer returned it (no reshaping / re-indenting),
    so the fixture is a faithful capture for the adapter and diffing tests to consume.
    """
    path = FIXTURES_DIR / filename
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Capture steps
# ---------------------------------------------------------------------------


async def _capture_feed_pages(
    client: HttpClient,
    budget: RequestBudget,
    name: str,
    feed_path: str,
    saved: list[Path],
    products: list[dict[str, Any]],
) -> None:
    """Page one discovery feed with ``limit=250`` & incrementing ``page``, saving each
    page's RAW JSON and accumulating its ``products`` entries for the candidate table.

    Stops on a short/empty page (feed exhausted), at :data:`MAX_PAGE_COUNT`, or when the
    shared request budget would be exceeded. Each page costs one request against the
    budget; the shared :class:`HttpClient` enforces robots/spacing/UA/classification.
    """
    for page in range(1, MAX_PAGE_COUNT + 1):
        if not budget.can_spend():
            print(
                f"  ! request budget ({MAX_TOTAL_REQUESTS}) reached; stopping "
                f"{feed_path} at page {page}"
            )
            raise StopCapture
        url = f"{NCG_BASE_URL}{feed_path}?limit={PAGE_LIMIT}&page={page}"
        budget.charge()
        print(f"  GET {url}  (request {budget.used}/{MAX_TOTAL_REQUESTS})")
        response = await _get_checked(client, url)
        text = response.text
        path = _write_raw(f"{name}_products_page{page}.json", text)
        saved.append(path)
        print(f"    -> saved {path.name}")

        page_products = _extract_products(text, url)
        products.extend(page_products)
        if len(page_products) < PAGE_LIMIT:
            # A short / empty page means the feed is exhausted (Req 12.6).
            break


async def _capture_products(
    client: HttpClient,
    budget: RequestBudget,
    handles: list[str],
    saved: list[Path],
) -> None:
    """Fetch each candidate ``/products/<handle>.js`` and save its RAW ``.js`` JSON.

    Charges one request per handle against the shared budget and stops early if the
    budget would be exceeded, so the ``.js`` captures never push the run over the cap.
    """
    for handle in handles:
        if not budget.can_spend():
            print(
                f"  ! request budget ({MAX_TOTAL_REQUESTS}) reached; "
                f"skipping remaining product fetches"
            )
            break
        url = f"{NCG_BASE_URL}/products/{handle}.js"
        budget.charge()
        print(f"  GET {url}  (request {budget.used}/{MAX_TOTAL_REQUESTS})")
        response = await _get_checked(client, url)
        if response.status_code == 404:
            print(f"    -> 404 for handle {handle!r}; skipping")
            continue
        path = _write_raw(f"products_{handle}.js.json", response.text)
        saved.append(path)
        print(f"    -> saved {path.name}")


async def _get_checked(client: HttpClient, url: str) -> httpx.Response:
    """GET ``url`` through the shared client, converting a 429 into an immediate stop.

    The client already raises :class:`~restockradar.adapters.errors.Blocked` (incl.
    :class:`~restockradar.adapters.errors.SoftBlock`) for a 403/429/503-challenge or a
    challenge/HTML-where-JSON body; those propagate out of :func:`main` and abort the
    run. A plain 429 that somehow slips through as a returned status is treated the same
    way here, defensively.
    """
    response = await client.get(url)
    if response.status_code == 429:
        raise Blocked(f"HTTP 429 from {url}")
    return response


# ---------------------------------------------------------------------------
# Candidate-table derivation
# ---------------------------------------------------------------------------


def _extract_products(raw_text: str, url: str) -> list[dict[str, Any]]:
    """Parse a feed page body and return its ``products`` list (empty on an odd shape).

    Defensive by design: a page that is not a JSON object, or whose ``products`` is not a
    list, yields no products rather than aborting the capture — the raw page is still
    saved for inspection.
    """
    try:
        body: Any = json.loads(raw_text)
    except json.JSONDecodeError:
        print(f"    ! could not parse JSON from {url}; no candidates from this page")
        return []
    if not isinstance(body, dict):
        return []
    products = body.get("products")
    if not isinstance(products, list):
        return []
    return [p for p in products if isinstance(p, dict)]


def _lowest_price(variants: list[dict[str, Any]]) -> Decimal | None:
    """Lowest AVAILABLE variant price, else lowest overall, else ``None`` (Req 5.2).

    Feed variant prices are decimal strings (major units) — parsed directly, mirroring
    :class:`~restockradar.adapters.shopify.ShopifyAdapter` so the printed price matches
    what the adapter will compute.
    """
    available: list[Decimal] = []
    everything: list[Decimal] = []
    for variant in variants:
        price = _variant_price(variant)
        if price is None:
            continue
        everything.append(price)
        if bool(variant.get("available", False)):
            available.append(price)
    if available:
        return min(available)
    if everything:
        return min(everything)
    return None


def _variant_price(variant: dict[str, Any]) -> Decimal | None:
    """Parse one feed variant's ``price`` (decimal string / number) as major units."""
    raw = variant.get("price")
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except (ArithmeticError, ValueError):
        return None


def _to_candidate(product: dict[str, Any]) -> Candidate:
    """Build a candidate-table row from a feed ``products[]`` entry.

    ``in_stock`` is true when ANY variant is available; the price follows the Shopify
    lowest-available-else-lowest-overall rule.
    """
    raw_variants = product.get("variants")
    variants = (
        [v for v in raw_variants if isinstance(v, dict)]
        if isinstance(raw_variants, list)
        else []
    )
    return Candidate(
        handle=str(product.get("handle", "")),
        title=str(product.get("title", "")),
        price=_lowest_price(variants),
        in_stock=any(bool(v.get("available", False)) for v in variants),
        product_type=str(product.get("product_type", "") or ""),
    )


def _unique_candidates(products: list[dict[str, Any]]) -> list[Candidate]:
    """Dedupe feed products by handle (feeds can repeat across pages) into candidates."""
    seen: set[str] = set()
    candidates: list[Candidate] = []
    for product in products:
        candidate = _to_candidate(product)
        if not candidate.handle or candidate.handle in seen:
            continue
        seen.add(candidate.handle)
        candidates.append(candidate)
    return candidates


def _print_candidate_table(candidates: list[Candidate]) -> None:
    """Print the operator-facing table: handle, title, price, in_stock, product_type.

    This is what the operator reads to choose the real listings to put in
    ``config/config.yaml`` (task 11); the script writes nothing to that file itself.
    """
    print()
    print("Candidate listings (choose real ones for config/config.yaml — task 11):")
    header = f"{'handle':<40} {'title':<44} {'price':>10} {'in_stock':>9}  product_type"
    print(header)
    print("-" * len(header))
    if not candidates:
        print("(no products discovered)")
        return
    for c in candidates:
        price = "-" if c.price is None else f"{c.price}"
        print(
            f"{_clip(c.handle, 40):<40} {_clip(c.title, 44):<44} "
            f"{price:>10} {('yes' if c.in_stock else 'no'):>9}  {c.product_type}"
        )
    print(f"\n{len(candidates)} unique product(s) discovered across the feeds.")


def _clip(text: str, width: int) -> str:
    """Truncate ``text`` to ``width`` characters with an ellipsis, for table alignment."""
    if len(text) <= width:
        return text
    return text[: width - 1] + "\u2026"


def _pick_candidate_handles(candidates: list[Candidate], budget: RequestBudget) -> list[str]:
    """Choose up to :data:`CANDIDATE_PRODUCT_COUNT` handles to fetch as ``.js``.

    Prefers in-stock products (more interesting fixtures), then falls back to any, and
    never asks for more ``.js`` fetches than the remaining request budget allows.
    """
    in_stock = [c.handle for c in candidates if c.in_stock]
    others = [c.handle for c in candidates if not c.in_stock]
    ordered = in_stock + others
    affordable = min(CANDIDATE_PRODUCT_COUNT, budget.remaining)
    return ordered[:affordable]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> int:
    """Run one capture pass; return a process exit code (0 ok, non-zero on a block).

    Builds the real HTTP stack, captures each MVP feed's pages, prints the candidate
    table, fetches a handful of candidate ``.js`` products, and reports the synthetic
    soft-block sample. Any Blocked/SoftBlock/429 stops the run immediately and returns a
    non-zero code without hammering the host.
    """
    _ensure_fixtures_dir()
    print(f"Capturing Northern Card Gaming fixtures into {FIXTURES_DIR}")
    print(
        f"Base: {NCG_BASE_URL}  minor_unit_exponent={MINOR_UNIT_EXPONENT}  "
        f"total request cap={MAX_TOTAL_REQUESTS}\n"
    )

    budget = RequestBudget(MAX_TOTAL_REQUESTS)
    heartbeat = HeartbeatRegistry()
    host_gate = HostGate()  # default ≥1s spacing, one in-flight per host (Req 1.4)
    saved: list[Path] = []
    all_products: list[dict[str, Any]] = []

    async with HttpClient(heartbeat=heartbeat, host_gate=host_gate) as client:
        try:
            for name, feed_path in FEEDS:
                print(f"Feed {feed_path}:")
                await _capture_feed_pages(
                    client, budget, name, feed_path, saved, all_products
                )

            candidates = _unique_candidates(all_products)
            _print_candidate_table(candidates)

            handles = _pick_candidate_handles(candidates, budget)
            if handles:
                print(f"\nFetching {len(handles)} candidate product(s) as .js:")
                await _capture_products(client, budget, handles, saved)
        except StopCapture:
            # Budget exhausted mid-run: not an error, just a polite stop. Still print
            # whatever we gathered so the operator gets a partial candidate table.
            candidates = _unique_candidates(all_products)
            _print_candidate_table(candidates)
        except Blocked as exc:
            # SoftBlock is a Blocked subclass, so this covers both (Req 16.3).
            kind = type(exc).__name__
            print(
                f"\n! {kind}: the retailer is pushing back ({exc}). Stopping immediately "
                f"without further requests.",
                file=sys.stderr,
            )
            return 2
        except FetchError as exc:
            print(f"\n! Fetch failed ({exc}). Stopping.", file=sys.stderr)
            return 1

    _report_summary(saved)
    return 0


def _report_summary(saved: list[Path]) -> None:
    """Print the final list of saved fixtures and the synthetic soft-block reminder."""
    print(f"\nSaved {len(saved)} fixture file(s):")
    for path in saved:
        print(f"  {path}")
    soft_block = FIXTURES_DIR / "soft_block.html"
    note = "present" if soft_block.exists() else "MISSING — add it by hand"
    print(
        f"\nSynthetic soft-block sample: {soft_block} ({note}). "
        "It is hand-written, NOT captured from NCG."
    )
    print("\nNothing was written to config/config.yaml (operator approves that in task 11).")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
