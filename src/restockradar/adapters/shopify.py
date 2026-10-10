"""``ShopifyAdapter``: ``/products/{handle}.js`` single-listing fetch (404 =>
NOT_FOUND state; explicit same-host 301/302/307/308 redirect resolution) and
``/collections/{c}/products.json`` + ``/products.json`` discovery (Req 11, 12).

The adapter is the ONLY retailer-specific code in the system (Req 11.1/11.3): the core
speaks only in :mod:`restockradar.adapters.base` value types. Everything here is wired
through the shared :class:`~restockradar.http.client.HttpClient`, so the politeness,
robots.txt, ethics, and per-request heartbeat rules cannot be bypassed (Req 1, 15, 16,
19.7/19.8). The HTTP client is built with ``follow_redirects=False``, so a 3xx is
returned to the adapter for the explicit, opportunistic rename handling below rather
than silently followed.

Two paths:

* **discovery** (:meth:`ShopifyAdapter.discover`): paginate a feed with ``limit=250`` and
  an incrementing ``page``, stop on a short/empty page, raise
  :class:`~restockradar.adapters.errors.TruncatedFeed` at ``max_page_count``. The URL
  never carries a ``sort_by`` or a ``+``-tag filter (Req 15.6). One client request is
  issued per page, and the client beats the in-memory scheduler heartbeat on every
  request, so the beat happens per page fetched (per-page liveness, Req 19.7/19.8).
* **single-listing fetch** (:meth:`ShopifyAdapter.fetch_state`): ``GET
  {base}/products/{handle}.js`` (NOT ``.json`` — the ``.json`` form omits the per-variant
  ``available`` flag, Req 11.4). Prices on ``.js`` are integer minor units converted to a
  major-unit :class:`~decimal.Decimal` via the retailer's ``minor_unit_exponent``; a
  feed's decimal string is parsed directly as major units (Req 11.5). A 404 is RETURNED
  as ``ProductState(existence=NOT_FOUND)``, never raised (Req 7.1/7.2); a same-host
  ``.js`` redirect or this cycle's feed data can resolve a renamed handle (Req 11.8).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Final, Protocol, runtime_checkable
from urllib.parse import urljoin, urlparse

import httpx

from restockradar.adapters.base import (
    DiscoveredProduct,
    Existence,
    FeedLike,
    ListingLike,
    ProductState,
    VariantState,
)
from restockradar.adapters.errors import MalformedResponse, SoftBlock, TruncatedFeed
from restockradar.http.client import HttpClient

__all__ = ["ShopifyAdapter", "shopify_factory"]

#: Shopify discovery pagination page size (Req 12.5).
_PAGE_LIMIT: Final = 250

#: 3xx codes the adapter resolves explicitly on a ``.js`` request (Req 11.8). The HTTP
#: client never follows them (``follow_redirects=False``); the adapter decides.
_REDIRECT_CODES: Final = frozenset({301, 302, 307, 308})

#: Content-type fragments that indicate an HTML body (soft-block detection, Req 16.3).
_HTML_CONTENT_TYPES: Final = ("text/html", "application/xhtml")


@runtime_checkable
class RetailerLike(Protocol):
    """The retailer attributes the Shopify adapter reads.

    Declared structurally so the adapter need not import the concrete ``domain``
    ``Retailer`` dataclass; any object carrying these attributes satisfies the
    contract. ``base_url`` is the retailer origin (e.g.
    ``https://www.northerncardgaming.com``) and ``minor_unit_exponent`` converts a
    ``.js`` integer price to a major-unit :class:`~decimal.Decimal` (default 2, Req 11.5).
    """

    base_url: str
    minor_unit_exponent: int


class ShopifyAdapter:
    """Shopify ``adapter_type`` implementation (Req 11, 12).

    Holds the shared :class:`~restockradar.http.client.HttpClient` and the
    :class:`RetailerLike` it serves (the ``minor_unit_exponent`` and ``base_url`` are
    read from the retailer, not from a listing, so a listing carries only its own
    url/handle/external_id — Req 11.5/11.7).
    """

    adapter_type: str = "shopify"

    __slots__ = ("_base", "_http", "_retailer")

    def __init__(self, http: HttpClient, retailer: RetailerLike) -> None:
        self._http = http
        self._retailer = retailer
        # Normalise away a trailing slash so f"{base}{path}" never doubles it.
        self._base = retailer.base_url.rstrip("/")

    # -- path (a): discovery -----------------------------------------------------

    async def discover(self, feed: FeedLike) -> list[DiscoveredProduct]:
        """Enumerate products on a discovery feed (Req 12.5/12.6, 11.10).

        Paginates ``{base}{feed.feed_url}?limit=250&page={n}`` from page 1, stopping on a
        page that returns fewer than ``limit`` products (short or empty page). The URL
        carries only ``limit`` and ``page`` — never a ``sort_by`` or a ``+``-tag filter
        (Req 15.6). One client request per page means the client's per-request heartbeat
        beat lands once per page fetched (per-page liveness, Req 19.7/19.8). Reaching
        ``max_page_count`` without a short page raises
        :class:`~restockradar.adapters.errors.TruncatedFeed` (Req 12.6, 2.5).
        """
        results: list[DiscoveredProduct] = []
        for page in range(1, feed.max_page_count + 1):
            url = f"{self._base}{feed.feed_url}?limit={_PAGE_LIMIT}&page={page}"
            body = await self._http.get_json(url)
            products = self._products_array(body, url)
            for raw in products:
                results.append(self._to_discovered(raw))
            if len(products) < _PAGE_LIMIT:
                # A short or empty page means the feed is exhausted (Req 12.6).
                break
        else:
            # The for-loop ran to max_page_count without a short page: truncated crawl.
            raise TruncatedFeed(
                f"discovery feed {feed.feed_url!r} did not terminate within "
                f"{feed.max_page_count} pages"
            )
        return results

    def _products_array(self, body: Any, url: str) -> list[dict[str, Any]]:
        """Extract the ``products`` list from a feed body, rejecting a wrong shape."""
        if not isinstance(body, Mapping):
            raise MalformedResponse(f"feed {url!r} returned a non-object JSON body")
        products = body.get("products", [])
        if not isinstance(products, list):
            raise MalformedResponse(f"feed {url!r} 'products' was not a list")
        return products

    def _to_discovered(self, raw: Mapping[str, Any]) -> DiscoveredProduct:
        """Build a :class:`DiscoveredProduct` from one feed ``products[]`` entry.

        ``external_id`` is ``str(id)`` (Req 14.7); the image is normalised (Req 8.11);
        variants carry the feed's ``available`` flag and decimal-string prices parsed as
        major units (Req 11.5). ``tags`` on a feed may be a list or a comma string.
        """
        handle = str(raw.get("handle", ""))
        return DiscoveredProduct(
            external_id=str(raw["id"]),
            title=str(raw.get("title", "")),
            product_type=_opt_str(raw.get("product_type")),
            vendor=_opt_str(raw.get("vendor")),
            tags=_coerce_tags(raw.get("tags")),
            handle=handle,
            url=f"{self._base}/products/{handle}",
            image=self._normalise_image(_feed_image(raw)),
            variants=self._feed_variants(raw.get("variants")),
        )

    def _feed_variants(self, raw_variants: Any) -> tuple[VariantState, ...] | None:
        """Map a feed's ``variants[]`` to :class:`VariantState`, prices as major units.

        A feed's variant ``price`` is a decimal string (e.g. ``"49.99"``) parsed directly
        as major units (Req 11.5). Returns ``None`` when the feed carries no usable
        variant array, so feed-reuse (Req 11.6) only triggers when full variant data is
        present.
        """
        if not isinstance(raw_variants, list):
            return None
        variants: list[VariantState] = []
        for v in raw_variants:
            if not isinstance(v, Mapping):
                return None
            variants.append(
                VariantState(
                    variant_id=str(v["id"]),
                    available=bool(v.get("available", False)),
                    price=_decimal_major(v["price"]),
                )
            )
        return tuple(variants)

    # -- path (b): single-listing fetch -----------------------------------------

    async def fetch_state(
        self, listing: ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        """Fetch one listing's live state (Req 11.4-11.6, 7.1, 11.8).

        Feed-reuse first (Req 11.6): if this cycle's discovery already carries this
        listing's ``external_id`` with full variant data, build the state from it and
        issue no ``.js`` request. Otherwise ``GET {base}/products/{handle}.js``:

        * a 301/302/307/308 is resolved explicitly, and only a same-host ``.js`` form
          whose product id equals the listing's ``external_id`` is trusted as a rename
          (Req 11.8); any other redirect falls through to the 404/feed path;
        * a 404 is RETURNED as ``ProductState(existence=NOT_FOUND)`` after feed-based
          rename resolution has failed (Req 7.1/7.2, 11.8);
        * a 2xx body is parsed as JSON (HTML-where-JSON is a
          :class:`~restockradar.adapters.errors.SoftBlock`, Req 16.3) into an EXISTING
          state.
        """
        cached = cycle_products.get(listing.external_id or "")
        if cached is not None and cached.variants is not None:
            # Feed-reuse within this cycle (Req 11.6): no new request.
            return self._state_from_feed(cached, listing)

        url = f"{self._base}/products/{listing.handle}.js"
        resp = await self._http.get(url)
        if resp.status_code in _REDIRECT_CODES:
            resolved = await self._resolve_js_redirect(listing, resp)
            if resolved is not None:
                return resolved
            # Redirect unusable as a rename -> fall through to feed / NOT_FOUND.
            return await self._handle_404(listing, cycle_products)
        if resp.status_code == 404:
            return await self._handle_404(listing, cycle_products)
        body = self._require_json(resp, url)
        return self._state_from_js(body, listing)

    async def _resolve_js_redirect(
        self, listing: ListingLike, resp: httpx.Response
    ) -> ProductState | None:
        """Resolve a ``.js`` redirect as a rename, or return ``None`` to fall through.

        Shopify does NOT reliably 301 a stale ``.js`` path, so a redirect is treated as
        an opportunistic signal only (Req 11.8). Requires, in order: the listing has a
        known ``external_id``; a ``Location`` header; a SAME-HOST target (compared via
        :func:`urllib.parse.urlparse` ``.hostname``); the target PATH requested with a
        ``.js`` suffix returns HTTP 200 JSON; and that body's ``id`` equals the listing's
        ``external_id``. Any failure returns ``None``.
        """
        if not listing.external_id:
            return None
        location = resp.headers.get("location")
        if not location:
            return None
        target = urljoin(self._base + "/", location)
        if urlparse(target).hostname != urlparse(self._base).hostname:
            return None  # cross-host redirect: never trusted (Req 11.8)
        # Request the redirect target's PATH with an explicit .js suffix — do not assume
        # the Location itself is already a .js URL.
        target_path = urlparse(target).path.removesuffix(".js").removesuffix("/")
        js_url = f"{self._base}{target_path}.js"
        redirected = await self._http.get(js_url)
        if redirected.status_code != 200:
            return None
        body = self._require_json(redirected, js_url)
        if str(body.get("id")) != listing.external_id:
            return None  # id mismatch: not this listing's rename
        return self._state_from_js(body, listing, resolved_handle=str(body["handle"]))

    async def _handle_404(
        self, listing: ListingLike, cycle_products: Mapping[str, DiscoveredProduct]
    ) -> ProductState:
        """Resolve a plain 404 via this cycle's feed, else report NOT_FOUND (Req 7.1/11.8).

        If the listing's ``external_id`` appears in this cycle's discovery data under a
        (possibly new) handle, build EXISTING state from it — reusing full feed variant
        data when present, else re-fetching that handle's ``.js``. Otherwise return a
        genuine ``ProductState(existence=NOT_FOUND)``; the watcher applies the
        2-consecutive-404 existence rule to it (Req 7.6).
        """
        external_id = listing.external_id
        if external_id:
            found = cycle_products.get(external_id)
            if found is not None:
                if found.variants is not None:
                    return self._state_from_feed(found, listing)
                refetch_url = f"{self._base}/products/{found.handle}.js"
                refetched = await self._http.get(refetch_url)
                if refetched.status_code == 200:
                    body = self._require_json(refetched, refetch_url)
                    return self._state_from_js(body, listing, resolved_handle=found.handle)
        return ProductState(
            existence=Existence.NOT_FOUND,
            in_stock=False,
            price=None,
            variants=(),
            url=listing.url,
            image=None,
            title=None,
            external_id=external_id,
        )

    # -- state builders ----------------------------------------------------------

    def _state_from_js(
        self,
        body: Mapping[str, Any],
        listing: ListingLike,
        resolved_handle: str | None = None,
    ) -> ProductState:
        """Build EXISTING state from a ``.js`` body (prices are integer minor units).

        ``resolved_handle``/``resolved_url`` are set ONLY when the fetched/resolved handle
        DIFFERS from the listing's stored handle (a rename, Req 11.8); an equal handle
        sets neither and the watcher queues no admin notice.
        """
        variants = tuple(
            VariantState(
                variant_id=str(v["id"]),
                available=bool(v.get("available", False)),
                price=self._minor_to_decimal(v["price"]),
            )
            for v in self._js_variants(body)
        )
        new_handle = resolved_handle if resolved_handle is not None else str(body["handle"])
        url = f"{self._base}/products/{new_handle}"
        is_rename = new_handle != listing.handle
        return ProductState(
            existence=Existence.EXISTING,
            in_stock=any(v.available for v in variants),
            price=self._lowest_price(variants),
            variants=variants,
            url=url,
            image=self._normalise_image(_js_image(body)),
            title=_opt_str(body.get("title")),
            external_id=str(body["id"]),
            resolved_handle=new_handle if is_rename else None,
            resolved_url=url if is_rename else None,
        )

    def _state_from_feed(self, found: DiscoveredProduct, listing: ListingLike) -> ProductState:
        """Build EXISTING state from this cycle's feed data (Req 11.6, prices already
        major units).

        ``resolved_handle``/``resolved_url`` are set ONLY when the feed handle differs
        from the listing's stored handle; the equal-handle case is ordinary feed-reuse
        and queues no admin notice (Req 11.8).
        """
        variants = tuple(found.variants or ())
        is_rename = found.handle != listing.handle
        return ProductState(
            existence=Existence.EXISTING,
            in_stock=any(v.available for v in variants),
            price=self._lowest_price(variants),
            variants=variants,
            url=found.url,
            image=self._normalise_image(found.image),
            title=found.title,
            external_id=found.external_id,
            resolved_handle=found.handle if is_rename else None,
            resolved_url=found.url if is_rename else None,
        )

    def _js_variants(self, body: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Return the ``.js`` body's ``variants`` list, rejecting a wrong shape."""
        variants = body.get("variants")
        if not isinstance(variants, list):
            raise MalformedResponse("product .js body had no 'variants' list")
        return variants

    # -- price + image normalisation --------------------------------------------

    def _minor_to_decimal(self, minor: Any) -> Decimal:
        """Convert an integer minor-unit ``.js`` price to a major-unit Decimal (Req 11.5)."""
        return Decimal(int(minor)) / (Decimal(10) ** self._retailer.minor_unit_exponent)

    @staticmethod
    def _lowest_price(variants: Sequence[VariantState]) -> Decimal | None:
        """Lowest AVAILABLE variant price, else lowest overall, else ``None`` (Req 5.2)."""
        available = [v.price for v in variants if v.available]
        if available:
            return min(available)
        all_prices = [v.price for v in variants]
        return min(all_prices) if all_prices else None

    def _normalise_image(self, raw: str | None) -> str | None:
        """Normalise a Shopify image URL to an absolute http(s) URL, or ``None`` (Req 8.11).

        A leading ``//`` (protocol-relative) becomes ``https:`` + url; a relative path is
        joined against the retailer base URL; anything whose scheme is still not
        ``http``/``https`` is dropped (returned as ``None``) so the embed omits it.
        """
        if not raw:
            return None
        if raw.startswith("//"):
            raw = "https:" + raw
        elif raw.startswith("/") or not urlparse(raw).scheme:
            raw = urljoin(self._base + "/", raw)
        return raw if urlparse(raw).scheme in ("http", "https") else None

    # -- response JSON parsing (soft-block aware) --------------------------------

    def _require_json(self, response: httpx.Response, url: str) -> dict[str, Any]:
        """Parse a 2xx response body as a JSON object, detecting the HTML soft-block.

        The shared client issues the request and already classified Blocked/5xx; here the
        adapter parses an in-hand 200 body: an HTML body where JSON was expected is a
        :class:`~restockradar.adapters.errors.SoftBlock` (Req 16.3); an unparseable or
        non-object body is a :class:`~restockradar.adapters.errors.MalformedResponse`.
        """
        if _looks_like_html(response):
            raise SoftBlock(f"expected JSON from {url!r} but received an HTML body")
        try:
            parsed = json.loads(response.content)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            if _body_starts_like_html(response):
                raise SoftBlock(f"expected JSON from {url!r} but received an HTML body") from exc
            raise MalformedResponse(f"could not parse JSON from {url!r}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise MalformedResponse(f"expected a JSON object from {url!r}")
        return parsed


# ---------------------------------------------------------------------------
# Registry factory
# ---------------------------------------------------------------------------


def shopify_factory(http: HttpClient) -> Callable[[RetailerLike], ShopifyAdapter]:
    """Return an :data:`~restockradar.adapters.registry.AdapterFactory` bound to ``http``.

    The registry's factory takes only the retailer; the shared HTTP client is captured
    here so a registration reads ``registry.register("shopify", shopify_factory(http))``.
    """

    def _build(retailer: RetailerLike) -> ShopifyAdapter:
        return ShopifyAdapter(http, retailer)

    return _build


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _opt_str(value: Any) -> str | None:
    """Return ``str(value)`` for a non-empty value, else ``None``."""
    if value is None or value == "":
        return None
    return str(value)


def _decimal_major(value: Any) -> Decimal:
    """Parse a feed price (decimal string or number) as major units (Req 11.5)."""
    return Decimal(str(value))


def _coerce_tags(raw: Any) -> tuple[str, ...]:
    """Coerce a Shopify ``tags`` field (list or comma-separated string) to a tuple."""
    if isinstance(raw, str):
        return tuple(t.strip() for t in raw.split(",") if t.strip())
    if isinstance(raw, list):
        return tuple(str(t) for t in raw)
    return ()


def _feed_image(raw: Mapping[str, Any]) -> str | None:
    """Extract a feed product's image URL from ``featured_image`` or ``images[0]``.

    A feed ``featured_image`` may be a URL string or an object with a ``src``; some feeds
    only populate ``images[]``. Returns the first URL found, else ``None`` (normalisation
    happens in :meth:`ShopifyAdapter._normalise_image`).
    """
    featured = raw.get("featured_image")
    url = _image_src(featured)
    if url is not None:
        return url
    images = raw.get("images")
    if isinstance(images, list) and images:
        return _image_src(images[0])
    return None


def _js_image(body: Mapping[str, Any]) -> str | None:
    """Extract a ``.js`` product's image URL from ``featured_image`` or ``images[0]``."""
    url = _image_src(body.get("featured_image"))
    if url is not None:
        return url
    images = body.get("images")
    if isinstance(images, list) and images:
        return _image_src(images[0])
    return None


def _image_src(value: Any) -> str | None:
    """Pull a URL string out of a Shopify image field (string or ``{"src": ...}``)."""
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping):
        src = value.get("src")
        if isinstance(src, str) and src:
            return src
    return None


def _content_type(response: httpx.Response) -> str:
    """Return the lower-cased ``Content-Type`` of ``response`` (header part only)."""
    raw = str(response.headers.get("content-type", ""))
    return raw.split(";", 1)[0].strip().casefold()


def _looks_like_html(response: httpx.Response) -> bool:
    """True when the response is HTML by content-type or body shape (soft-block)."""
    ctype = _content_type(response)
    if any(ctype.startswith(h) for h in _HTML_CONTENT_TYPES):
        return True
    return _body_starts_like_html(response)


def _body_starts_like_html(response: httpx.Response) -> bool:
    """True when the body's opening bytes look like an HTML document."""
    try:
        head = response.text[:512].lstrip().casefold()
    except (UnicodeDecodeError, ValueError):
        return False
    return head.startswith(("<!doctype html", "<html", "<head", "<body", "<!--"))
