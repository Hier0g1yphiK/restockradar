"""Adapter error classes: ``FetchError``, ``Blocked``, ``SoftBlock``,
``MalformedResponse``, ``TruncatedFeed`` and the registry's ``UnknownAdapterType``.

There is deliberately no ``NotFound`` class — a 404 on a product endpoint is a RETURNED
:class:`~restockradar.adapters.base.ProductState` with ``existence=NOT_FOUND``, not an
error (Req 11.3, 7.1).

The hierarchy mirrors the response classification the HTTP client performs (Req 16):

* :class:`FetchError` — the base for every *genuine* fetch failure. A plain 5xx, a
  request timeout, a network error, or malformed JSON are errors (not blocking).
* :class:`Blocked` — active blocking: HTTP 403/429, a 503 carrying a challenge
  indicator, or a detected captcha/challenge page (Req 16, Blocked definition).
* :class:`SoftBlock` — a *soft* block: a 2xx response whose body is HTML where a JSON
  feed was expected (Req 16.3). It is a kind of :class:`Blocked`.
* :class:`MalformedResponse` — a response that parsed as the wrong shape / unparseable
  JSON; a genuine fetch failure, so it extends :class:`FetchError`.
* :class:`TruncatedFeed` — a discovery crawl that reached ``max_page_count`` without
  exhausting the feed (Req 12.6); a feed-level failure, so it extends
  :class:`FetchError`.
"""

from __future__ import annotations


class FetchError(Exception):
    """Base class for a genuine adapter fetch failure (Req 16).

    Plain 5xx responses, request timeouts, network errors, and malformed JSON are all
    fetch errors — distinct from blocking, which is signalled by :class:`Blocked`.
    """


class Blocked(FetchError):
    """The retailer actively blocked the request (Req 16, Blocked definition).

    Covers HTTP 403, HTTP 429, a 503 carrying a challenge indicator, and detected
    captcha/challenge pages. The client classifies this and backs off; it never
    attempts to solve or bypass the challenge.
    """

    def __init__(self, message: str = "", *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class SoftBlock(Blocked):
    """A soft block: a 2xx HTML body where a JSON feed was expected (Req 16.3)."""


class MalformedResponse(FetchError):
    """A response whose body could not be parsed into the expected shape."""


class TruncatedFeed(FetchError):
    """A discovery crawl reached ``max_page_count`` without exhausting the feed (Req 12.6)."""


class UnknownAdapterType(Exception):
    """No adapter is registered for the retailer's ``adapter_type`` (Req 11.9, 13.6).

    Raised by :meth:`~restockradar.adapters.registry.AdapterRegistry.resolve` so config
    validation fails fast on an unknown adapter type.
    """

    def __init__(self, adapter_type: str) -> None:
        super().__init__(f"no adapter registered for adapter_type {adapter_type!r}")
        self.adapter_type = adapter_type


__all__ = [
    "Blocked",
    "FetchError",
    "MalformedResponse",
    "SoftBlock",
    "TruncatedFeed",
    "UnknownAdapterType",
]
