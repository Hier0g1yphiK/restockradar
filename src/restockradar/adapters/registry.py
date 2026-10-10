"""``AdapterRegistry``: maps an adapter-type string to a factory. Registers only
``shopify``, resolves by type, and raises
:class:`~restockradar.adapters.errors.UnknownAdapterType` for anything else
(Req 11.9, 22.2).

The registry is the one indirection the retailer-agnostic core uses to obtain an
:class:`~restockradar.adapters.base.Adapter` for a retailer, so the core never imports a
concrete adapter module. The MVP registers only ``"shopify"``; Phase 2 adapter types
(sitemap, generic-HTML, custom) are intentionally absent (Req 22.2).

The registry stays decoupled from the still-in-flight ``domain.models`` and ``http``
modules: a retailer is described structurally by :class:`RetailerLike` (it only needs an
``adapter_type``), and a factory is any callable that returns an
:class:`~restockradar.adapters.base.Adapter`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from .base import Adapter
from .errors import UnknownAdapterType

AdapterFactory = Callable[["RetailerLike"], Adapter]
"""A factory that builds an :class:`Adapter` for a given retailer.

Dependencies a concrete factory needs (e.g. the shared HTTP client) are captured when
the factory is registered — typically via a closure or ``functools.partial`` — so the
registry itself stays dependency-free.
"""


@runtime_checkable
class RetailerLike(Protocol):
    """The single retailer attribute the registry reads to pick an adapter."""

    adapter_type: str


class AdapterRegistry:
    """Maps an ``adapter_type`` string to the factory that builds its adapter."""

    def __init__(self) -> None:
        self._factories: dict[str, AdapterFactory] = {}

    def register(self, adapter_type: str, factory: AdapterFactory) -> None:
        """Register ``factory`` as the builder for ``adapter_type``.

        Re-registering an ``adapter_type`` replaces the previous factory.
        """
        self._factories[adapter_type] = factory

    def is_registered(self, adapter_type: str) -> bool:
        """Return whether a factory is registered for ``adapter_type``."""
        return adapter_type in self._factories

    def resolve(self, retailer: RetailerLike) -> Adapter:
        """Build the adapter for ``retailer`` by its ``adapter_type``.

        Raises :class:`UnknownAdapterType` when no factory is registered — so config
        validation fails fast on an unknown adapter type (Req 11.9, 13.6).
        """
        try:
            factory = self._factories[retailer.adapter_type]
        except KeyError:
            raise UnknownAdapterType(retailer.adapter_type) from None
        return factory(retailer)


__all__ = ["AdapterFactory", "AdapterRegistry", "RetailerLike"]
