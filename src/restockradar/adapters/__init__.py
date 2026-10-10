"""Adapter interface, registry, and the Shopify adapter (Req 11, 12). The only place
retailer-specific code lives: ``base.py`` defines the contract and the core depends
only on ``base.py`` and ``registry.py``. A 404 on a product endpoint is *returned* as
a ``ProductState`` with ``existence = NOT_FOUND``, never raised (Req 7.1).
"""
