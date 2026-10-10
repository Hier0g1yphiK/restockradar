"""restockradar — Stock & Drop Monitor.

Polls retailer product feeds (Shopify first) and emits Discord webhook alerts for
restocks, price drops, newly listed products, and pages going live. Python 3.12,
fully type-annotated. The core never imports from ``adapters/`` except through the
``Adapter`` protocol and the registry (Req 11, 13).
"""
