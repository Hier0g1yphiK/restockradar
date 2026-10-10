"""Notifier / outbox dispatch + Discord (Req 8, 10, 17). The async side: durable
at-least-once delivery with a 60s ``sending`` lease and a 45s per-delivery in-request
budget, one worker task per destination bounded by a global dispatch concurrency
semaphore (default 10), retries, destination health and probe scheduling, and admin
warnings/digests. A warning *about* a destination is never routed *through* it.
"""
