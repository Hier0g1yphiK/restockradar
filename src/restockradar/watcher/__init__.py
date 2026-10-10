"""Watcher / scheduler core (Req 1, 2, 12). Decides what to poll and when
(fixed-tick, due-ordered selection, feeds before listings), calls the adapter, and
hands results to ``detect/``. The watcher — not the adapter — writes a resolved
handle/url change and queues the handle-change admin notice.
"""
