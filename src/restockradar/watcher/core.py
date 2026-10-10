"""Per-retailer tick: retailer backoff gate, due-ordered entity selection (feeds
before listings) processed sequentially under a 5-minute per-poll cap, fetch ->
``BEGIN IMMEDIATE`` -> read -> diff -> write -> commit, ``cycle_products`` per run,
and the watcher-level handle-rename write path (Req 1, 2, 3, 4, 5, 6, 7, 15).
"""
