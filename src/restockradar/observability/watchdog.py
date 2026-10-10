"""Dedicated daemon thread: compares the in-memory monotonic heartbeats
(``time.monotonic()``) against fixed RR_ thresholds (sched 120s, dispatch 60s) after a
120s startup grace; on staleness logs and calls ``os._exit(1)`` so the restart policy
restarts. Never reads the database. Not started in ``--once`` mode (Req 19.6, 19.10).
"""
