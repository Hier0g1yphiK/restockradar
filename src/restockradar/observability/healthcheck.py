"""Container healthcheck probe: reads the ``heartbeat`` TABLE (scheduler + dispatch
rows, written <=5s) plus DB writability, against fixed thresholds (sched 120s,
dispatch 60s). This is NOT what the watchdog reads (Req 19.6).
"""
