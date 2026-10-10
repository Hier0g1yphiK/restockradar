"""``AsyncIOScheduler`` wiring: one job per retailer ticking at fixed ``tick_s`` with
``coalesce=True`` and ``max_instances=1`` (staggered initial ticks), plus one
independent interval job (every ``tick_s``) that beats the scheduler heartbeat
regardless of any retailer job (Req 1.5, 1.9, 1.10, 19.7).
"""
