"""``Backoff_State``: ordinary vs Blocked exponential backoff starting at the entity
poll interval, doubling, capped at 1h, honouring Retry-After for Blocked; per
retailer/feed/listing counters; reset on success (Req 16).
"""
