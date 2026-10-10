"""Webhook POST with in-request retry + Retry-After under a 45s total budget; beats
the dispatch heartbeat at every await boundary (before POST, after response, before/
after each retry sleep); plus the GET probe (Req 10.3, 10.11).
"""
