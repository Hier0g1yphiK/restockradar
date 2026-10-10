"""Resolve matching enabled + healthy destinations into one ``alert_deliveries`` row
each, inside the same transaction as the state change (Req 6.2, 8.1, 10.10).
"""
