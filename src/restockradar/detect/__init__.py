"""Diffing / event detection (Req 3, 4, 5, 7, 9). Pure-ish diffing: given stored
state + fetched state + clock, it decides which events fire and builds the write-set.
This is the main property-tested surface.
"""
