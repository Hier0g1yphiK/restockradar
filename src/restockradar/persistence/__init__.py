"""Repository layer and migrations (Req 14, 6). Owns the schema, the migration
runner, the type adapters (Decimal/bool/UTC), and the single ``BEGIN IMMEDIATE``
transaction helper every mutating path funnels through. All ``sqlite3`` work runs
on one dedicated single-thread executor that owns the connection; the module
exposes ``async`` methods so blocking SQLite is never called on the event loop.
"""
