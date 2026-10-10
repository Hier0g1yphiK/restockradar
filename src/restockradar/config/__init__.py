"""Config loading (Req 13, 18): reads YAML, validates with pydantic (fail-fast,
all-errors-in-one-pass), upserts by stable id / ``config_key``, enforces the
``config`` vs ``bot`` Source ownership rule, and clears ``seeded_at`` on re-enable.
Secrets never live here; destinations store only the env-var name.
"""
