"""Single-flight dispatch loop (<=5s): one worker task per destination bounded by a
global concurrency semaphore (default 10); lease (60s), POST, retry, expire, mark
terminal; set ``outbox.terminal`` when all deliveries are terminal; triggers the
per-scope_key hourly Admin_Digest when a scope's window has elapsed (Req 8, 10, 17).
"""
