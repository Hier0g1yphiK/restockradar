-- 0001_initial.sql — initial schema for the Stock & Drop Monitor (Req 14.1–14.15, 14.23).
--
-- Forward-only, numbered migration (see design "Data Models / Migration approach").
-- Applied statement-by-statement by persistence/migrate.py under one held
-- BEGIN IMMEDIATE transaction (never executescript), with the schema_migrations
-- version row inserted in the same transaction so the file is all-or-nothing.
--
-- Conventions (Req 14.15, 14.18): prices are TEXT holding a canonical Decimal
-- string (e.g. '12.99'); timestamps are TEXT ISO-8601 UTC
-- ('2025-01-02T03:04:05.000000+00:00'); booleans are INTEGER (0/1). All foreign
-- keys are declared and PRAGMA foreign_keys=ON is set per connection.

-- ─────────────────────────── migration bookkeeping ───────────────────────────
-- The runner (persistence/migrate.py) bootstraps this table with CREATE TABLE IF NOT
-- EXISTS before taking the migration lock, so this statement uses IF NOT EXISTS too:
-- the table's definition still lives in the migration (the authoritative schema), but
-- re-declaring it when the bootstrap already created it is a harmless no-op.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT    NOT NULL          -- ISO-8601 UTC
);

-- ─────────────────────────── configurable catalogue ──────────────────────────
CREATE TABLE games (
    id          TEXT PRIMARY KEY,         -- stable id, e.g. 'pokemon'
    name        TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'config'   -- 'config' | 'bot'  (Req 13.3)
                CHECK (source IN ('config','bot')),
    enabled     INTEGER NOT NULL DEFAULT 1       -- Enabled_Flag (Req 13.4)
);

CREATE TABLE products (
    id          TEXT PRIMARY KEY,
    game_id     TEXT NOT NULL REFERENCES games(id),
    name        TEXT NOT NULL,
    category    TEXT,                            -- category or set (Req 14.2)
    source      TEXT NOT NULL DEFAULT 'config' CHECK (source IN ('config','bot')),
    enabled     INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE retailers (
    id                   TEXT PRIMARY KEY,       -- e.g. 'northern-card-gaming'
    name                 TEXT NOT NULL,
    adapter_type         TEXT NOT NULL,          -- resolved by Adapter_Registry (Req 11.3)
    base_url             TEXT NOT NULL,
    poll_interval_s      INTEGER NOT NULL DEFAULT 60,   -- Req 15.1 (floored at 30, Req 15.2); the
                                                        -- default interval inherited by this retailer's
                                                        -- feeds/listings, NOT a retailer-level due time
    minor_unit_exponent  INTEGER NOT NULL DEFAULT 2,    -- Req 11.5
    -- NOTE: retailer-level scheduling has no next_due_at/backoff_until columns. Retailer-wide deferral
    -- is driven ENTIRELY by the retailer's backoff_state row (scope_key = 'retailer:<id>'); the only
    -- reason to defer a whole retailer is retailer-wide backoff (a Blocked retailer, Req 1.5/16.5).
    source               TEXT NOT NULL DEFAULT 'config' CHECK (source IN ('config','bot')),
    enabled              INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE listings (
    id                   INTEGER PRIMARY KEY,
    config_key           TEXT NOT NULL,          -- stable upsert / bot-collision identity (Req 13.2/13.6)
    product_id           TEXT NOT NULL REFERENCES products(id),
    retailer_id          TEXT NOT NULL REFERENCES retailers(id),
    url                  TEXT NOT NULL,
    handle               TEXT NOT NULL,
    external_id          TEXT,                   -- Shopify product id as string (Req 11.7, 14.7)
    -- last known state (Req 14.4):
    in_stock             INTEGER,                -- last OBSERVED stock; NULL until seeded (Req 2.3)
    price                TEXT,                   -- last OBSERVED price (Decimal string), NULL until seeded;
                                                 -- NOT the last_alerted_price (that is alert_state.last_alerted_value)
    existence            INTEGER,                -- 1 existing / 0 not-found / NULL unseeded (Req 7.3)
    consecutive_404      INTEGER NOT NULL DEFAULT 0,    -- Req 7.6
    poll_interval_s      INTEGER NOT NULL DEFAULT 60,
    cooldown_s           INTEGER,                -- per-subject cooldown override, NULL => default 300 (Req 9.7)
    next_due_at          TEXT,
    seeded_at            TEXT,                   -- set only on a complete poll (Req 2.1, 2.4)
    source               TEXT NOT NULL DEFAULT 'config' CHECK (source IN ('config','bot')),
    enabled              INTEGER NOT NULL DEFAULT 1,
    UNIQUE (config_key),                         -- loader upserts by config_key (Req 13.2/13.6)
    UNIQUE (retailer_id, product_id)             -- Req 13.9: a product+retailer is one listing
);

CREATE TABLE discovery_feeds (
    id                   INTEGER PRIMARY KEY,
    config_key           TEXT NOT NULL,          -- stable upsert / bot-collision identity (Req 13.2/13.6)
    retailer_id          TEXT NOT NULL REFERENCES retailers(id),
    game_id              TEXT NOT NULL REFERENCES games(id),
    feed_url             TEXT NOT NULL,          -- relative path, e.g. '/collections/pokemon/products.json'
    include_keywords     TEXT NOT NULL DEFAULT '[]',   -- JSON array of strings
    exclude_keywords     TEXT NOT NULL DEFAULT '[]',   -- JSON array of strings
    max_page_count       INTEGER NOT NULL DEFAULT 20,  -- Req 12.6, 22.13
    poll_interval_s      INTEGER NOT NULL DEFAULT 60,  -- full-catalogue defaults to 900 (Req 12.3)
    next_due_at          TEXT,
    seeded_at            TEXT,
    source               TEXT NOT NULL DEFAULT 'config' CHECK (source IN ('config','bot')),
    enabled              INTEGER NOT NULL DEFAULT 1,
    UNIQUE (config_key),                         -- loader upserts by config_key (Req 13.2/13.6)
    UNIQUE (retailer_id, game_id, feed_url)
);

-- ─────────────────────────── discovery de-dup store ──────────────────────────
-- Keyed by (retailer_id, game_id, external_id) (Req 3.2, 14.6). NEVER pruned (Req 14.20).
CREATE TABLE seen_products (
    retailer_id   TEXT NOT NULL REFERENCES retailers(id),
    game_id       TEXT NOT NULL REFERENCES games(id),
    external_id   TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    title         TEXT,
    handle        TEXT,
    PRIMARY KEY (retailer_id, game_id, external_id)
);

-- ─────────────────────────── alerting pipeline ───────────────────────────────
-- One row per emitted Alert of either kind (Req 14.8).
CREATE TABLE alerts_outbox (
    id            INTEGER PRIMARY KEY,
    alert_kind    TEXT NOT NULL CHECK (alert_kind IN ('product','admin')),   -- Req 8/17
    event_type    TEXT NOT NULL,       -- 'new_product'|'restock'|'price_change'|'page_live'|'combined'
                                        -- |'admin_warning'|'admin_recovery'|'admin_digest'
    game_id       TEXT REFERENCES games(id),      -- NULL for admin alerts (Req 14.14)
    subject_kind  TEXT,                -- 'listing' | 'feed_product'
    listing_id    INTEGER REFERENCES listings(id),
    feed_id       INTEGER REFERENCES discovery_feeds(id),
    external_id   TEXT,                -- for new-product subject
    payload       TEXT NOT NULL,       -- JSON render-model (title,url,image,prices,event detail)
    created_at    TEXT NOT NULL,       -- detection time; drives max-age expiry (Req 10.7)
    terminal      INTEGER NOT NULL DEFAULT 0      -- all deliveries done/expired => prunable (Req 14.20)
);

-- One row per (alert, destination) attempt record (Req 14.9, 6.1).
CREATE TABLE alert_deliveries (
    id              INTEGER PRIMARY KEY,
    alert_id        INTEGER NOT NULL REFERENCES alerts_outbox(id) ON DELETE CASCADE,
    destination_id  INTEGER NOT NULL REFERENCES destinations(id),
    -- 'sending' is a transient in-flight lease taken BEFORE the POST (Req 10 at-least-once);
    -- terminal statuses are 'delivered', 'expired', 'failed'.
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','sending','delivered','expired','failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,     -- when the dispatch loop may next try (Req 10.5)
    lease_until     TEXT,              -- set with status='sending'; an expired lease -> back to 'pending' (counts as an attempt)
    last_error      TEXT,
    delivered_at    TEXT,
    UNIQUE (alert_id, destination_id)  -- Req 10.8: never two live attempts for same pair
);

-- Never-pruned authority for cooldown & dedupe (Req 14.10, 9.2).
-- SCOPE: alert_state holds rows ONLY for event types that carry cooldown state — 'restock' and
-- 'price_change'. One-shot events write NO alert_state row: 'new_product' dedupes via seen_products
-- (Req 9.6) and 'page_live' dedupes via the listing's stored existence (Req 7.4, 9.6). The CHECK below
-- enforces this (Req 9.6, 14.10).
-- OWNERSHIP: last_alerted_value on the 'price_change' row IS the authoritative last_alerted_price
-- (the last price actually alerted on, Req 5.1/5.5). listings.price is the LAST OBSERVED price, not
-- the last alerted price, and is never read for cooldown/dedupe. Baseline seeding and page-live upsert
-- BOTH rows for a listing (restock + price_change) in the same transaction.
-- Keyed by a single NON-NULL subject_key so the PK has no nullable columns (SQLite treats NULLs as
-- distinct in a PRIMARY KEY). subject_key examples: 'listing:12' (restock), 'listing:12' (price_change).
-- listing_id / feed_id / external_id remain as nullable informational/FK columns OUTSIDE the key.
CREATE TABLE alert_state (
    subject_key        TEXT NOT NULL,  -- e.g. 'listing:12'
    event_type         TEXT NOT NULL CHECK (event_type IN ('restock','price_change')),  -- cooldown-bearing only
    subject_kind       TEXT NOT NULL,  -- 'listing'  (informational; new-product/page-live keep no row here)
    listing_id         INTEGER REFERENCES listings(id),         -- nullable, outside the key
    feed_id            INTEGER REFERENCES discovery_feeds(id),  -- nullable, outside the key
    external_id        TEXT,           -- for new-product subject; nullable, outside the key
    last_alert_at      TEXT,           -- restock => last_restock_alert_at
    last_alerted_value TEXT,           -- price_change => last_alerted_price, the AUTHORITY (Decimal string); NOT listings.price
    pending            INTEGER NOT NULL DEFAULT 0,   -- restock => restock_pending (Req 4.3)
    PRIMARY KEY (subject_key, event_type)
);

-- Prunable append-only history; NOT an authority (Req 14.11, 9.2).
CREATE TABLE alert_history (
    id            INTEGER PRIMARY KEY,
    subject_kind  TEXT NOT NULL,
    listing_id    INTEGER REFERENCES listings(id),
    feed_id       INTEGER REFERENCES discovery_feeds(id),
    external_id   TEXT,
    event_type    TEXT NOT NULL,
    detected_at   TEXT NOT NULL,
    detail        TEXT               -- JSON snapshot (prices etc.)
);

-- Per retailer / feed / listing failure counters + backoff (Req 14.12, 16).
-- Keyed by a single NON-NULL scope_key so the PK has no nullable columns. scope_key examples:
-- 'retailer:northern-card-gaming', 'feed:3', 'listing:12'. The id columns remain nullable, outside the key.
CREATE TABLE backoff_state (
    scope_key          TEXT PRIMARY KEY,             -- e.g. 'retailer:northern-card-gaming' | 'feed:3' | 'listing:12'
    scope_kind         TEXT NOT NULL CHECK (scope_kind IN ('retailer','feed','listing')),
    retailer_id        TEXT REFERENCES retailers(id),         -- nullable, outside the key
    feed_id            INTEGER REFERENCES discovery_feeds(id),-- nullable, outside the key
    listing_id         INTEGER REFERENCES listings(id),       -- nullable, outside the key
    error_count        INTEGER NOT NULL DEFAULT 0,    -- ordinary errors (Req 16.6)
    blocked_count      INTEGER NOT NULL DEFAULT 0,    -- Blocked condition (Req 16.5)
    empty_count        INTEGER NOT NULL DEFAULT 0,    -- consecutive empty results (Req 17.2)
    backoff_until      TEXT,                          -- effective-due input (Req 1.8)
    last_outcome       TEXT                           -- 'ok'|'error'|'blocked'|'empty'
);

-- Open-then-closed health conditions (Req 14.13, 17).
-- The open-incident lookup keys on (kind, scope_key, closed_at IS NULL); scope_key is a single
-- NON-NULL TEXT key, e.g. 'retailer:northern-card-gaming', 'feed:3', 'listing:12',
-- 'path:/collections/pokemon/products.json'. The id columns remain nullable informational FKs.
CREATE TABLE incidents (
    id              INTEGER PRIMARY KEY,
    scope_key       TEXT NOT NULL,     -- e.g. 'retailer:...' | 'feed:3' | 'listing:12' | 'path:/collections/...'
    scope_kind      TEXT NOT NULL,     -- 'retailer'|'feed'|'listing'|'path'
    retailer_id     TEXT REFERENCES retailers(id),
    feed_id         INTEGER REFERENCES discovery_feeds(id),
    listing_id      INTEGER REFERENCES listings(id),
    kind            TEXT NOT NULL CHECK (kind IN ('error','empty_result','truncated_feed','robots_disallowed')),
    opened_at       TEXT NOT NULL,
    closed_at       TEXT,              -- NULL while open
    last_warning_at TEXT               -- incident-local bookkeeping only (once-per-incident); NOT the authority for
                                       -- the hourly admin-warning rate limit, which lives in admin_scope_state (Req 17.6)
);

-- Delivery targets (Req 14.14). Webhook URL is NOT stored; only the env-var name (Req 18.2).
CREATE TABLE destinations (
    id                 INTEGER PRIMARY KEY,
    config_key         TEXT NOT NULL,         -- the YAML destination `id`; upsert / bot-collision identity (Req 13.6)
    server_id          TEXT NOT NULL,         -- Discord server identifier
    game_id            TEXT REFERENCES games(id),   -- NULL allowed only when is_admin=1 (Req 14.14)
    webhook_env_var    TEXT NOT NULL,         -- name of the env var holding the URL (Req 18.2)
    role_to_ping       TEXT,                  -- optional role id (Req 8.6)
    is_admin           INTEGER NOT NULL DEFAULT 0,  -- Admin_Destination_Flag (Req 14.14)
    -- Destination_Health (Req 14.14, 10):
    health_status      TEXT NOT NULL DEFAULT 'healthy' CHECK (health_status IN ('healthy','unhealthy')),
    unhealthy_since    TEXT,
    last_error         TEXT,
    next_probe_at      TEXT,                  -- when to next GET-probe an unhealthy destination (Req 10.11)
    last_probe_at      TEXT,                  -- when it was last probed (Req 10.11)
    source             TEXT NOT NULL DEFAULT 'config' CHECK (source IN ('config','bot')),
    enabled            INTEGER NOT NULL DEFAULT 1,
    UNIQUE (config_key),                      -- loader upserts by config_key (Req 13.6)
    CHECK (is_admin = 1 OR game_id IS NOT NULL)   -- non-admin destinations require a game
);

-- Pending suppressed admin warnings awaiting the hourly Admin_Digest (Req 17.6-17.8).
-- A warning that is rate-limited is parked here; when a scope's hourly window ends the digest reads
-- all un-digested rows for that scope_key, emits one Admin_Digest alert (fanned out to ALL enabled,
-- healthy admin destinations), and stamps digested_at. The hourly rate limit AND the digest are both
-- computed PER scope_key -- there is no retailer_id column.
CREATE TABLE admin_notices (
    id            INTEGER PRIMARY KEY,
    scope_key     TEXT NOT NULL,     -- coalescing bucket: 'retailer:<id>' | 'destination:<id>' | 'system'
    kind          TEXT NOT NULL,     -- e.g. 'error'|'empty_result'|'truncated_feed'|'robots_disallowed'
                                      --      |'expired_delivery'|'permanent_webhook_failure'|'handle_change'
    message       TEXT NOT NULL,     -- human-readable warning body (already redacted, Req 18.4)
    created_at    TEXT NOT NULL,
    digested_at   TEXT               -- NULL until folded into an emitted Admin_Digest
);

-- Per-scope_key admin-warning rate-limit state (Req 17.6-17.8). This table — NOT incidents — backs the
-- hourly per-scope limit, because the 'destination:<id>' and 'system' scopes have NO incident to carry
-- a last_warning_at. One row per scope_key; NEVER pruned.
CREATE TABLE admin_scope_state (
    scope_key        TEXT PRIMARY KEY,   -- 'retailer:<id>' | 'destination:<id>' | 'system'
    last_warning_at  TEXT NOT NULL       -- ISO-8601 UTC of the last SENT warning/digest for this scope
);

-- Liveness heartbeats for the EXTERNAL CONTAINER HEALTHCHECK ONLY (Req 19.6, written per Req 19.9). A ROW per component. The
-- scheduler and dispatch loops write these rows AT MOST EVERY 5s (a throttled write derived from the
-- same points as the in-memory heartbeats, never on the hot path of every entity/request). This table
-- is NOT what the watchdog reads: the watchdog uses in-memory monotonic (time.monotonic()) heartbeats
-- and never touches the database (see Concurrency Model / Error Handling). The healthcheck reads these
-- rows with fixed thresholds (scheduler 120s, dispatch 60s) plus a DB-writability probe.
CREATE TABLE heartbeat (
    component     TEXT PRIMARY KEY CHECK (component IN ('scheduler','dispatch')),
    beat_at       TEXT NOT NULL      -- ISO-8601 UTC of the most recent (<=5s throttled) beat
);

-- ─────────────────────────── indexes for hot paths ───────────────────────────
-- Due polling: on each fine-grained tick the job scans a retailer's feeds/listings ordered by
-- effective due time (Req 1.6). next_due_at drives the ordering; backoff_until is joined from backoff_state.
CREATE INDEX idx_feeds_due     ON discovery_feeds (retailer_id, enabled, next_due_at);
CREATE INDEX idx_listings_due  ON listings        (retailer_id, enabled, next_due_at);
CREATE INDEX idx_backoff_until ON backoff_state   (scope_kind, backoff_until);

-- Pending deliveries: the dispatch loop scans every <=5s (Req 10.2); it also reclaims expired leases.
CREATE INDEX idx_deliveries_pending ON alert_deliveries (status, next_attempt_at);
CREATE INDEX idx_deliveries_lease   ON alert_deliveries (status, lease_until);
CREATE INDEX idx_deliveries_alert   ON alert_deliveries (alert_id);

-- Seen-products lookups happen once per discovered product per poll (hot during discovery).
-- Covered by the composite PK (retailer_id, game_id, external_id); no extra index needed.

-- Open incidents: dedupe/rate-limit checks look up the open incident for a scope by scope_key.
CREATE INDEX idx_incidents_open ON incidents (kind, scope_key, closed_at);

-- Admin digest: find each scope's un-digested notices and read them per scope_key (Req 17.7/17.8).
-- Keyed by scope_key + digested_at + created_at so the digest-age query (per scope) and the per-scope
-- read are both covered. The per-scope hourly window itself is read from admin_scope_state (below).
CREATE INDEX idx_admin_notices_pending ON admin_notices (scope_key, digested_at, created_at);

-- admin_scope_state is a tiny table keyed by its scope_key PRIMARY KEY; the per-scope hourly-limit
-- lookup is a point read on that key, so no secondary index is needed.

-- Health probe scheduling: find unhealthy destinations due for a probe (Req 10.11).
CREATE INDEX idx_destinations_probe ON destinations (health_status, next_probe_at);

-- Pruning sweep (Req 14.20) scans terminal outbox rows by age.
CREATE INDEX idx_outbox_terminal ON alerts_outbox (terminal, created_at);
