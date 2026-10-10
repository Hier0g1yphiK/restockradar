# Implementation Plan: Stock & Drop Monitor

## Overview

Convert the feature design into a series of prompts for a code-generation LLM that will implement each
step with incremental progress. Each prompt builds on the previous ones and ends with wiring things
together; there is no hanging or orphaned code that is not integrated into a previous step. The plan is
test-driven: correctness Properties 1–24 and the Req 21 fixture/mock tests are interleaved as explicit
test sub-tasks next to the code they verify. **Tests are required, not optional** — every test sub-task
below is part of its milestone's definition of done.

The implementation language is **Python 3.12** (fixed by Req 19.1/19.3; the design uses concrete Python,
not pseudocode). Property-based tests use **Hypothesis**; async tests use **pytest-asyncio**. Each
property test runs ≥100 iterations and carries the tag
`Feature: stock-drop-monitor, Property {n}: {text}`.

The work is organised into **four milestones (M1–M4)**, each ending with a `STOP: user review` boundary:

- **M1 — Foundations & scaffolding.** Scaffold, settings, in-memory heartbeat registry, persistence,
  migrations, domain, config, adapter interface + Shopify_Adapter, polite HTTP, the manual
  fixture-capture script, the real `config.yaml`, app wiring, and a baseline summary log. Deliverable:
  `python -m restockradar --once --dry-run` runs end to end, seeds state, emits zero alerts, prints the
  baseline summary.
- **M2 — Detection core (still dry-run).** Diffing (new-product, restock, price, page-live, combined),
  atomic write-set + outbox fan-out, watcher/scheduler, backoff, and the watcher-level handle-rename
  write path. Deliverable: a dry-run `--once` over fixtures exercises detection and writes outbox rows.
- **M3 — Delivery (real webhook).** Notifier/webhook, dispatch loop, Discord formatting, destination
  health + probes. Deliverable: real webhook delivery to a test Discord destination with retries and
  leases working.
- **M4 — Operations & hardening.** Incidents + admin digest, secrets/redaction, operational-mode
  finalisation, observability/watchdog, Docker/compose, MVP fixture completeness. Deliverable:
  end-to-end `--once` on a separate DB, the container builds, and the healthcheck passes.

**Out of scope for this plan (Phase 2 — do NOT build here):** the Sitemap_Adapter (Req 11.11),
Generic_HTML_Adapter (Req 11.12), Custom_Adapter (Req 11.13), and Playwright fallback (Req 11.14), per
Req 22.2. The Discord bot and slash commands (Req 23.1), dashboards/price-history charts (Req 23.2),
per-user watchlists (Req 23.3), and automated purchasing (Req 23.4). These are retained in the
requirements as clearly-labelled Phase 2 / excluded work and have no tasks here.

---

## Tasks

## Milestone 1 — Foundations & scaffolding

- [x] 1. Project scaffold and tooling
  - Create the `restockradar/` repo layout and the `src/restockradar/` package tree (empty modules with
    docstrings per the design Module Layout), plus `tests/`, `migrations/`, `config/`, `fixtures/`.
  - Author `pyproject.toml`: runtime deps (httpx, apscheduler, pydantic, pyyaml, python-dotenv) and dev
    deps (hypothesis, pytest, pytest-asyncio, ruff, mypy); configure `ruff` and `mypy --strict`.
  - Add `tests/conftest.py` with the no-live-network httpx transport guard, a temp/in-memory SQLite DB
    fixture, and a frozen/injectable monotonic+wall clock.
  - _Requirements: 19.1, 19.2, 19.3_

  - [ ] 1.1 Write the no-network guard test
    - Assert any real network attempt in the suite raises (guard is active for all tests).
    - _Requirements: 21.4_

- [x] 2. Process settings and environment
  - Implement `settings.py`: load process-level settings from env (DB path, busy_timeout, retention
    days, tick_s, lease/budget/concurrency, drain timeout, watchdog thresholds + startup grace, modes),
    with typed defaults matching the design (tick 5s, lease 60s, budget 45s, concurrency 10, retention
    14d, drain 120s, watchdog sched 120s / dispatch 60s / grace 120s).
  - _Requirements: 13.8, 18.1, 19.9, 20.7_

  - [ ] 2.1 Write unit tests for settings defaults and env overrides
    - Confirm defaults and that `RR_*` env vars override them.
    - _Requirements: 20.7_

- [x] 3. In-memory heartbeat registry
  - Implement the in-memory monotonic heartbeat registry (process memory only; NO DB writes here):
    `scheduler` and `dispatch` `time.monotonic()` values with thread-safe setters/getters, the sole
    liveness source the Watchdog will later read. The heartbeat DB-table writes and the Watchdog itself
    are deferred to M4; this task provides only the in-memory beats so the scheduler, HTTP, and dispatch
    code built in later milestones can beat them.
  - _Requirements: 19.7, 19.8, 19.9, 19.13_

  - [ ] 3.1 Write unit tests for the in-memory heartbeat registry
    - Setters update the monotonic values; getters read them back; no database access occurs.
    - _Requirements: 19.9, 19.13_

- [x] 4. Persistence foundation: type adapters and DB executor
  - [ ] 4.1 Implement `persistence/types.py`
    - Decimal↔TEXT (canonical Decimal string), bool↔INTEGER (0/1), datetime↔ISO-8601 UTC adapters.
    - _Requirements: 14.17, 14.20 (TEXT Decimal), 14.18 (UTC)_

  - [ ] 4.2 Write unit tests for type adapters
    - Round-trip Decimal/bool/datetime through TEXT/INTEGER forms.
    - _Requirements: 14.17, 14.18_

  - [ ] 4.3 Implement `persistence/db.py` single-thread executor
    - One dedicated executor thread owning the sqlite3 connection; async wrappers that submit to it;
      PRAGMAs (WAL, busy_timeout, foreign_keys=ON, synchronous=NORMAL); a synchronous `BEGIN IMMEDIATE`
      context manager with no `await` inside the lock; a WAL-checkpoint(TRUNCATE) method.
    - _Requirements: 14.19, 14.16, 14.17_

- [x] 5. Schema migrations
  - [ ] 5.1 Author `migrations/0001_initial.sql`
    - All tables and indexes from the design DDL: schema_migrations, games, products, retailers,
      listings, discovery_feeds, seen_products, alerts_outbox, alert_deliveries, alert_state,
      alert_history, backoff_state, incidents, destinations, admin_notices, admin_scope_state,
      heartbeat; CHECK constraints, single non-null subject_key/scope_key keys, and hot-path indexes.
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6, 14.7, 14.8, 14.9, 14.10, 14.11, 14.12, 14.13, 14.14, 14.15, 14.23, 6.1, 9.6_

  - [ ] 5.2 Implement `persistence/migrate.py` runner
    - Forward-only, numbered files; `isolation_level=None`; take `BEGIN IMMEDIATE`, re-read max applied
      version under the lock; split each file with `sqlite3.complete_statement`; `execute()` each
      statement (never `executescript`); insert the version row in the same transaction; commit once.
      Idempotent and safe under concurrent starts.
    - _Requirements: 14.18 (versioned migrations)_

  - [ ] 5.3 Write migration safety tests (fail-midway rollback + concurrent-start)
    - A file whose second statement errors leaves neither its schema changes nor its version row, and a
      retry re-applies from the last committed version; two concurrent starts apply each migration
      exactly once (loser applies nothing under the held lock).
    - _Requirements: 14.18_

- [x] 6. Domain models and key builders
  - [ ] 6.1 Implement `domain/models.py` and `domain/ids.py`
    - Frozen dataclasses: Game, Product, Retailer, Listing, DiscoveryFeed, Destination. Build
      `subject_key` (`listing:{id}`, `feed:{feed_id}:ext:{external_id}`) and `scope_key`
      (`retailer:{id}` | `feed:{id}` | `listing:{id}` | `path:{path}` | `destination:{id}` | `system`);
      event-type enum.
    - _Requirements: 14.10, 14.12, 11.2 (shared types)_

- [x] 7. Config schema and loader
  - [ ] 7.1 Implement `config/schema.py` pydantic models
    - Models for games/products/retailers/listings/discovery_feeds/destinations matching the YAML
      schema; poll-interval floor at 30s; defaults (minor_unit_exponent 2, max_page_count 20,
      full-catalogue interval 900s); destination requires game unless admin.
    - _Requirements: 13.1, 15.1, 15.2, 11.5, 12.3, 12.6, 14.14, 22.13_

  - [ ] 7.2 Implement `config/loader.py` Config_Loader
    - Validate-all-then-upsert by stable id / `config_key`; fail fast reporting every error in one pass
      (unknown adapter type, destination referencing unknown game, unset referenced env var, YAML
      config_key colliding with a `source='bot'` row); upsert config rows only, never touch `bot` rows;
      disable absent entities by clearing enabled; on re-enable set enabled and clear `seeded_at` to
      re-baseline; represent product+retailer as a listing; read secrets only from env.
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 13.8, 13.9, 18.1, 18.2_

  - [ ] 7.3 Write config-validation fail-fast test
    - All errors reported in a single pass.
    - _Requirements: 21.13_

  - [ ] 7.4 Write re-enable rebaseline test
    - Re-enabling a disabled entity clears `seeded_at` and re-baselines with no alert on next complete
      poll.
    - _Requirements: 21.22, 13.5, 2.2_

  - [ ] 7.5 Write bot-row survival test
    - Rows with `source='bot'` survive a config reload unchanged.
    - _Requirements: 21.23, 13.3_

- [x] 8. Adapter interface, registry, errors, keyword matching
  - [ ] 8.1 Implement `adapters/base.py`, `adapters/registry.py`, `adapters/errors.py`
    - Adapter Protocol (fetch_state, discover) and ProductState / DiscoveredProduct / VariantState /
      Existence; Adapter_Registry mapping adapter-type string → factory (register only `shopify`,
      resolve, raise UnknownAdapterType for unknown); error classes FetchError, Blocked, SoftBlock,
      MalformedResponse, TruncatedFeed (no NotFound — a 404 is a returned state).
    - _Requirements: 11.1, 11.2, 11.3, 11.9, 22.2_

  - [ ] 8.2 Implement `adapters/keywords.py`
    - Unicode NFKD normalise + strip combining marks (accent-insensitive) + casefold; substring match
      over title/product_type/vendor/tags; exclude precedes include; empty-include means match-all.
    - _Requirements: 3.3, 3.4_

  - [ ] 8.3 Write property test: exclude keywords take precedence over include
    - **Property 3: Exclude keywords take precedence over include keywords**
    - **Validates: Requirements 3.3, 3.4**

  - [ ] 8.4 Write keyword-matching unit test (Unicode/accent/case)
    - Matching is Unicode-normalised, accent-insensitive, case-insensitive.
    - _Requirements: 21.14_

- [x] 9. Polite HTTP client, host gate, robots.txt
  - [ ] 9.1 Implement `http/host_gate.py` and `http/robots.py`
    - Per-host one-in-flight lock + ≥1s spacing; robots.txt fetch+cache(24h), 4xx=allow,
      5xx/network=retry-later, path allow/deny.
    - _Requirements: 1.4, 15.5_

  - [ ] 9.2 Implement `http/client.py`
    - httpx.AsyncClient wrapper: descriptive User-Agent, 30s per-request timeout, response
      classification (JSON vs HTML-where-JSON soft-block, 403/429/503-challenge Blocked, plain 5xx /
      timeout / network / malformed → FetchError), robots.txt enforcement on the request path, host-gate
      integration; `get`, `get_json`, `require_json`. The client performs read-only monitoring requests
      only — it never attempts a purchase and never attempts to bypass a captcha or bot-protection
      mechanism (on a detected challenge it classifies Blocked and backs off, it does not solve it). The
      client MUST beat the in-memory scheduler heartbeat on each HTTP request (per-request liveness),
      using the in-memory heartbeat registry from task 3, consistent with Req 19.8/19.7.
    - _Requirements: 1.2, 15.4, 15.5, 16.1, 16.2, 16.3, 16.4, 19.7, 19.8_

  - [ ] 9.3 Write soft-block detection test
    - A 2xx HTML response where JSON was expected is treated as Blocked.
    - _Requirements: 21.20, 16.3_

- [x] 10. Manual NCG fixture-capture script (not part of the pytest suite)
  - Write a standalone script (e.g. `scripts/capture_ncg_fixtures.py`) the USER runs by hand to hit live
    Northern Card Gaming and save real listing/feed JSON (and a soft-block HTML sample) into `fixtures/`.
    The script obeys robots.txt and leaves ≥1s spacing between requests to the host (reusing the M1 HTTP
    client / host gate / robots helpers), prints the candidate real listings it found, and writes the
    saved fixtures. This script is NOT imported by or run as part of the pytest suite (the suite stays
    offline per the no-network guard); it is an operator tool that produces the fixtures the adapter and
    diffing tests consume. MUST be built BEFORE the adapter/diffing fixture tests.
  - _Requirements: 22.8, 15.4, 15.5, 1.4, 21.3_

- [ ] 11. Author `config/config.yaml` for Northern Card Gaming
  - Game `pokemon`; retailer NCG (shopify, `https://www.northerncardgaming.com`, 60s poll,
    minor_unit_exponent 2); the `/collections/pokemon/products.json` and
    `/collections/pre-orders-1/products.json` feeds with the exclude keywords (plush, playmat, sleeves,
    binder) and the pre-orders include keyword `pokemon`, max_page_count 20; at least two REAL product
    listings (chosen from the fixture-capture script output in task 10); one product destination and one
    admin destination (webhook env-var names only). MUST be authored BEFORE the adapter/diffing fixture
    tests, since those tests consume these real listings and feeds.
  - _Requirements: 12.1, 12.2, 22.1, 22.3, 22.7, 22.9, 22.10, 22.11, 22.12, 22.13_

- [x] 12. Shopify_Adapter
  - [ ] 12.1 Implement discovery (`shopify.py` discover)
    - Paginate `/collections/{c}/products.json` and `/products.json` with `limit=250`, incrementing
      `page`; stop on short/empty page; raise TruncatedFeed at `max_page_count`; no `sort_by`/`+`-tags;
      build DiscoveredProduct with `external_id = str(id)` and normalised image. During pagination the
      adapter MUST beat the in-memory scheduler heartbeat per page fetched (per-page liveness), using the
      in-memory heartbeat registry from task 3, so a long multi-page crawl keeps the scheduler beat fresh;
      consistent with Req 19.7/19.8.
    - _Requirements: 11.10, 12.1, 12.3, 12.5, 12.6, 15.6, 22.3, 2.5, 19.7, 19.8_

  - [ ] 12.2 Implement single-listing fetch (`shopify.py` fetch_state)
    - `GET /products/{handle}.js` (never `.json`); variants → VariantState; minor-unit→Decimal via
      `minor_unit_exponent`; feed decimal strings parsed as major units; price = lowest available else
      lowest overall; feed-reuse from this cycle's `cycle_products`.
    - _Requirements: 11.4, 11.5, 11.6, 4.2, 5.2, 22.5, 22.6_

  - [ ] 12.3 Implement 404 / redirect / handle-rename resolution and image normalisation
    - 404 returned as `ProductState(existence=NOT_FOUND)` (never raised); explicit same-host
      301/302/307/308 `.js`-suffix resolution requiring matching `external_id`; feed-based resolution of
      a plain 404 under a new handle; set `resolved_handle`/`resolved_url` only when the handle actually
      changed; image URL normalisation (`//` → `https:`, relative → urljoin, non-http(s) → None); store
      learned `external_id` on the listing.
    - _Requirements: 7.1, 7.2, 11.7, 11.8, 8.11_

  - [ ] 12.4 Write property test: price equality under normalisation yields no price-change event
    - **Property 5: Price equality under normalisation yields no price-change event**
    - **Validates: Requirements 5.1, 5.5, 11.5**

  - [ ] 12.5 Write pagination-termination test
    - Pagination stops on a short/empty page and respects configurable `max_page_count`.
    - _Requirements: 21.10, 12.6_

  - [ ] 12.6 Write minor-units vs decimal-string parity test
    - `.js` minor-units and feed decimal string normalise to the same Decimal and produce no false
      price-change alert.
    - _Requirements: 21.21, 11.5_

  - [ ] 12.7 Write image-URL normalisation test (fixture-driven)
    - Protocol-relative → https, relative → urljoin'd, non-http(s) → None.
    - _Requirements: 8.11_

  - [ ] 12.8 Write handle-rename / not-found ADAPTER tests (both redirect paths)
    - Redirecting 404 (same-host `.js` id match) resolves; non-redirecting 404 resolves via feed;
      cross-host redirect, `.js` id mismatch, and plain 404 with no feed match fall through to
      NOT_FOUND; a resolved rename sets `resolved_*` only when the handle changed and performs NO DB
      write (the adapter only reports resolution; the watcher-level write is tested in M2).
    - _Requirements: 21.2, 21.3, 7.1, 11.8_

- [ ] 34. Baseline write path (M1 task — numbered 34 to avoid renumbering)
  - This task lives in Milestone 1, physically placed between task 12 (Shopify_Adapter) and task 13 (app
    wiring), despite its out-of-sequence top-level number. It is numbered **34** (the first free
    top-level number) rather than 25, because top-level task 25 is already the M3 real-webhook
    checkpoint and reusing it would renumber/clobber an existing task. The out-of-sequence number is
    intentional and keeps every existing task number untouched.
  - [ ] 34.1 Implement the baseline write path (minimal repository write used by the M1 dry-run `--once`)
    - A narrow persistence write path that the M1 dry-run `--once` calls so Baseline_Seeding actually
      persists. Inside ONE synchronous `BEGIN IMMEDIATE` transaction via the DB executor (task 4.3), it
      performs the upserts Baseline_Seeding requires: write stored state (in_stock / price / existence as
      last-observed) for each seeded listing; insert `seen_products` rows for every discovered product;
      upsert BOTH `alert_state` rows per listing (the restock row with `pending=0` and
      `last_restock_alert_at` NULL; the price_change row with `last_alerted_value` = the fetched price);
      and set `seeded_at`. Emits no alerts and writes no outbox rows. This is a narrower precursor to the
      full atomic write-set helper built later in M2 (task 14.1 generalises it into the combined
      state + alert_state + outbox + history + deliveries write-set).
    - _Requirements: 2.1, 2.2, 2.3, 6.1, 14.16_

  - [ ] 34.2 Write property test: baseline seeding never alerts and seeds both alert_state rows
    - **Property 4: Baseline seeding never alerts and seeds both alert_state rows**
    - **Validates: Requirements 2.1, 2.2, 2.3**
    - Also includes the baseline-poll fixture coverage moved out of M2: a baseline poll over the captured
      fixtures writes stored state, both `alert_state` rows, and `seeded_at`, and emits zero alerts.
    - _Requirements: 21.24, 2.1, 2.2, 2.3_

- [ ] 13. App wiring and baseline summary (dry-run `--once`)
  - [ ] 13.1 Implement minimal `app.py` + `__main__.py` for `--once --dry-run`
    - Arg parse (`--dry-run`, `--once`); build the M1 dependencies (settings, DB, migrations, config
      load, registry + Shopify_Adapter, HTTP client, heartbeat registry); run one dry-run `--once` cycle
      that fetches/seeds each enabled feed and listing against fixtures or live, performs Baseline_Seeding
      (silent, no alerts), and exits. This wiring DEPENDS ON the baseline write path (task 34): the cycle
      invokes task 34's write path to persist the seed, so task 34 must complete before 13.1. (Detection,
      dispatch, watchdog, and real webhooks arrive in later milestones; this wiring only has to produce a
      clean seeded first run.)
    - _Requirements: 19.12, 20.1, 20.4, 2.1, 2.3_

  - [ ] 13.2 Implement the baseline summary log
    - On a dry-run `--once` run, log a per-feed and per-listing summary: products found, whether the
      subject was seeded (y/n), and alerts emitted (= 0 on a baseline run).
    - _Requirements: 2.1, 2.3, 17.12_

  - [ ] 13.3 Write M1 end-to-end dry-run `--once` test
    - `python -m restockradar --once --dry-run` runs end to end against fixtures, seeds state, emits zero
      alerts, prints the baseline summary, and exits 0.
    - _Requirements: 20.1, 20.4, 2.1, 21.4_

**Definition of done (M1):** `ruff` clean; `mypy --strict` clean; the full `pytest` suite green; AND the
runnable deliverable — `python -m restockradar --once --dry-run` runs end to end against the captured
fixtures / live NCG, seeds state via Baseline_Seeding, emits zero alerts, and prints the baseline
summary log (products found / seeded y-n / alerts = 0 per feed and listing).
STOP: user review before M2

---

## Milestone 2 — Detection core (still dry-run)

- [ ] 14. Repository layer and atomic write-set helper
  - [ ] 14.1 Implement typed repository methods in `persistence/repository.py`
    - Read helpers (stored listing/feed state, alert_state rows, enabled+healthy destinations for a
      game, backoff_state, incidents, due entities) and the single synchronous `BEGIN IMMEDIATE`
      write-set helper that writes stored state + alert_state (upsert ON CONFLICT) + alerts_outbox +
      alert_history + per-destination alert_deliveries together, running on the executor.
    - _Requirements: 6.1, 6.2, 6.3, 6.6, 6.7, 9.1_

  - [ ] 14.2 Write property test: keyed upserts yield exactly one row per key
    - **Property 15: Keyed upserts yield exactly one row per key**
    - **Validates: Requirements 14.10, 14.12, 6.1**

  - [ ] 14.3 Write property test: transaction rollback leaves no partial rows
    - **Property 14: A transaction rollback leaves no partial rows**
    - **Validates: Requirements 6.3**

  - [ ] 14.4 Write property test: outbox rows survive a restart
    - **Property 12: Outbox rows survive a restart** (close/reopen DB; pending set identical).
    - **Validates: Requirements 6.5**

  - [ ] 14.5 Write atomicity + restart durability example test
    - Transition + alert_state + outbox + history + deliveries written atomically; undelivered
      deliveries survive a simulated restart.
    - _Requirements: 21.11, 14.21_

- [ ] 15. Detection / new-product diffing
  - [ ] 15.1 Implement `detect/events.py` and `detect/diff_feed.py`
    - Event dataclasses; new-product detection vs seen_products keyed by (retailer,game,external_id),
      order-independent; keyword filtering; always record seen_products regardless of
      include/exclude/alert; tracked-listing suppression (match enabled listing by external_id OR handle
      → record seen_products but emit no new-product alert).
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 12.7, 22.4_

  - [ ] 15.2 Write property test: a new product yields exactly one new-product event
    - **Property 1: A new product yields exactly one new-product event**
    - **Validates: Requirements 3.1, 3.5, 3.6**

  - [ ] 15.3 Write property test: feed processing is order-independent
    - **Property 2: Feed processing is order-independent**
    - **Validates: Requirements 3.2, 12.7**

  - [ ] 15.4 Write property test: tracked-listing suppression records but emits no new-product alert
    - **Property 23: A product covered by a tracked listing is recorded but emits no new-product alert**
    - **Validates: Requirements 3.1, 3.5, 3.6**

  - [ ] 15.5 Write new-product + two-games fixture tests
    - New product in a fixture → exactly one new-product alert; two feeds for different games on the
      same retailer each alert for the same External_Id.
    - _Requirements: 21.5, 21.28, 3.1, 14.6_

- [ ] 16. Detection / listing diffing (restock + price + page-live + combined)
  - [ ] 16.1 Implement `detect/diff_listing.py`
    - Restock transition logic with cooldown + restock_pending (pending set/clear/trailing/sellout);
      price-change via exact-Decimal vs last_alerted_price + cooldown; A→B→A flap suppression;
      page-live one-shot keyed on stored existence with 2-consecutive-404 flip, NOT_FOUND never writes
      in_stock/price, page-live payload carries current stock+price and upserts both alert_state rows;
      combined-alert rule (combine only when both actually emitted, else separate); default cooldown
      300s with per-subject override.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.9, 4.10, 5.1, 5.3, 5.5, 5.6, 7.3, 7.4, 7.5, 7.6, 7.8, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_

  - [ ] 16.2 Write property test: no duplicate alert within the cooldown
    - **Property 6: No duplicate alert within the cooldown**
    - **Validates: Requirements 9.3, 9.7**

  - [ ] 16.3 Write property test: deferred (restock_pending) trailing alert fires exactly once
    - **Property 7: The deferred (restock_pending) trailing alert fires exactly once**
    - **Validates: Requirements 4.3, 4.4, 4.5**

  - [ ] 16.4 Write property test: A→B→A price flap within the cooldown yields no alert
    - **Property 8: An A→B→A price flap within the cooldown yields no alert**
    - **Validates: Requirements 9.5**

  - [ ] 16.5 Write property test: restock → sell-out → restock yields two alerts
    - **Property 9: Restock → sell-out → restock, both gaps beyond the cooldown, yields two alerts**
    - **Validates: Requirements 4.7**

  - [ ] 16.6 Write property test: page going live produces exactly one alert and no stock/price alert
    - **Property 22: A page going live produces exactly one alert and no stock/price alert that poll**
    - **Validates: Requirements 7.3, 7.4, 7.5**

  - [ ] 16.7 Write property test: existence flips to not-found only after two consecutive 404s
    - **Property 20: Existence flips to not-found only after two consecutive 404s**
    - **Validates: Requirements 7.6**

  - [ ] 16.9 Write restock/price/cooldown fixture tests
    - Variant unavailable→available ⇒ one restock, no repeat in cooldown; price change ⇒ one alert;
      trailing price-change fires once after cooldown; A→B→A flap no alert; restock→sellout→restock ⇒
      two alerts; flap within cooldown ⇒ no restock; restock within cooldown sets pending then fires
      once after cooldown. (Baseline-seeding coverage — Property 4 and the baseline-poll fixture — now
      lives in M1 under task 34.2, not here. Sub-task 16.8, formerly the Property-4 test, was removed and
      its number intentionally left as a gap to avoid renumbering 16.9/16.10.)
    - _Requirements: 21.6, 21.7, 21.8, 21.9, 21.18, 21.25, 21.26_

  - [ ] 16.10 Write page-live 404 fixture tests
    - First-poll 404 stores existence false and a later transition produces exactly one Page_Live_Event;
      a single transient 404 on a previously-existing page stores no change and produces no event.
    - _Requirements: 21.19, 21.27, 7.3_

- [ ] 17. Fan-out into the atomic write-set
  - [ ] 17.1 Implement `detect/fanout.py` and wire detection to the write-set helper
    - Resolve matching enabled + healthy non-admin destinations for the alert's game → one
      alert_deliveries row each inside the same transaction as the state change; combined/single alert
      payloads; integrate diff_feed/diff_listing output with the repository write-set from task 14.
    - _Requirements: 6.1, 6.2, 8.1, 10.10_

  - [ ] 17.2 Write property test: fan-out writes one delivery row per matching enabled, healthy destination
    - **Property 10: Fan-out writes one delivery row per matching enabled, healthy destination**
    - **Validates: Requirements 6.2, 8.1, 10.10**

- [ ] 18. Backoff state machine
  - [ ] 18.1 Implement `watcher/backoff.py` and `watcher/due.py`
    - Ordinary vs Blocked exponential backoff starting at the entity poll interval, doubling, capped at
      1h, honouring Retry-After for Blocked; per retailer/feed/listing counters; reset on success;
      effective-due = max(next_due_at, backoff_until) for feeds/listings and backoff_until alone for a
      retailer.
    - _Requirements: 1.8, 16.5, 16.6, 16.7_

  - [ ] 18.2 Write property test: backoff grows monotonically to the cap and resets on success
    - **Property 21: Backoff grows monotonically to the cap and resets on success**
    - **Validates: Requirements 16.5, 16.6, 16.7**

- [ ] 19. Watcher core and scheduler
  - [ ] 19.1 Implement `watcher/core.py` per-retailer tick
    - Read retailer backoff_state and defer the whole job on retailer-wide backoff; select enabled due
      feeds+listings ordered by effective due time ascending, feeds before listings; process
      sequentially under a 5-minute per-poll cap; fetch → BEGIN IMMEDIATE → read → diff → write → commit;
      clear/populate `cycle_products` per run; on finish set next_due_at = finish + interval + jitter;
      beat the in-memory scheduler heartbeat between entities and per HTTP request; record adapter
      failures in backoff_state and emit no transition.
    - _Requirements: 1.1, 1.3, 1.6, 1.7, 2.1, 2.2, 2.3, 2.4, 3.7, 3.8, 4.8, 4.11, 5.4, 5.7, 6.6, 6.7, 7.7, 7.9, 15.1, 15.3_

  - [ ] 19.2 Implement the watcher-level handle-rename WRITE path
    - When the adapter reports a rename (`resolved_handle` / `resolved_url` set), the watcher — inside its
      own `BEGIN IMMEDIATE` transaction — updates the listing's `url` + `handle`, treats the listing as
      EXISTING, does NOT increment `consecutive_404` and emits NO Page_Live_Event, and queues the
      `retailer:<id>` handle-change admin notice (routed through the per-scope_key admin-warning path).
      When the adapter reports the EQUAL-handle case (no `resolved_*` field set), the watcher performs no
      url/handle change and queues NO notice.
    - _Requirements: 11.8, 7.7_

  - [ ] 19.3 Implement `watcher/scheduler.py`
    - AsyncIOScheduler: exactly one job per retailer ticking at fixed `tick_s`, `coalesce=True`,
      `max_instances=1`, staggered initial ticks; plus one independent interval job (every `tick_s`)
      that beats the scheduler heartbeat regardless of any retailer job.
    - _Requirements: 1.5, 1.9, 1.10, 19.7_

  - [ ] 19.4 Write property test: existence flip and handle-rename write path (watcher-level)
    - When the adapter reports `resolved_handle`/`resolved_url`, the watcher transaction updates the
      listing url+handle, keeps existence EXISTING with no Page_Live_Event, and queues exactly one
      `retailer:<id>` admin notice; when NO resolved field is set (equal handle), NO url/handle change
      and NO notice are written.
    - _Requirements: 11.8, 7.7_

  - [ ] 19.5 Write scheduler/due-selection unit tests
    - Retailer-wide backoff defers the whole job; due-ascending ordering with feeds-before-listings;
      per-entity next_due_at set on finish; poll-interval floor applied; staggered startup ticks; missed
      ticks coalesce.
    - _Requirements: 1.5, 1.6, 1.7, 1.9, 1.10, 15.2_

- [ ] 20. Checkpoint — core detection pipeline (dry-run `--once`)
  - Ensure all tests pass, ask the user if questions arise. Confirm a dry-run `--once` over fixtures
    exercises detection and writes outbox rows (no real POSTs).

**Definition of done (M2):** `ruff` clean; `mypy --strict` clean; the full `pytest` suite green; AND the
runnable deliverable — a dry-run `--once` over the captured fixtures exercises detection (new-product,
restock, price, page-live, combined, handle-rename) and writes the resulting `alerts_outbox` /
`alert_deliveries` rows durably, with no real webhook POSTs issued.
STOP: user review before M3

---

## Milestone 3 — Delivery (real webhook)

- [ ] 21. Notifier webhook sender
  - [ ] 21.1 Implement `notifier/webhook.py`
    - Resolve each destination's webhook URL from the env var named on the destination and POST to that
      target; in-request exponential backoff + Retry-After, up to 3 extra attempts, under a 45s total
      budget; return-to-pending with next_attempt_at when a wait would exceed the remaining budget; GET
      probe helper; beat the in-memory dispatch heartbeat at every await boundary (before POST, after
      response, before/after each retry sleep).
    - _Requirements: 8.8, 10.6, 10.7, 19.8_

- [ ] 22. Notifier dispatch loop
  - [ ] 22.1 Implement `notifier/dispatch.py` single-flight loop
    - Scan pending deliveries joined to outbox at most every 5s; reclaim stale `sending` leases to
      pending (counts as attempt); one worker task per destination bounded by a global concurrency
      semaphore (default 10); per-batch lease (60s) taken immediately before each batch POST; mark
      delivered / expired / failed; expire alerts older than max age; set `alerts_outbox.terminal` when
      all deliveries terminal; never re-send to an already-succeeded destination.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.8, 10.9, 10.11, 6.4, 6.5, 14.20_

  - [ ] 22.2 Write property test: a retry never re-sends to an already-succeeded destination
    - **Property 11: A retry never re-sends to an already-succeeded destination (clean dispatch rounds)**
    - **Validates: Requirements 10.8**

  - [ ] 22.3 Write property test: an alert older than the maximum age is never delivered late
    - **Property 13: An alert older than the maximum age is never delivered late**
    - **Validates: Requirements 10.7**

  - [ ] 22.4 Write delivery retry/backoff + budget + partial-multi-destination + lease tests (mocked transport)
    - Retry/backoff honouring Retry-After; in-request budget vs Retry-After returns to pending and frees
      the worker; partial multi-destination delivery + retry delivers to the previously-failed
      destination without re-sending to a succeeded one; per-batch `sending` lease and at-least-once
      reclamation (crash between POST and `delivered` re-POSTs only that batch); concurrency semaphore
      caps concurrent workers.
    - _Requirements: 21.12, 21.17, 10.6, 10.7_

- [ ] 23. Discord formatting
  - [ ] 23.1 Implement `notifier/discord.py`
    - Embed formatting (event type, title, stock status, previous+new price, product link, image), field
      truncation to Discord limits; `allowed_mentions` role-only (never @everyone/@here); markdown escape
      on retailer-supplied text; accept only http/https URLs; batch ≤10 embeds / ≤6000 chars with
      splitting; HTTP 400 split-into-single-alert handling; combined vs single-event message.
    - _Requirements: 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.9, 8.10, 8.11_

  - [ ] 23.2 Write property test: batching never exceeds Discord's limits
    - **Property 16: Batching never exceeds Discord's limits**
    - **Validates: Requirements 8.2, 8.9**

  - [ ] 23.3 Write property test: allowed_mentions only ever contains the configured role
    - **Property 17: allowed_mentions only ever contains the configured role**
    - **Validates: Requirements 8.5**

  - [ ] 23.4 Write allowed_mentions fixture/unit test
    - Delivered messages set `allowed_mentions` so only the configured role may be pinged.
    - _Requirements: 21.15_

- [ ] 24. Destination health and probes
  - [ ] 24.1 Implement `notifier/health.py`
    - Health transitions: 401/403/404 permanent → unhealthy + expire pending + set next_probe_at; skip
      unhealthy/disabled destinations during fan-out; GET-probe unhealthy destinations every 15 min
      without logging the URL; recovery on successful probe (healthy, clear unhealthy_since, exempt
      recovery notice); reset to healthy on restart if a probe succeeds; skip probes in dry-run.
    - _Requirements: 10.11, 10.12, 10.13, 10.14, 10.15, 10.16, 10.17, 8.1_

- [ ] 25. Checkpoint — real webhook delivery
  - Ensure all tests pass, ask the user if questions arise. Confirm delivery to a test Discord
    destination succeeds with retries and leases.

**Definition of done (M3):** `ruff` clean; `mypy --strict` clean; the full `pytest` suite green; AND the
runnable deliverable — real webhook delivery to a test Discord destination works end to end, with the
in-request retry/budget behaviour and the per-batch `sending` lease / reclamation exercised against that
live destination.
STOP: user review before M4

---

## Milestone 4 — Operations & hardening

- [ ] 26. Retention pruning
  - [ ] 26.1 Implement `persistence/pruning.py`
    - Prune terminal alerts_outbox / alert_deliveries / alert_history older than retention (default
      14d); never prune seen_products, alert_state, or admin_scope_state.
    - _Requirements: 14.20, 14.22, 14.23_

  - [ ] 26.2 Write property test: pruning never deletes protected or non-terminal rows
    - **Property 19: Pruning never deletes protected or non-terminal rows**
    - **Validates: Requirements 14.20**

- [ ] 27. Incidents and admin notices / Admin_Digest
  - [ ] 27.1 Implement `incidents/incidents.py`
    - Open/close incidents (error after 3 consecutive, empty_result after 3 consecutive non-empty→empty,
      truncated_feed, robots_disallowed) with recovery notices; park/send admin warnings under scope_key
      with per-scope hourly limit backed by admin_scope_state; Admin_Digest per scope_key coalescing
      parked notices; never route a destination warning through that destination; zero-admin-destination
      ERROR logging with notices kept un-digested; recovery notices exempt from the limit.
    - _Requirements: 2.5, 10.9, 10.10, 11.8, 17.1, 17.2, 17.3, 17.4, 17.5, 17.6, 17.7, 17.8, 17.9, 17.10, 17.11, 17.12, 17.13, 15.5_

  - [ ] 27.2 Wire the Admin_Digest trigger into the dispatch round
    - On each dispatch round, per scope_key, emit one Admin_Digest when un-digested notices exist and the
      scope's last_warning_at is ≥1h old; stamp digested_at and advance last_warning_at; fan out to the
      other enabled healthy admin destinations.
    - _Requirements: 17.6, 17.7, 17.8_

  - [ ] 27.3 Write property test: per-scope_key admin-warning hourly limit sends one and parks the rest
    - **Property 24: The per-scope_key admin-warning hourly limit sends at most one warning per hour and parks the rest**
    - **Validates: Requirements 17.6, 17.7, 17.8, 10.9**

  - [ ] 27.4 Write zero-admin-destination + never-route-through-destination tests
    - Zero enabled healthy admin destination → ERROR log with redacted URL, notices kept un-digested and
      last_warning_at unchanged until a destination exists; a destination-scoped warning routes only to
      the other admin destinations and never through the destination it is about.
    - _Requirements: 17.10, 17.11, 10.9_

  - [ ] 27.5 Write admin rate-limit storage test (nothing reads `incidents.last_warning_at` for the limit)
    - Assert the per-scope_key hourly limit is backed SOLELY by `admin_scope_state.last_warning_at`:
      driving the rate limit never reads `incidents.last_warning_at`, and the destination-scope and
      system-scope paths (which have no incident) rate-limit correctly from `admin_scope_state` alone.
    - _Requirements: 17.6, 14.23, 17.8_

- [ ] 28. Secrets handling and log redaction
  - [ ] 28.1 Implement `observability/logging.py`
    - stdout logging; pin httpx/httpcore loggers to WARNING; install a redaction filter that strips
      webhook URLs and tokens from every record and from exception text on all paths.
    - _Requirements: 18.3, 18.4, 19.14_

  - [ ] 28.2 Write property test: log redaction removes webhook URLs from arbitrary text
    - **Property 18: Log redaction removes webhook URLs from arbitrary text**
    - **Validates: Requirements 18.4**

  - [ ] 28.3 Write no-webhook-URL-in-logs test (including error paths)
    - No webhook URL appears in captured log output on any path.
    - _Requirements: 21.16, 18.4_

- [ ] 29. Operational modes finalisation (`--dry-run`, `--once`)
  - [ ] 29.1 Finalise `app.py` lifecycle and `__main__.py` entrypoint
    - Build the full dependency graph (settings, DB, migrations, config load, registry+adapter, http
      client, watcher/scheduler, notifier, incidents, logging, watchdog); host the independent
      scheduler-liveness beat; SIGTERM shutdown ordering (stop watchdog first, stop scheduler, cancel
      in-flight polls, let open txn/POST finish in 30s, WAL checkpoint, close, exit).
    - _Requirements: 19.12, 14.21_

  - [ ] 29.2 Finalise dry-run and once modes
    - `--dry-run`: polling/state normal, replace every POST with a redacted log line, skip health probes,
      drain immediately. `--once`: reclaim stale leases at start, treat enabled entities and all pending
      deliveries as due (ignore next_due_at/backoff/next_attempt_at), honour Retry-After only within the
      budget, bounded drain timeout, exit 0 only if no pending/sending remain else non-zero + log count,
      watchdog disabled.
    - _Requirements: 20.1, 20.2, 20.3, 20.4, 20.5, 20.6, 20.7, 20.8, 20.9, 19.11_

  - [ ] 29.3 Write dry-run and once-mode tests
    - Dry-run issues no POST, logs rendered content, writes no webhook URL, drains immediately; `--once`
      runs one cycle, drains, exits — with the non-zero-exit + remaining-count case for an undeliverable
      destination.
    - _Requirements: 21.29, 21.30_

  - [ ] 29.4 Write end-to-end `--once` test against a SEPARATE database path
    - Per the design's dry-run/`--once` caveat that first runs use a separate DB: run a full `--once`
      cycle pointed at a throwaway DB path distinct from the production DB (via `RR_DB_PATH`), confirm it
      migrates, seeds, detects, drains the outbox, and exits with the correct status, leaving the
      production DB untouched.
    - _Requirements: 20.4, 20.5, 20.8, 14.19_

- [ ] 30. Observability: heartbeat table, healthcheck, watchdog
  - [ ] 30.1 Wire throttled heartbeat-table writes into the scheduler and dispatch loops
    - The in-memory monotonic heartbeat registry already exists (task 3, M1); this task's remaining job
      is the throttled DB heartbeat-TABLE persistence used by the external healthcheck. Wire the
      throttled (≤5s) heartbeat-table writes for `scheduler` and `dispatch` liveness INTO both the
      scheduler loop and the dispatch loop: the table write is DERIVED from the in-memory beats (task 3)
      but performed from those loops at most every 5s, kept off the hot path. For the healthcheck only;
      the Watchdog continues to read only the in-memory beats (task 30.3).
    - _Requirements: 19.7, 19.8, 19.9, 19.13, 14.16_

  - [ ] 30.2 Implement `observability/healthcheck.py`
    - Container healthcheck probe: read heartbeat table rows under fixed thresholds (sched 120s, dispatch
      60s) plus a BEGIN IMMEDIATE/ROLLBACK DB-writability probe.
    - _Requirements: 19.6_

  - [ ] 30.3 Implement `observability/watchdog.py`
    - Dedicated daemon thread reading only the in-memory monotonic beats (task 3); fixed thresholds +
      120s startup grace; log + `os._exit(1)` on staleness; not started in `--once`; stopped first on
      SIGTERM.
    - _Requirements: 19.10, 19.11, 19.12, 19.13_

  - [ ] 30.4 Write watchdog edge-case + healthcheck + heartbeat-separation tests
    - Zero-retailer deployment does not trip the watchdog; a long legitimate retry sequence keeps the
      dispatch beat fresh while a wedged POST goes stale and fires; `--once` disables the watchdog;
      SIGTERM stops the watchdog first and the quiet period does not trip it; startup grace suppresses
      stale prior-run state; healthcheck reports unhealthy on stale rows or failed write probe; the
      watchdog never reads the table and the table write is throttled.
    - _Requirements: 19.6, 19.7, 19.8, 19.10, 19.11, 19.12, 19.13_

  - [ ] 30.5 Write graceful-shutdown SIGTERM test
    - In-flight poll cancelled with no state loss; open txn + in-flight POST finish within 30s; WAL
      checkpoint runs; connection closes and process exits.
    - _Requirements: 19.12, 14.16_

- [ ] 31. Deployment artifacts (authoring only)
  - [ ] 31.1 Author the `Dockerfile`
    - Python 3.12 base; create non-root user with FIXED uid/gid 10001 (`useradd -u 10001 -g 10001`);
      chown app dir and `/data`; end with `USER 10001:10001`.
    - _Requirements: 19.4_

  - [ ] 31.2 Author `docker-compose.yml`
    - Service `restockradar`, `restart: unless-stopped`, named volume for the SQLite dir, `:ro` config
      bind mount, `env_file: .env`, `stop_grace_period: 45s`, healthcheck invoking the healthcheck
      module, `RR_DB_PATH` env. Document that the FUTURE Discord bot service MUST run as the same
      UID/GID 10001 so the shared WAL volume's `-wal`/`-shm` sidecar files remain writable by both
      processes.
    - _Requirements: 19.4, 19.5, 19.6_

- [ ] 32. MVP fixture completeness
  - [ ] 32.1 Finalise and commit NCG JSON/HTML fixtures
    - Ensure the saved collection feed JSON, `products/{handle}.js` JSON, and soft-block HTML page under
      `fixtures/` (captured by the task 10 script) cover every adapter/diffing/mode test used across
      M1–M4; no network access in tests.
    - _Requirements: 21.3, 22.8_

- [ ] 33. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass (and `ruff` + `mypy --strict` are clean), ask the user if questions arise.

- [ ] 35. db.py cold-start lock ordering + two-process cold-start test
  - In `persistence/db.py`, set `PRAGMA busy_timeout` BEFORE `PRAGMA journal_mode = WAL` on connection
    open, so that two processes racing a cold start on the same new database file do not hit an unhandled
    "database is locked" during WAL journal-mode setup. The `busy_timeout` must be armed before the WAL
    switch takes its brief write lock, otherwise a concurrent cold start can fail immediately instead of
    waiting out the lock. This matters because a separate Discord bot process will share the database
    (Req 14.19).
  - _Requirements: 14.16, 14.19_

  - [ ] 35.1 Write a two-process cold-start race test
    - Add a two-process (or two-connection, each with its own executor) cold-start test that races two
      `Database` constructions against the SAME brand-new file path concurrently and asserts both open
      successfully with WAL active and no "database is locked" error — i.e. the busy_timeout-before-WAL
      ordering makes cold-start contention safe. Use a temp-file DB (WAL is a no-op on `:memory:`).
    - _Requirements: 14.16, 14.19_

**Definition of done (M4):** `ruff` clean; `mypy --strict` clean; the full `pytest` suite green; AND the
runnable deliverable — an end-to-end `--once` run against a SEPARATE database path completes
(migrate → seed → detect → drain → exit), the image builds and the healthcheck module passes against a
locally running instance.
STOP: user review before adding Pokémon Center and Smyths

---

## Notes

- **All tests are required.** No sub-task is optional; every unit, property, and integration test sub-task
  above is part of its milestone's definition of done and must be green before that milestone's STOP.
- Each task references the specific requirement sub-clauses it implements for traceability, and property
  test tasks name the design Correctness Property they implement.
- `ruff` and `mypy --strict` passing is part of the definition of done for every milestone (Req 19.2); it
  is a standing convention rather than a standalone task.
- Property tests use Hypothesis at ≥100 iterations and are tagged
  `Feature: stock-drop-monitor, Property {n}: {text}`; Property 11 is tested only for clean, non-crash
  dispatch rounds, with a companion example test for the at-least-once lease-reclamation window.
- The manual fixture-capture script (task 10) is an operator tool, NOT part of the pytest suite; the
  suite stays offline per the no-network guard (task 1.1).
- **Phase 2 / excluded (no tasks here):** Sitemap_Adapter (11.11), Generic_HTML_Adapter (11.12),
  Custom_Adapter (11.13), Playwright fallback (11.14), per Req 22.2; Discord bot + slash commands
  (23.1), dashboards/price-history (23.2), per-user watchlists (23.3), automated purchasing (23.4).

**Project Notes checkpoint:** After the Northern Card Gaming MVP is complete and wired end to end (the
M4 STOP), the plan pauses for review before adding the Pokémon Center and Smyths retailers (per the
Project Notes in requirements.md — a process checkpoint, not a system behaviour). The FUTURE Discord bot
service that shares this database MUST run as UID/GID 10001 (the same fixed uid/gid as this service's
container) so the shared WAL volume's `-wal` and `-shm` sidecar files stay writable by both processes.

## Task Dependency Graph

Per-milestone waves. Tasks within a wave are independent and may run in parallel; a wave runs only after
all earlier waves in the SAME milestone complete. Each milestone runs only after the previous milestone's
STOP review — so no task depends on a task in a later milestone, and the STOP boundaries are never crossed
by a forward reference. The fixture-capture script (10) runs only AFTER the HTTP client it reuses (9.2) is
built, and `config.yaml` (11) is authored only AFTER task 10 has produced candidate real listings; the
adapter fixture-consuming tests (12.x) run only AFTER task 11. The in-memory heartbeat registry (3) is an
early M1 wave. The baseline write path (task 34, placed in M1) runs after the Shopify_Adapter and the DB
executor, and app wiring 13.1 runs only after task 34.

**Human-in-the-loop gate (M1):** Task 10 (the manual NCG fixture-capture script) and task 11 (authoring
and approving `config/config.yaml` with real listings/feeds) are performed BY THE USER by hand. The USER
must manually run task 10 and approve `config.yaml` (task 11) BEFORE the adapter fixture-test wave (the
12.x tests that consume those fixtures) can run. This is an explicit human-in-the-loop gate: automation
pauses after 9.2 so the operator captures fixtures (10) and curates config (11), then the fixture-driven
test waves proceed.

Both representations below are kept and must agree: the flat top-level `"waves"` array (the canonical
scheduling format) and the `"milestones"` breakdown (a convenience view). The flat array is retained
because the per-milestone structure is a non-canonical extension; keeping the flat array guarantees the
graph validates. The flat array has been re-flattened after the reordering so that 10 follows 9.2, 11
follows 10, the 12.x fixture tests follow 11, and the new task 34 (34.1 then 34.2) precedes 13.1.

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1", "3.1", "4.1"] },
    { "id": 1, "tasks": ["4.2", "4.3", "5.1", "6.1"] },
    { "id": 2, "tasks": ["5.2", "7.1", "8.1", "8.2", "9.1"] },
    { "id": 3, "tasks": ["5.3", "7.2", "8.3", "8.4", "9.2"] },
    { "id": 4, "tasks": ["7.3", "7.4", "7.5", "9.3", "10"] },
    { "id": 5, "tasks": ["11"] },
    { "id": 6, "tasks": ["12.1", "12.2"] },
    { "id": 7, "tasks": ["12.3"] },
    { "id": 8, "tasks": ["12.4", "12.5", "12.6", "12.7", "12.8"] },
    { "id": 9, "tasks": ["34.1"] },
    { "id": 10, "tasks": ["34.2"] },
    { "id": 11, "tasks": ["13.1"] },
    { "id": 12, "tasks": ["13.2"] },
    { "id": 13, "tasks": ["13.3"] },
    { "id": 14, "tasks": ["14.1"] },
    { "id": 15, "tasks": ["14.2", "14.3", "14.4", "14.5", "15.1", "16.1"] },
    { "id": 16, "tasks": ["15.2", "15.3", "15.4", "15.5", "16.2", "16.3", "16.4", "16.5", "16.6", "16.7", "16.9", "16.10", "18.1"] },
    { "id": 17, "tasks": ["17.1", "18.2"] },
    { "id": 18, "tasks": ["17.2", "19.1"] },
    { "id": 19, "tasks": ["19.2", "19.3"] },
    { "id": 20, "tasks": ["19.4", "19.5"] },
    { "id": 21, "tasks": ["21.1"] },
    { "id": 22, "tasks": ["22.1", "23.1"] },
    { "id": 23, "tasks": ["22.2", "22.3", "22.4", "23.2", "23.3", "23.4", "24.1"] },
    { "id": 24, "tasks": ["26.1", "28.1", "30.1", "35.1"] },
    { "id": 25, "tasks": ["26.2", "27.1", "28.2", "28.3", "30.2", "30.3"] },
    { "id": 26, "tasks": ["27.2", "29.1", "30.4", "30.5", "31.1"] },
    { "id": 27, "tasks": ["27.3", "27.4", "27.5", "29.2", "31.2", "32.1"] },
    { "id": 28, "tasks": ["29.3", "29.4"] }
  ],
  "milestones": [
    {
      "milestone": "M1",
      "waves": [
        { "id": 0, "tasks": ["1.1", "2.1", "3.1", "4.1"] },
        { "id": 1, "tasks": ["4.2", "4.3", "5.1", "6.1"] },
        { "id": 2, "tasks": ["5.2", "7.1", "8.1", "8.2", "9.1"] },
        { "id": 3, "tasks": ["5.3", "7.2", "8.3", "8.4", "9.2"] },
        { "id": 4, "tasks": ["7.3", "7.4", "7.5", "9.3", "10"] },
        { "id": 5, "tasks": ["11"] },
        { "id": 6, "tasks": ["12.1", "12.2"] },
        { "id": 7, "tasks": ["12.3"] },
        { "id": 8, "tasks": ["12.4", "12.5", "12.6", "12.7", "12.8"] },
        { "id": 9, "tasks": ["34.1"] },
        { "id": 10, "tasks": ["34.2"] },
        { "id": 11, "tasks": ["13.1"] },
        { "id": 12, "tasks": ["13.2"] },
        { "id": 13, "tasks": ["13.3"] }
      ]
    },
    {
      "milestone": "M2",
      "waves": [
        { "id": 0, "tasks": ["14.1"] },
        { "id": 1, "tasks": ["14.2", "14.3", "14.4", "14.5", "15.1", "16.1"] },
        { "id": 2, "tasks": ["15.2", "15.3", "15.4", "15.5", "16.2", "16.3", "16.4", "16.5", "16.6", "16.7", "16.9", "16.10", "18.1"] },
        { "id": 3, "tasks": ["17.1", "18.2"] },
        { "id": 4, "tasks": ["17.2", "19.1"] },
        { "id": 5, "tasks": ["19.2", "19.3"] },
        { "id": 6, "tasks": ["19.4", "19.5"] }
      ]
    },
    {
      "milestone": "M3",
      "waves": [
        { "id": 0, "tasks": ["21.1"] },
        { "id": 1, "tasks": ["22.1", "23.1"] },
        { "id": 2, "tasks": ["22.2", "22.3", "22.4", "23.2", "23.3", "23.4", "24.1"] }
      ]
    },
    {
      "milestone": "M4",
      "waves": [
        { "id": 0, "tasks": ["26.1", "28.1", "30.1", "35.1"] },
        { "id": 1, "tasks": ["26.2", "27.1", "28.2", "28.3", "30.2", "30.3"] },
        { "id": 2, "tasks": ["27.2", "29.1", "30.4", "30.5", "31.1"] },
        { "id": 3, "tasks": ["27.3", "27.4", "27.5", "29.2", "31.2", "32.1"] },
        { "id": 4, "tasks": ["29.3", "29.4"] }
      ]
    }
  ]
}
```
