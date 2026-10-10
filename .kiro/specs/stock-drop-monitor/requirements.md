# Requirements Document

## Introduction

This document specifies the requirements for a backend stock and drop monitor for trading card retailers. The system detects new product appearances, out-of-stock to in-stock transitions, price changes, and page-existence transitions across retailer websites, then delivers alerts to Discord servers via webhooks. The service also supports two operational modes: a dry-run mode that renders alerts to the log instead of posting webhooks, and a single-cycle `--once` mode that runs one poll cycle, drains pending deliveries, and exits.

The architecture is extensibility-first: new games, products, retailers, listings, and discovery feeds are added through configuration changes alone, without modifying core code. Retailer-specific logic lives behind a uniform adapter interface. The MVP implements only the Shopify_Adapter; additional adapter types are retained as clearly labelled Phase 2 (post-MVP) requirements so that simple sites can later be supported with CSS selectors or standard feeds while harder sites use dedicated custom adapters.

The first game under test is Pokémon TCG; later games include Topps Premier League flagship cards and others. The MVP targets a single retailer (Northern Card Gaming, a Shopify store) to prove the pipeline end to end.

The Discord bot and slash commands are a separate, later specification and are explicitly out of scope here. Because that later bot process will share this specification's database, the persistence layer is designed for concurrent multi-process access. This specification covers the backend only, including outbound Discord webhook notifications.

## Glossary

- **Monitor**: The complete backend system that detects stock, drop, price, and existence events and delivers alerts.
- **Watcher_Core**: The scheduling component that iterates over listings and discovery feeds, invokes adapters, diffs results against stored state, and emits events on state transitions.
- **Scheduler**: The subcomponent of the Watcher_Core that triggers polling at configured per-retailer intervals.
- **Adapter**: A component implementing a uniform interface that retrieves product state from, and discovers products on, a specific retailer or site type.
- **Adapter_Registry**: The component that maps a Retailer's adapter type string to its Adapter implementation and resolves the Adapter at poll time.
- **Shopify_Adapter**: An Adapter that reads a single product's state from the Shopify `/products/{Handle}.js` endpoint and discovers products from Shopify collection product JSON feeds and, as a general capability, the full-catalogue `/products.json` feed.
- **Handle**: The URL slug segment that identifies a single Shopify product, used to build the `/products/{Handle}.js` endpoint URL for that product.
- **Minor_Units**: An integer representation of a monetary amount in the currency's smallest unit (for example pence), where the number of minor units in one major unit is given by the Minor_Unit_Exponent (a value of 2 means 100 minor units per major unit).
- **Minor_Unit_Exponent**: A per-Retailer integer, defaulting to 2, giving the power of ten that converts Minor_Units to major units when normalising a price to Decimal.
- **Sitemap_Adapter**: A Phase 2 Adapter that discovers new product URLs by diffing a site's `sitemap.xml`.
- **Generic_HTML_Adapter**: A Phase 2 Adapter driven by CSS selectors defined in YAML configuration, requiring no code for simple sites.
- **Custom_Adapter**: A Phase 2 Adapter implemented as a Python class for sites that cannot be served by the standard adapters (e.g., Pokémon Center, Smyths).
- **Notifier**: The component that formats alerts as Discord embeds and delivers them to matching destinations via webhook, reading pending work from the Alerts_Outbox and Alert_Deliveries tables, and that probes and recovers unhealthy Destinations.
- **Config_Loader**: The component that reads YAML configuration and loads it into the database on startup.
- **Game**: A trading card product line being monitored (e.g., pokemon, topps-pl).
- **Product**: A trackable item belonging to a Game, with a name and category/set.
- **Retailer**: A seller whose site is monitored, with an adapter type, base URL, and poll interval.
- **Listing**: The association of a Product with a Retailer, identified by a URL and a Handle, carrying the last known state, its own poll interval, a Next_Due_At, and a Seeded_At.
- **Discovery_Feed**: A retailer-and-game-scoped collection, sitemap, or search URL used to detect new, not-yet-tracked products, with include/exclude keyword filters, its own poll interval, a Next_Due_At, and a Seeded_At.
- **Full_Catalogue_Feed**: A Discovery_Feed backed by the Shopify `/products.json` endpoint that enumerates a retailer's entire catalogue, polled on a longer interval and required to define include keywords.
- **Destination**: A delivery target consisting of a Discord server identifier, a game identifier, a channel/webhook target referenced by environment-variable name, an optional role to ping, and a Destination_Health.
- **Destination_Health**: The delivery-health condition of a Destination, comprising a health_status of `healthy` or `unhealthy`, an unhealthy_since timestamp, and a last_error, used to skip fan-out to an unhealthy Destination, to expire its pending Alert_Deliveries, and to drive periodic recovery probes.
- **Admin_Destination**: A Destination whose Admin_Destination flag is set, designated to receive operational warnings rather than product alerts; an Admin_Destination requires no game.
- **Admin_Destination_Flag**: A boolean flag on a Destination indicating the Destination receives operational warnings rather than product alerts.
- **Product_State**: The structured result of an Adapter fetch, containing existence, in-stock status, price, per-variant detail, URL, image, and title.
- **External_Id**: The canonical, adapter-defined identifier for a product used to key stored state: the Shopify product id as a string for the Shopify_Adapter, and a normalised URL for the Sitemap_Adapter and Generic_HTML_Adapter.
- **Seen_Products**: The persistent store of discovered products keyed by (retailer id, game id, External_Id), used to decide whether a discovered product is new; because the game id is part of the key, two Discovery_Feeds for different games on the same retailer each track the same External_Id independently.
- **Alert**: A notification message describing a detected event, carrying an Alert_Kind. A product Alert describes a new-product, restock, price-change, or page-live event and may be a combined alert carrying both a restock and a price change for a single Listing in a single poll (only when both events are actually emitted in that poll). An admin Alert describes an operational warning or recovery notice.
- **Alert_Kind**: The classification of an Alert, one of `product` (a new-product, restock, price-change, page-live, or combined event delivered to non-admin Destinations) or `admin` (an operational warning, recovery notice, or Admin_Digest delivered to the Admin_Destination through the same durable outbox as product Alerts).
- **Admin_Digest**: A single admin Alert that aggregates every rate-limited operational warning coalesced per Scope_Key, emitted for a Scope_Key when un-digested notices exist for that Scope_Key AND at least 1 hour has elapsed since that Scope_Key's admin_scope_state.last_warning_at, so that no rate-limited warning is silently dropped; emitting the Admin_Digest updates that Scope_Key's admin_scope_state.last_warning_at, and an Admin_Digest for a Scope_Key fans out to all enabled, healthy Admin_Destinations. Recovery notices are exempt and do not update last_warning_at.
- **Alerts_Outbox**: The persistent table holding one row per emitted Alert of either Alert_Kind (`product` or `admin`), written in the same transaction as the state change or incident change that produced it, from which the Notifier fans out deliveries durably with retry and dedupe.
- **Alert_Deliveries**: The persistent table holding one row per (Alert, Destination) delivery attempt record, carrying alert_id, destination_id, status (one of `pending`, `sending`, `delivered`, or `expired`), a lease_until timestamp used while the row is in the `sending` status, attempts, next_attempt_at, and last_error, so that delivery state is tracked per Destination and a retry never re-sends to a Destination that already succeeded.
- **Sending_Status**: The transient `sending` delivery status set on an Alert_Deliveries row, together with a Lease, immediately before a worker POSTs the webhook; a `sending` row whose Lease has expired is reclaimed to `pending` and the reclamation counts as a delivery attempt.
- **Lease**: The time-limited claim a delivery worker places on an Alert_Deliveries row by marking it `sending` with a lease_until (defaulting to 60 seconds, configurable); once lease_until passes, another worker may reclaim the row to `pending`.
- **Delivery_Request_Budget**: The total in-request time budget, defaulting to 45 seconds and configurable, within which a single delivery's initial POST and its in-request retries must complete; if a required Retry-After or backoff wait would exceed the remaining budget, the Notifier stops retrying, returns the delivery to `pending` with next_attempt_at set to now plus the required wait, and frees the worker.
- **Delivery_Concurrency_Limit**: The configurable upper bound, defaulting to 10, on the number of concurrent destination deliveries the Notifier runs at once.
- **Alert_History**: The prunable, append-only log of emitted alerts, keyed by Subject and event type, written at emission in the same transaction as the Alerts_Outbox row; it is a historical record only and is NOT the authority for cooldown or dedupe decisions (that authority is the Alert_State).
- **Alert_State**: The persistent, never-pruned authority for cooldown and dedupe decisions, keyed by (Subject, event type) and holding rows only for event types that carry cooldown state (restock and price_change), with last_alert_at, last_alerted_value, and a pending flag; for a restock Subject the last_alerted_value is tracked as last_restock_alert_at and the pending flag is restock_pending, and for a price Subject the last_alerted_value is last_alerted_price. One-shot events (new_product and page_live) carry no cooldown state and write no Alert_State row, deduplicating via the Seen_Products store and stored existence respectively.
- **Subject**: The entity a cooldown and Alert_State record are keyed to: a Listing, or a (Discovery_Feed, External_Id) pair for new-product events. The Discovery_Feed carries the game id, so a new-product Subject is distinguished per game on the same retailer, consistent with the Seen_Products key (retailer id, game id, External_Id).
- **Cooldown**: A per-Subject, per-event-type time window during which a repeat alert of the same type is suppressed.
- **Last_Alerted_Price**: The price carried by a Listing's most recent price-change Alert, held as the last_alerted_value of the Listing's price_change Alert_State row (an Alert_State value, not a column on the listings row) and compared against each observed price to decide whether a new price-change Alert is due; it is the authority for the price-change Cooldown, distinct from the listings row's stored price, which holds the last OBSERVED price.
- **Baseline_Seeding**: The rule that the first complete, successful poll of a Discovery_Feed or Listing (or a feed/listing newly added or re-enabled via config) records state silently with no alert, sets the Listing's stored in_stock, upserts the Listing's Alert_State rows in the same transaction (the restock row with restock_pending set to false and last_restock_alert_at null; the price_change row with last_alerted_value set to the fetched price), sets the Subject's Seeded_At, and that a null or unseeded stored value never counts as a transition. The last_alerted_price lives in the price_change Alert_State row's last_alerted_value, not as a column on the listings row.
- **Seeded_At**: The persisted timestamp recorded on a feed or listing when its Baseline_Seeding completes; only a complete poll may set it.
- **Next_Due_At**: The persisted timestamp on a Discovery_Feed or Listing before which it is not due to be polled, set when a poll finishes to the finish time plus the entity's Poll_Interval plus Jitter; the per-retailer job uses the entity's effective due time (the later of Next_Due_At and backoff_until) to decide whether to poll it. A Retailer has no Next_Due_At; retailer-level scheduling and backoff are tracked in the Backoff_State.
- **Tick_Interval**: The fixed, configurable interval, defaulting to 5 seconds, at which each per-Retailer scheduled job ticks; on each tick the job selects and processes that Retailer's due feeds and listings rather than ticking at the Retailer's Poll_Interval.
- **Page_Live_Event**: A one-shot State_Transition emitted exactly once when a Listing's stored existence moves from false to existing, subject to Baseline_Seeding; it deduplicates purely on stored existence and is NOT subject to the Cooldown.
- **State_Transition**: A change in stored state that qualifies as an alertable event (new product, restock, price change, or page-live).
- **Variant**: A purchasable option within a Shopify product carrying its own `available` flag and `price`.
- **Poll_Interval**: The configured delay between successive polls of a Retailer, Discovery_Feed, or Listing.
- **Jitter**: A random offset added to a Poll_Interval to avoid synchronized requests.
- **Blocked**: A retailer response treated as active blocking: an HTTP 403; an HTTP 429; an HTTP 503 carrying a challenge indicator (a `cf-mitigated` header or a known challenge marker); a 2xx response whose body is HTML where a JSON feed was expected; or a detected captcha/challenge page. A plain 5xx response is an error, not Blocked.
- **Backoff_State**: The persisted failure counters and a backoff_until next-eligible-poll timestamp, keyed by scope (per-retailer, per-feed, and per-listing), used to drive exponential backoff for both ordinary errors and the Blocked condition; a feed's or listing's effective due time is the later of its Next_Due_At and its backoff_until, while a Retailer's retailer-wide effective due time comes from its backoff_until alone (a Blocked retailer's backoff defers all of that Retailer's feeds and listings). Retailer-level backoff is tracked here keyed by scope rather than on the retailers row.
- **Source**: A column on games, products, retailers, listings, discovery feeds, and destinations with the values `config` (the row originates from and is owned by the YAML configuration) or `bot` (the row was created by the later Discord bot process and is never modified by the Config_Loader).
- **Enabled_Flag**: A boolean flag on a configurable entity indicating whether it is currently active; the Config_Loader disables an entity by clearing this flag rather than deleting the row.
- **Max_Page_Count**: The per-feed maximum number of pages a discovery poll may request before it is treated as truncated, defaulting to 20 and configurable per Discovery_Feed.
- **Dry_Run_Mode**: An operational mode, enabled by a dry-run flag, in which polling and state changes run normally but every webhook POST is replaced by a log line containing the rendered Alert content and no webhook URL.
- **Once_Mode**: An operational mode, enabled by a `--once` flag, in which the Monitor runs exactly one poll cycle for all enabled retailers, drains the Alerts_Outbox by delivering all pending Alert_Deliveries, and then exits; during that single cycle it reclaims stale `sending` leases at start, treats enabled entities as due once (ignoring Next_Due_At and backoff), and treats all pending Alert_Deliveries as due once (ignoring next_attempt_at), honouring `Retry-After` only within the Delivery_Request_Budget and stopping after the Once_Mode_Drain_Timeout.
- **Once_Mode_Drain_Timeout**: The configurable upper bound, defaulting to 120 seconds, on how long Once_Mode continues draining pending Alert_Deliveries before it stops and exits.
- **Incident**: A persisted open-then-closed health condition backing the once-per-incident admin-warning behaviour, with a scope reference, a kind, opened_at, closed_at, and last_warning_at. Kinds are: `error` (a Retailer, feed, or listing with consecutive errors), `empty_result` (a feed with consecutive empty results), `truncated_feed` (a feed whose discovery poll reached the configured maximum page count), and `robots_disallowed` (a requested path disallowed by robots.txt). The Incident's last_warning_at records when that Incident last emitted a warning for historical and once-per-incident purposes; the per-Scope_Key hourly admin-warning rate limit and Admin_Digest timing are backed by the admin_scope_state store (keyed by Scope_Key), not by the Incident, because destination-scope and system-scope warnings have no Incident.
- **Scope_Key**: The coalescing key under which admin warnings are rate-limited and aggregated into an Admin_Digest, one of `retailer:<id>` (retailer-scope, used for retailer/feed/listing warnings and handle-change warnings), `destination:<id>` (destination-scope, used for permanent-webhook-failure and expired-delivery warnings tied to a specific Destination), or `system` (system-scope, used for warnings not tied to a retailer or destination); the hourly admin-warning rate limit and the Admin_Digest are computed per Scope_Key.
- **admin_notices**: The persistent store of pending suppressed admin warnings awaiting the next hourly Admin_Digest, keyed by a non-null Scope_Key (one of `retailer:<id>`, `destination:<id>`, or `system`) and the admin-warning rate-limit window, so that every rate-limited warning is coalesced and emitted rather than silently dropped; a warning is parked here only when its Scope_Key's admin_scope_state.last_warning_at is less than 1 hour old, and parked notices for a Scope_Key stay un-digested (including while no enabled, healthy Admin_Destination exists) until an Admin_Digest for that Scope_Key fires.
- **admin_scope_state**: The never-pruned persistent store keyed by Scope_Key as its primary key (one of `retailer:<id>`, `destination:<id>`, or `system`) carrying a non-null last_warning_at, used as the sole backing store for the per-Scope_Key hourly admin-warning rate limit and Admin_Digest timing across all scopes — including `destination:<id>` and `system` scopes that have no Incident. A warning for a Scope_Key sends immediately (updating last_warning_at) when last_warning_at is absent or at least 1 hour old; otherwise it is parked in admin_notices. Emitting an Admin_Digest also updates last_warning_at, while recovery notices are exempt and never update it.
- **Heartbeat**: A liveness signal that exists in two distinct forms. The in-memory monotonic heartbeats are a scheduler-liveness value and a dispatch-liveness value held in process memory and read only by the Watchdog. The scheduler-liveness value is beaten by an INDEPENDENT periodic liveness beat that runs on a fixed cadence and does not depend on any retailer job running or on any entity being due (so zero enabled retailers, or all retailers deferred by backoff, still beats it), with the per-entity beats emitted between entities within a poll being supplementary rather than the source. The dispatch-liveness value is beaten on each dispatch round and additionally at every await boundary of a webhook POST attempt (before the POST, after the response, and before and after each in-request retry sleep). The Heartbeat DB record (criterion 14.16) carries persisted scheduler-liveness and dispatch-liveness timestamps written at most every 5 seconds and is the signal the external container healthcheck reads; it is distinct from the in-memory heartbeats the Watchdog reads.
- **Watchdog**: The self-healing mechanism that runs on a dedicated daemon thread and compares the in-memory monotonic heartbeats (not the database) against the fixed Watchdog_Threshold values after a startup grace period; when a heartbeat is stale, it logs the stall and hard-exits the process with a non-zero status so the container restart policy restarts it. The Watchdog is DISABLED in Once_Mode, where the run is bounded by the Once_Mode_Drain_Timeout instead; and on SIGTERM the Monitor stops the Watchdog first, before cancelling in-flight polls or deliveries, so that the graceful-shutdown quiet period cannot trip it.
- **Watchdog_Threshold**: The fixed, configurable staleness bounds beyond which an in-memory liveness heartbeat is considered stale, defaulting to 120 seconds for the scheduler-liveness heartbeat and 60 seconds for the dispatch-liveness heartbeat, applied only after a configurable startup grace period defaulting to 120 seconds; staleness triggers the Watchdog to log and hard-exit.

## Requirements

### Requirement 1: Polling, Timeouts, and Scheduling Discipline

**User Story:** As a retailer, I want the Monitor to issue requests in a disciplined, bounded way, so that it does not overlap runs, hang indefinitely, or hammer my site.

#### Acceptance Criteria

1. WHEN the Scheduler triggers a Discovery_Feed poll, THE Watcher_Core SHALL invoke the matching Adapter's discover operation for that Discovery_Feed within 5 seconds of the trigger.
2. WHEN an Adapter issues an individual HTTP request, THE Adapter SHALL abort that request if no response is received within 30 seconds.
3. WHEN a poll of a Retailer begins, THE Watcher_Core SHALL abort the remaining work of that poll if the poll has not completed within an overall per-poll cap defaulting to 5 minutes.
4. THE Watcher_Core SHALL issue at most one in-flight HTTP request at a time per Retailer host and SHALL leave a minimum spacing of 1 second between successive requests to the same Retailer host; this rule applies to Retailer hosts only, and Discord webhook delivery is governed solely by Requirement 10 and Discord's own rate-limit responses rather than by this host-spacing rule.
5. THE Scheduler SHALL run exactly one scheduled job per Retailer, and that job SHALL tick at a fixed, configurable Tick_Interval defaulting to 5 seconds rather than at the Retailer's Poll_Interval; IF the Retailer's retailer-wide effective due time is in the future when the job ticks (that is, the Retailer is in retailer-wide backoff), THEN THE Watcher_Core SHALL defer the entire job for that Retailer and process none of that Retailer's discovery feeds and listings; OTHERWISE, on each tick the job SHALL select that Retailer's discovery feeds and listings whose effective due time has passed and process them.
6. WHEN the per-retailer job runs on a tick, THE Watcher_Core SHALL select each Discovery_Feed and Listing WHERE its effective due time is less than or equal to the current time, ORDER the selected entities BY effective due time ascending, and process them sequentially; THE Retailer's Poll_Interval SHALL serve as the default Poll_Interval inherited by that Retailer's discovery feeds and listings that do not set their own Poll_Interval.
7. WHEN a poll of a Discovery_Feed or Listing finishes, THE Watcher_Core SHALL set that feed's or listing's Next_Due_At to the finish time plus that feed's or listing's own Poll_Interval plus Jitter.
8. THE Watcher_Core SHALL compute a feed's or listing's effective due time as the later of that entity's Next_Due_At and its backoff_until in the Backoff_State, and SHALL compute a Retailer's retailer-wide effective due time from its backoff_until in the Backoff_State alone (a Retailer has no Next_Due_At), applying the effective-due rule to Retailers, discovery feeds, and listings alike.
9. WHEN a scheduled job for a Retailer is already running at its next trigger time, THE Scheduler SHALL coalesce the missed run rather than start an overlapping run of the same job.
10. WHEN the Monitor starts, THE Scheduler SHALL stagger the initial polls across retailers rather than triggering all retailers simultaneously.

### Requirement 2: Baseline Seeding

**User Story:** As an operator, I want the first observation of any feed or listing to be recorded silently, so that enabling monitoring does not produce a burst of spurious alerts.

#### Acceptance Criteria

1. WHEN a Discovery_Feed or Listing completes its first successful poll and no prior state is stored for that Subject, THE Watcher_Core SHALL record the observed state on the listing (storing the fetched price as the last OBSERVED price), upsert that Listing's Alert_State rows in the same transaction (the restock row with restock_pending set to false and last_restock_alert_at null; the price_change row with last_alerted_value set to the fetched price), set that feed's or listing's Seeded_At, and SHALL emit no Alert.
2. WHEN a Discovery_Feed or Listing is newly added via configuration, or is re-enabled after having been disabled, THE Watcher_Core SHALL treat its next complete successful poll as a Baseline_Seeding poll and SHALL emit no Alert for that poll.
3. WHERE a stored value for a Subject is null or the Subject has no Seeded_At, THE Watcher_Core SHALL treat that value as unseeded and SHALL NOT emit a restock, price-change, or page-live Alert on the basis of a transition from that unseeded value.
4. IF a poll ends by reaching the maximum page count, the per-poll cap (Requirement 1), a timeout, or an error, THEN THE Watcher_Core SHALL NOT set the Seeded_At for that feed or listing and SHALL leave any existing Seeded_At unchanged.
5. WHEN a discovery poll reaches the configured maximum page count, THE Watcher_Core SHALL open a `truncated_feed` Incident for that feed and send a single admin warning through that Incident rather than stopping silently, and SHALL close that Incident with a recovery notice when a later poll of that feed completes without reaching the maximum page count (Requirement 17).

### Requirement 3: New Product Discovery

**User Story:** As a card collector, I want to be alerted when a retailer lists a new product, including pages that exist before they are linked or announced, so that I can act before the product is widely known.

#### Acceptance Criteria

1. WHEN an Adapter's discover operation returns a product whose (retailer id, game id, External_Id) is absent from the Seen_Products store, THE Watcher_Core SHALL emit exactly one new-product State_Transition for that product, subject to Baseline_Seeding (Requirement 2), where the game id is the Discovery_Feed's game id.
2. WHEN comparing discovered products against the Seen_Products store, THE Watcher_Core SHALL match products by (retailer id, game id, External_Id) using exact string comparison, independently of feed ordering.
3. WHERE a Discovery_Feed defines include keywords, THE Watcher_Core SHALL emit new-product events only for products where at least one include keyword matches the product title, product_type, vendor, or tags as a Unicode-normalised, accent-insensitive and case-insensitive substring.
4. WHERE a Discovery_Feed defines exclude keywords, THE Watcher_Core SHALL suppress new-product events for products where at least one exclude keyword matches the product title, product_type, vendor, or tags as a Unicode-normalised, accent-insensitive and case-insensitive substring, and this suppression SHALL take precedence over any include-keyword match.
5. WHEN a discover operation returns a product, THE Watcher_Core SHALL record that product under its (retailer id, game id, External_Id) key in the Seen_Products store regardless of whether any include keyword matched, whether an exclude keyword matched, or whether an Alert was emitted, so that a later edit to keyword filters does not retroactively produce a new-product Alert.
6. WHEN an Adapter's discover operation returns a product that matches an enabled Listing on the same Retailer by equal External_Id OR, where no stored External_Id is available to compare, equal Handle, THE Watcher_Core SHALL record that product under its (retailer id, game id, External_Id) key in the Seen_Products store (consistent with criterion 3.5) but SHALL emit no new-product Alert for that product, because the matched Listing already covers that product's page-live, restock, and price-change events.
7. WHEN a new-product State_Transition is written, THE Watcher_Core SHALL persist the discovered product under its (retailer id, game id, External_Id) key in the Seen_Products store in the same transaction as the alert (Requirement 6).
8. IF an Adapter's discover operation fails, raises an error, or exceeds the per-poll cap (Requirement 1), THEN THE Watcher_Core SHALL abort processing for that Discovery_Feed poll, emit no new-product State_Transition for that poll, retain existing stored state unchanged, not set the feed's Seeded_At, and record a failure indication in the Backoff_State for that Discovery_Feed.

### Requirement 4: Restock Detection

**User Story:** As a card collector, I want to be alerted when an out-of-stock item becomes available, so that I can purchase it before it sells out again.

#### Acceptance Criteria

1. THE Watcher_Core SHALL define a restock transition as the Listing's observed in_stock status going from false (the stored in_stock status) to true (the fetched Product_State in_stock status).
2. WHERE the Adapter is the Shopify_Adapter, THE Watcher_Core SHALL treat a Listing as in stock when at least one Variant reports `available` as true, and as out of stock when zero Variants report `available` as true.
3. WHEN a restock transition is observed for a Listing, subject to Baseline_Seeding (Requirement 2), IF no prior restock Alert exists for that Listing OR the time since the Listing's last_restock_alert_at is at least the Cooldown (Requirement 9), THEN THE Watcher_Core SHALL emit exactly one restock State_Transition for that Listing and set the Listing's last_restock_alert_at to the detection time; OTHERWISE THE Watcher_Core SHALL set the Listing's restock_pending to true and emit no Alert.
4. WHILE a Listing's restock_pending is true, WHEN the Watcher_Core polls that Listing and the observed in_stock status is false, THE Watcher_Core SHALL clear the Listing's restock_pending to false and emit no Alert.
5. WHILE a Listing's restock_pending is true, WHEN the Watcher_Core polls that Listing and the observed in_stock status is true and at least the Cooldown has elapsed since the Listing's last_restock_alert_at, THE Watcher_Core SHALL emit exactly one trailing restock State_Transition, set the Listing's last_restock_alert_at to the detection time, and clear the Listing's restock_pending to false.
6. WHEN a Listing's observed in_stock status goes from true to false, THE Watcher_Core SHALL update the Listing's stored in_stock status to false and SHALL emit no out-of-stock Alert.
7. WHEN a later restock transition is observed after the Listing has gone out of stock, THE Watcher_Core SHALL treat it as a new restock transition and emit a restock State_Transition subject to the same Cooldown (Requirement 9).
8. WHEN a restock State_Transition is written, or when the Listing's observed in_stock status changes, THE Watcher_Core SHALL update the Listing's stored in_stock status in the same transaction as the alert (Requirement 6).
9. WHILE a Listing's fetched in_stock status equals the stored in_stock status and restock_pending is false, THE Watcher_Core SHALL emit no restock event for that Listing.
10. WHEN a restock event and a price-change event are both emitted for the same Listing in the same poll, THE Watcher_Core SHALL emit exactly one combined Alert conveying both events rather than two separate Alerts; IF either event is deferred by its Cooldown in that poll (the restock sets restock_pending, or the price change falls within its Cooldown), THEN THE Watcher_Core SHALL NOT combine, and SHALL deliver each actually-emitted event as a separate Alert (Requirement 5, Requirement 8, Requirement 9).
11. IF an Adapter's fetch-state operation fails, raises an error, times out, or returns a malformed or empty Product_State, THEN THE Watcher_Core SHALL emit no restock State_Transition, retain the Listing's stored in_stock status and restock_pending unchanged, and record a failure indication in the Backoff_State for that Listing.

### Requirement 5: Price Change Detection

**User Story:** As a card collector, I want to be alerted when the price of a tracked item changes, so that I can decide whether to buy at the new price.

#### Acceptance Criteria

1. WHEN a fetched Product_State price differs from the Listing's last_alerted_price under exact Decimal comparison AND at least the Cooldown (Requirement 9) has elapsed since the Listing's last price-change Alert, THE Watcher_Core SHALL emit exactly one price-change State_Transition for that Listing and set the Listing's last_alerted_price to the fetched price, subject to Baseline_Seeding (Requirement 2).
2. WHERE the Adapter is the Shopify_Adapter, THE Watcher_Core SHALL compute the Product_State price as the lowest `price` among available Variants, or the lowest `price` across all Variants when no Variant is available, after the Shopify_Adapter has normalised every price to Decimal in major units (Requirement 11).
3. WHEN a price-change State_Transition is emitted, THE Watcher_Core SHALL record both the previous price and the new price in the event.
4. WHEN a price-change State_Transition is written, THE Watcher_Core SHALL update the Listing's stored (last observed) price to the new price and set the price_change Alert_State row's last_alerted_value to the new price, both in the same transaction as the alert (Requirement 6).
5. WHILE a fetched price equals the Listing's last_alerted_price under exact Decimal comparison, THE Watcher_Core SHALL emit no price-change event for that Listing.
6. WHEN a price-change event and a restock event are both emitted for the same Listing in the same poll, THE Watcher_Core SHALL emit a single combined Alert conveying both events rather than two separate Alerts; IF either event is deferred by its Cooldown in that poll, THEN THE Watcher_Core SHALL NOT combine, and SHALL deliver each actually-emitted event as a separate Alert (Requirement 4, Requirement 8, Requirement 9).
7. IF a fetched Product_State price is missing, non-numeric, negative, or outside the range 0.00 to 999,999,999.99, THEN THE Watcher_Core SHALL emit no price-change State_Transition, retain the Listing's stored price unchanged, and record a failure indication in the Backoff_State for that Listing.

### Requirement 6: Atomic State, Alert, and Delivery Persistence

**User Story:** As an operator, I want each detected event, its state change, its history record, and its per-destination deliveries written together, so that no alert is lost and no event is double-counted across restarts.

#### Acceptance Criteria

1. WHEN the Watcher_Core detects a State_Transition, THE Watcher_Core SHALL write, within a single database transaction, the updated stored state, the updated Alert_State row for that Subject and event type, one Alerts_Outbox row, the Alert_History row for that Subject and event type, and one Alert_Deliveries row for each Destination to which the Alert must be delivered.
2. WHEN the Watcher_Core writes the Alert_Deliveries rows for an Alert, THE Watcher_Core SHALL fan the Alert out to every matching Destination inside the same transaction as the state change.
3. IF the transaction that writes a State_Transition, its Alert_State row, its Alerts_Outbox row, its Alert_History row, and its Alert_Deliveries rows fails, THEN THE Watcher_Core SHALL leave the stored state and Alert_State unchanged so that the event is re-detected on the next poll.
4. THE Notifier SHALL deliver Alerts by reading pending Alert_Deliveries rows joined to their Alerts_Outbox rows.
5. WHEN the Monitor restarts, THE Monitor SHALL retain undelivered Alert_Deliveries rows and resume delivering them.
6. WHEN the Watcher_Core sets or clears a Listing's restock_pending without emitting an Alert, THE Watcher_Core SHALL write that restock_pending change in a single database transaction together with any Alert_State change for that Subject.
7. WHEN the Watcher_Core updates a Listing's stored in_stock status, stored existence, or consecutive-404 count without emitting an Alert, THE Watcher_Core SHALL write that update in a single database transaction together with any Alert_State change for that Subject.

### Requirement 7: Page-Existence Transitions

**User Story:** As a card collector, I want to be alerted when a product page goes live, so that I can act the moment a listing becomes reachable.

#### Acceptance Criteria

1. WHEN the Shopify_Adapter fetches a Listing, THE Shopify_Adapter SHALL treat an HTTP 404 on the product endpoint as a "not found" result and SHALL NOT treat absence from a collection feed or a truncated crawl as "not found".
2. WHEN an Adapter cannot complete a fetch because the request raised an error, THE Adapter SHALL return an error result rather than a "not found" result.
3. WHEN a Listing's first poll returns a "not found" result, THE Watcher_Core SHALL store the Listing's existence as false (not null) immediately, so that a later transition to existing can fire a Page_Live_Event.
4. WHEN a Listing's fetched Product_State existence is NOT_FOUND, THE Watcher_Core SHALL NOT write the Listing's stored in_stock, the Listing's stored (last observed) price, or the price_change Alert_State row's last_alerted_value for that poll, leaving the Listing's in_stock and its price_change Alert_State unseeded (null) until the page exists.
5. WHEN a Listing's stored existence is false and the fetched Product_State existence is existing, THE Watcher_Core SHALL emit exactly one Page_Live_Event for that Listing as a one-shot event that dedupes purely on stored existence, subject to Baseline_Seeding (Requirement 2) and NOT subject to the Cooldown (Requirement 9).
6. WHEN a Page_Live_Event is emitted, THE Watcher_Core SHALL, in the same transaction as the alert (Requirement 6), include the fetched stock status and price in the Page_Live_Event, set the Listing's stored in_stock and the Listing's stored (last observed) price to the fetched values, upsert the Listing's price_change Alert_State row's last_alerted_value to the fetched price, and SHALL NOT emit a separate restock or price-change Alert for that Listing in that poll.
7. WHEN a Page_Live_Event is written, THE Watcher_Core SHALL update the Listing's stored existence to existing in the same transaction as the alert (Requirement 6).
8. WHILE a Listing's stored existence is existing, THE Watcher_Core SHALL store the existence as false only after 2 consecutive "not found" (HTTP 404) results, tracking the consecutive-404 count on the Listing and resetting that count to zero on any existing result.
9. IF an Adapter cannot determine existence because the fetch raised an error rather than returning a "not found" result, THEN THE Watcher_Core SHALL emit no Page_Live_Event, leave the consecutive-404 count unchanged, and SHALL record a failure indication in the Backoff_State for that Listing.

### Requirement 8: Alert Delivery to Discord Destinations

**User Story:** As a server administrator, I want alerts delivered to the correct Discord servers, channels, and roles safely, so that each community receives relevant notifications without unwanted pings.

#### Acceptance Criteria

1. WHEN the Watcher_Core emits a State_Transition for a product belonging to a Game, THE Notifier SHALL deliver exactly one Alert to every non-admin Destination configured for that Game whose Enabled_Flag is set and whose health_status is `healthy`, driven by that Alert's Alert_Deliveries rows, and SHALL skip any Destination whose Enabled_Flag is clear or whose health_status is `unhealthy` during fan-out.
2. WHEN the Notifier delivers an Alert, THE Notifier SHALL format the Alert as a Discord embed that shows the event type, the title, stock status, product link, and image, truncating each embed field to Discord's documented limits.
3. WHEN the Notifier delivers a price-change Alert, THE Notifier SHALL show both the previous price and the new price.
4. WHEN the Notifier delivers a combined Alert conveying both an emitted restock and an emitted price change for one Listing (Requirement 4, Requirement 5), THE Notifier SHALL present both events within a single message; WHERE only one of the two events was emitted because the other was deferred by its Cooldown, THE Notifier SHALL deliver the single emitted event as its own message rather than a combined message (Requirement 9).
5. WHEN the Notifier delivers an Alert, THE Notifier SHALL set `allowed_mentions` so that only the Destination's configured role may be pinged and SHALL never ping `@everyone` or `@here`.
6. WHERE a Destination defines a role to ping, THE Notifier SHALL include that role mention in the delivered Alert.
7. WHEN two or more Destinations are configured for the same Game, THE Notifier SHALL deliver the Alert to each configured Destination independently, such that a delivery failure to one Destination does not prevent delivery to the remaining Destinations.
8. WHEN the Notifier delivers an Alert, THE Notifier SHALL resolve each Destination's webhook URL from the environment variable named by that Destination and send the Alert to that webhook target.
9. WHERE multiple Alerts are pending for a single Destination, THE Notifier SHALL batch them into one message of up to 10 embeds and up to 6000 characters total, splitting the pending Alerts across additional messages whenever either limit would be exceeded.
10. WHEN the Notifier includes retailer-supplied text in an embed, THE Notifier SHALL escape Discord markdown in that text.
11. WHEN the Notifier sets a link or image URL on an embed, THE Notifier SHALL accept only `http` or `https` URLs and SHALL omit any URL using another scheme.

### Requirement 9: Alert Deduplication and Cooldown

**User Story:** As an alert recipient, I want to receive one notification per real event with a final update after quiet periods, so that I am neither spammed nor left with stale information.

#### Acceptance Criteria

1. WHEN the Watcher_Core emits a State_Transition, THE Watcher_Core SHALL update the Alert_State for its Subject and event type with the detection timestamp and the alerted value, and SHALL append the Alert to the Alert_History with its Subject, event type, and detection timestamp, both in the same transaction as the Alerts_Outbox row (Requirement 6).
2. THE Monitor SHALL use the Alert_State as the sole authority for cooldown and dedupe decisions, SHALL evaluate the relevant rule on every poll, and SHALL NOT read the Alert_History to make those decisions.
3. WHEN the Watcher_Core polls a Listing for a price-change event, THE Watcher_Core SHALL emit a price-change Alert if and only if the observed price differs from the Listing's last_alerted_price AND at least the Cooldown has elapsed since the last price-change Alert (Requirement 5).
4. WHEN the Watcher_Core polls a Listing for a restock event, THE Watcher_Core SHALL apply the transition-based and deferred (restock_pending) rules of Requirement 4 rather than a boolean last-alerted-state comparison.
5. IF an observed price changes and then reverts to the last_alerted_price within the Cooldown window, THEN THE Monitor SHALL emit no price-change Alert for that reversion.
6. THE Alert_State SHALL hold rows only for event types that carry cooldown state (restock and price_change); WHERE the event is a one-shot event (new-product or page-live), THE Monitor SHALL deduplicate using the Seen_Products store or the stored existence respectively rather than the Cooldown rule and SHALL write no Alert_State row for that event.
7. THE Monitor SHALL apply the Cooldown independently on a per-Subject, per-event-type basis, defaulting to 300 seconds when no Subject-specific Cooldown is configured.

### Requirement 10: Alert Timeliness and Delivery Durability

**User Story:** As a card collector, I want alerts to arrive promptly after a change occurs and to be retried durably without duplicates, so that time-sensitive drops are still actionable.

#### Acceptance Criteria

1. WHILE a Destination is reachable and not rate limited, WHEN the Watcher_Core detects a State_Transition during a poll, THE Monitor SHALL deliver the corresponding Alert to that Destination within 60 seconds of detection.
2. THE Notifier SHALL run a dispatch loop that scans pending Alert_Deliveries rows at an interval of at most 5 seconds.
3. THE Notifier SHALL bound the total number of concurrent destination deliveries with a configurable Delivery_Concurrency_Limit defaulting to 10.
4. WHEN the Notifier begins delivering an Alert_Deliveries row, THE Notifier SHALL, before issuing the webhook POST, mark that row `sending` with a lease_until set to the current time plus a configurable Delivery_Lease defaulting to 60 seconds, in a short database transaction.
5. IF an Alert_Deliveries row is in the `sending` status and its lease_until has passed, THEN THE Notifier SHALL reclaim that row to `pending` and SHALL count the reclamation as a delivery attempt.
6. WHEN a webhook delivery to a Destination fails or returns a rate-limit response, THE Notifier SHALL retry delivery in-request using exponential backoff, honouring the `Retry-After` header on an HTTP 429 response, up to 3 additional attempts within that request.
7. WHEN the Notifier delivers an Alert_Deliveries row, THE Notifier SHALL complete that delivery's in-request effort, comprising the initial POST plus its in-request retries, within a configurable Delivery_Request_Budget defaulting to 45 seconds; IF a required `Retry-After` or backoff wait would exceed the remaining Delivery_Request_Budget, THEN THE Notifier SHALL stop retrying in-request, set that delivery back to `pending` with next_attempt_at set to the current time plus the required wait, and free the worker.
8. IF the in-request attempts for a Destination are exhausted without success within the Delivery_Request_Budget, THEN THE Notifier SHALL update that Alert_Deliveries row with the attempt count, last error, and a next_attempt_at computed from capped exponential backoff, and SHALL continue delivery to all other Destinations.
9. WHILE an Alert_Deliveries row remains undelivered and is less than 1 hour old, THE Notifier SHALL retry it at its next_attempt_at using capped exponential backoff.
10. IF an Alert_Deliveries row has been retried for more than 1 hour without success, THEN THE Notifier SHALL mark that delivery expired and emit an admin warning of Alert_Kind `admin` through the Alerts_Outbox (Requirement 17), where that warning is subject to the hourly per-Scope_Key admin-warning rate limit under the destination-scope Scope_Key (`destination:<id>`) for the affected Destination and is coalesced per Scope_Key per window into an Admin_Digest rather than sent as one message per expired delivery.
11. IF an Alert is older than a configurable maximum age defaulting to 1 hour, THEN THE Notifier SHALL NOT deliver that Alert late and SHALL mark its pending deliveries expired.
12. WHEN a delivery to a Destination has already succeeded, THE Notifier SHALL NOT re-send that Alert to that Destination on any subsequent retry.
13. IF a webhook returns HTTP 401, 403, or 404, THEN THE Notifier SHALL treat the failure as permanent, perform no further retries for that delivery, set the Destination's health_status to `unhealthy` with its unhealthy_since and last_error, and emit an admin warning of Alert_Kind `admin` through the Alerts_Outbox (Requirement 17), where that warning is subject to the hourly per-Scope_Key admin-warning rate limit under the destination-scope Scope_Key (`destination:<id>`) for the failing Destination and is coalesced per Scope_Key per window into an Admin_Digest rather than sent as one message per permanent failure; WHERE the permanently failing Destination X is itself an Admin_Destination, THE Notifier SHALL route that warning to the OTHER enabled, healthy Admin_Destinations and SHALL NOT route it through Destination X, and IF no other enabled, healthy Admin_Destination exists THEN THE Notifier SHALL only log the failure (with the webhook URL redacted) rather than emit an admin warning (Requirement 17).
14. WHILE a Destination's health_status is `unhealthy` OR a Destination's Enabled_Flag is clear, THE Notifier SHALL skip that Destination during fan-out, and WHILE a Destination's health_status is `unhealthy` THE Notifier SHALL additionally mark that Destination's pending Alert_Deliveries rows expired.
15. WHILE a Destination's health_status is `unhealthy`, THE Notifier SHALL probe that Destination with an HTTP GET on its resolved webhook URL every 15 minutes and SHALL NOT write that webhook URL to the log output (Requirement 18).
16. WHEN a probe of an unhealthy Destination succeeds, THE Notifier SHALL set that Destination's health_status to `healthy`, clear its unhealthy_since, and emit a recovery notice of Alert_Kind `admin` to the Admin_Destination through the Alerts_Outbox, exempt from the hourly admin-warning rate limit (Requirement 17).
17. WHEN the Monitor restarts, THE Notifier SHALL reset a Destination to `healthy` if a probe of that Destination succeeds.

### Requirement 11: Adapter Interface and Extensibility

**User Story:** As a developer, I want all retailers served through one adapter interface resolved from a registry, so that adding support for new sites does not require changes to the core.

#### Acceptance Criteria

1. THE Monitor SHALL define a single Adapter interface providing a fetch-state operation that returns a Product_State and a discover operation that returns a list of products.
2. THE Product_State SHALL contain existence, in-stock status, price, per-variant detail, URL, image, and title fields.
3. THE Monitor SHALL provide an Adapter_Registry that resolves a Retailer's adapter type string to its Adapter implementation.
4. WHEN the Shopify_Adapter fetches a single Listing's state, THE Shopify_Adapter SHALL request `GET {base}/products/{Handle}.js` and SHALL NOT use `/products/{Handle}.json` for stock, because the `.json` variant omits the `available` flag.
5. WHEN the Shopify_Adapter reads a price, THE Shopify_Adapter SHALL normalise every price to Decimal in major units before storing or comparing, treating the `.js` endpoint's integer values as Minor_Units converted using the Retailer's Minor_Unit_Exponent (default 2) and parsing a collection feed's decimal string directly as major units.
6. WHERE the same External_Id was already fetched by a Discovery_Feed in the same poll cycle and the feed data contains the fields needed to build the Product_State, THE Shopify_Adapter SHALL reuse that feed data rather than issue another request for that Listing.
7. WHEN the Shopify_Adapter learns a Listing's External_Id, THE Monitor SHALL store that External_Id on the Listing.
8. IF a Listing's stored External_Id later appears in a feed under a different Handle, THEN THE Watcher_Core SHALL update the Listing URL to the new Handle, emit a handle-change admin warning of Alert_Kind `admin` through the Alerts_Outbox (Requirement 17) where that warning is subject to the hourly per-Scope_Key admin-warning rate limit under the retailer-scope Scope_Key (`retailer:<id>`) for the Listing's Retailer and is coalesced per Scope_Key per window into an Admin_Digest rather than sent as one message per handle change, and SHALL NOT emit a Page_Live_Event.
9. WHEN the Watcher_Core polls a Listing or Discovery_Feed, THE Watcher_Core SHALL select the Adapter indicated by the Retailer's adapter type via the Adapter_Registry.
10. THE Monitor SHALL provide a Shopify_Adapter that reads collection product JSON feeds and, as a general capability, the full-catalogue `/products.json` feed for discovery.
11. WHERE the Phase 2 scope is implemented, THE Monitor SHALL provide a Sitemap_Adapter that discovers new product URLs by diffing `sitemap.xml`. (Phase 2)
12. WHERE the Phase 2 scope is implemented, THE Monitor SHALL provide a Generic_HTML_Adapter whose extraction is driven by CSS selectors defined in YAML configuration. (Phase 2)
13. WHERE the Phase 2 scope is implemented, THE Monitor SHALL support Custom_Adapter implementations as Python classes for sites not served by the standard adapters. (Phase 2)
14. WHERE the Phase 2 scope is implemented and a site cannot be served by any other Adapter, THE Monitor SHALL support a Playwright-based Adapter as an optional fallback. (Phase 2)

### Requirement 12: Discovery Scope and Keyword Matching

**User Story:** As an operator, I want discovery feeds scoped to relevant collections with precise keyword matching, so that I detect the right products without crawling the entire catalogue.

#### Acceptance Criteria

1. THE Monitor's MVP discovery feeds SHALL be `/collections/pokemon/products.json` and `/collections/pre-orders-1/products.json`.
2. THE `/collections/pre-orders-1/products.json` Discovery_Feed SHALL define the include keyword `pokemon` and the same configurable exclude keywords as the `/collections/pokemon/products.json` feed (`plush`, `playmat`, `sleeves`, and `binder`).
3. WHERE a Full_Catalogue_Feed backed by `/products.json` is configured, THE Shopify_Adapter SHALL support it as a general capability, that feed SHALL define include keywords, and that feed SHALL use its own longer Poll_Interval defaulting to 15 minutes.
4. WHEN the per-retailer job runs, THE Watcher_Core SHALL poll each Discovery_Feed only when that feed's Next_Due_At has passed, using that feed's own Poll_Interval.
5. WHEN discovering products from a Discovery_Feed, THE Shopify_Adapter SHALL read the feed using pagination with `limit=250` and incrementing `page` values.
6. WHEN paginating a feed, THE Shopify_Adapter SHALL stop requesting further pages when a page returns fewer than `limit` items or returns an empty page, and SHALL not exceed the Max_Page_Count, which defaults to 20 pages and is configurable per Discovery_Feed.
7. WHEN comparing discovered products, THE Watcher_Core SHALL match products by (retailer id, game id, External_Id) against the Seen_Products store without assuming feed ordering.

### Requirement 13: Configuration-Driven Setup

**User Story:** As an operator, I want to add games, products, retailers, listings, and feeds by editing configuration, so that routine additions require no code changes.

#### Acceptance Criteria

1. WHEN the Monitor starts, THE Config_Loader SHALL read the YAML configuration for games, products, retailers, listings, discovery feeds, and destinations.
2. WHEN the Config_Loader loads the configuration, THE Config_Loader SHALL upsert entities by their stable ids and SHALL NOT reset stored Listing or feed state on reload.
3. THE Config_Loader SHALL only upsert, disable, and re-enable rows WHERE Source equals `config`, and SHALL NEVER create, modify, disable, or delete a row WHERE Source equals `bot`.
4. WHEN an entity previously present in configuration is absent on reload, THE Config_Loader SHALL disable that entity by clearing its Enabled_Flag rather than delete it or its stored state.
5. WHEN the Config_Loader re-enables a previously disabled entity, THE Config_Loader SHALL set its Enabled_Flag and clear its Seeded_At so that the entity is re-baselined on its next complete poll (Requirement 2).
6. WHEN the Config_Loader validates the configuration, THE Config_Loader SHALL fail fast and report every validation error found in a single pass, including an unknown adapter type, a Destination referencing an unknown game, a referenced environment variable that is not set, and, IF a YAML entity id already exists as a row WHERE Source equals `bot`, a validation error naming that id.
7. WHERE a new Product, Listing, or Discovery_Feed is defined in configuration for a supported adapter type, THE Monitor SHALL support it without changes to core code.
8. THE Config_Loader SHALL read secrets from environment variables rather than from the YAML configuration.
9. WHEN a Product is associated with a Retailer, THE Config_Loader SHALL represent the association as a Listing.

### Requirement 14: Persistent Data Model

**User Story:** As an operator, I want state and targets stored durably and safely for concurrent access, so that the system continues correctly across restarts and alongside the later bot process.

#### Acceptance Criteria

1. THE Monitor SHALL store games with an identifier, a name, a Source, and an Enabled_Flag.
2. THE Monitor SHALL store products with an identifier, a game reference, a name, a category or set, a Source, and an Enabled_Flag.
3. THE Monitor SHALL store retailers with an identifier, a name, an adapter type, a base URL, a Poll_Interval serving as the inherited default for the Retailer's feeds and listings, a Minor_Unit_Exponent, a Source, and an Enabled_Flag, and SHALL NOT store a Next_Due_At on the retailers row because retailer-level scheduling and backoff are tracked via the Backoff_State keyed by scope.
4. THE Monitor SHALL store listings with a product reference, a retailer reference, a URL, a Handle, an External_Id, the last known state (in_stock, the last OBSERVED price, and existence), a consecutive-404 counter, a per-listing Poll_Interval, a Next_Due_At, a Seeded_At, a Source, and an Enabled_Flag, and SHALL NOT store a last_alerted_price column on the listings row because the last alerted price is held in the price_change Alert_State row's last_alerted_value (criterion 14.10).
5. THE Monitor SHALL store discovery feeds with a retailer reference, a game reference, a feed URL, include and exclude keywords, a per-feed Poll_Interval, a Next_Due_At, a Seeded_At, a Source, and an Enabled_Flag.
6. THE Monitor SHALL store a Seen_Products store keyed by (retailer id, game id, External_Id).
7. THE Monitor SHALL define a canonical External_Id per adapter: the Shopify product id as a string for the Shopify_Adapter, and a normalised URL for the Sitemap_Adapter and Generic_HTML_Adapter.
8. THE Monitor SHALL store an alerts_outbox table holding one row per emitted Alert.
9. THE Monitor SHALL store an alert_deliveries table holding one row per (alert, destination) with alert_id, destination_id, status (one of `pending`, `sending`, `delivered`, or `expired`), a lease_until timestamp used while a row is `sending`, attempts, next_attempt_at, and last_error.
10. THE Monitor SHALL store an alert_state table keyed by (Subject, event type) holding last_alert_at, last_alerted_value, and a pending flag, where for a restock Subject last_alert_at is the last_restock_alert_at and pending is restock_pending, and for a price Subject the last_alerted_value holds the last ALERTED price (the sole authority for the price-change Cooldown, distinct from the listings row's last OBSERVED price); a Subject is a Listing or a (Discovery_Feed, External_Id) pair for new-product events, and the alert_state table SHALL NEVER be pruned.
11. THE Monitor SHALL store an alert_history table as a prunable, append-only log keyed by Subject and event type, recording detection timestamps for historical reporting only and not used as the authority for cooldown or dedupe decisions.
12. THE Monitor SHALL store Backoff_State comprising failure counters and a backoff_until timestamp per retailer, per feed, and per listing.
13. THE Monitor SHALL store an incidents table with a scope reference, a kind of `error`, `empty_result`, `truncated_feed`, or `robots_disallowed`, opened_at, closed_at, and last_warning_at.
14. THE Monitor SHALL store destinations with a server identifier, an optional game reference, a channel or webhook-environment-variable-name target, an optional role to ping, an Admin_Destination_Flag, a health_status of `healthy` or `unhealthy`, an unhealthy_since timestamp, a last_error, a next_probe_at timestamp, a last_probe_at timestamp, a Source, and an Enabled_Flag, where a Destination with the Admin_Destination_Flag set requires no game reference.
15. THE Monitor SHALL store an admin_notices table holding pending suppressed admin warnings awaiting the hourly Admin_Digest, keyed by a non-null Scope_Key (one of `retailer:<id>`, `destination:<id>`, or `system`) and the admin-warning rate-limit window, so that no rate-limited warning is dropped before the window ends (Requirement 17).
16. THE Monitor SHALL store a heartbeat record carrying a scheduler-liveness timestamp and a dispatch-liveness timestamp, written at most every 5 seconds and read by the container healthcheck as its liveness signal, distinct from the in-memory monotonic heartbeats the Watchdog reads (Requirement 19).
17. THE Monitor SHALL store all monetary prices as TEXT Decimal values in SQLite.
18. THE Monitor SHALL apply versioned schema migrations.
19. THE Monitor SHALL open its SQLite database in WAL journal mode with a configured `busy_timeout`, so that a separate Discord bot process can share the database.
20. THE Monitor SHALL store all timestamps in UTC.
21. WHEN the Monitor restarts, THE Monitor SHALL preserve all configured destinations and all last known Listing state.
22. THE Monitor SHALL prune alerts_outbox, alert_deliveries, and alert_history rows that are delivered, expired, or otherwise terminal and older than a configurable retention defaulting to 14 days, and SHALL NEVER prune seen_products, alert_state, or admin_scope_state rows.
23. THE Monitor SHALL store an admin_scope_state table keyed by scope_key as its PRIMARY KEY (one of `retailer:<id>`, `destination:<id>`, or `system`) with a non-null last_warning_at timestamp, used as the backing store for the per-Scope_Key hourly admin-warning rate limit and Admin_Digest timing for all scopes including `destination:<id>` and `system` scopes that have no associated Incident, and the admin_scope_state table SHALL NEVER be pruned (Requirement 17).

### Requirement 15: Polite Polling and robots.txt

**User Story:** As a retailer, I want the Monitor to access my site considerately and honour robots.txt, so that it does not impose an unreasonable load or behave abusively.

#### Acceptance Criteria

1. THE Watcher_Core SHALL poll each Retailer at that Retailer's configured Poll_Interval, defaulting to 60 seconds, and SHALL poll each Discovery_Feed and Listing at its own configured Poll_Interval.
2. IF a Retailer's, Discovery_Feed's, or Listing's configured Poll_Interval is below 30 seconds, THEN THE Watcher_Core SHALL use a Poll_Interval of 30 seconds for that entity.
3. WHEN scheduling a poll of a Retailer, Discovery_Feed, or Listing, THE Watcher_Core SHALL add random Jitter to that entity's Poll_Interval when computing its Next_Due_At.
4. WHEN an Adapter issues an HTTP request, THE Adapter SHALL send a descriptive User-Agent header.
5. WHEN a request to a host is required, THE Watcher_Core SHALL fetch that host's robots.txt once and cache it for 24 hours, treating a 4xx response as "allow", treating a 5xx response or network error as "retry later" and skipping the request for now, and WHERE a requested path is disallowed SHALL skip that path and open a `robots_disallowed` Incident for that path through which a single admin warning is sent, closing that Incident with a recovery notice when the path is no longer disallowed (Requirement 17).
6. THE Monitor's configured feed URLs SHALL NOT use a `sort_by` query parameter or a `+`-joined tag filter, because the retailer's robots.txt disallows those paths.

### Requirement 16: Ethical and Defensive Behavior

**User Story:** As a system owner, I want the Monitor to behave ethically and back off when blocked, so that it stays within acceptable use and avoids escalation.

#### Acceptance Criteria

1. THE Monitor SHALL perform monitoring and notification only, without performing automated purchasing.
2. THE Monitor SHALL operate without attempting to bypass captchas or bot-protection mechanisms.
3. WHEN a response from a Retailer is an HTTP 403, an HTTP 429, an HTTP 503 carrying a challenge indicator (a `cf-mitigated` header or a known challenge marker), a 2xx response whose body is HTML where a JSON feed was expected, or a detected captcha/challenge page, THE Watcher_Core SHALL treat that Retailer as Blocked.
4. WHEN a response from a Retailer is a plain 5xx response without a challenge indicator, THE Watcher_Core SHALL treat it as an error and apply the same backoff as other errors, and SHALL NOT treat it as Blocked.
5. IF a Retailer is Blocked, THEN THE Watcher_Core SHALL delay the next request to that Retailer using exponential backoff starting at the Poll_Interval, capped at 1 hour, honouring the `Retry-After` header when present, and SHALL reset the backoff on the next successful poll.
6. IF an ordinary error occurs — a timeout, a network error, a plain 5xx response without a challenge indicator, or a malformed response — THEN THE Watcher_Core SHALL set the affected entity's backoff_until using exponential backoff that starts at that entity's Poll_Interval, doubles on each consecutive ordinary error, and is capped at 1 hour, tracked per retailer, per feed, and per listing in the Backoff_State, distinct from the Blocked backoff.
7. WHEN an entity's poll succeeds, THE Watcher_Core SHALL reset that entity's ordinary-error backoff so that its backoff_until no longer defers the entity.

### Requirement 17: Health Monitoring

**User Story:** As an operator, I want to be warned when an adapter is failing or returning nothing, so that problems surface instead of failing silently.

#### Acceptance Criteria

1. IF an Adapter produces 3 consecutive errors for a Retailer, feed, or listing, THEN THE Notifier SHALL open an Incident of the `error` kind and emit a warning of Alert_Kind `admin` to the Admin_Destination through the Alerts_Outbox.
2. IF an Adapter returns 3 consecutive empty results where the previous successful poll was non-empty, THEN THE Notifier SHALL open an Incident of the `empty_result` kind and emit a warning of Alert_Kind `admin` to the Admin_Destination through the Alerts_Outbox.
3. WHEN a discovery poll reaches the configured maximum page count, THE Notifier SHALL open an Incident of the `truncated_feed` kind scoped to that feed and emit a warning of Alert_Kind `admin` to the Admin_Destination through the Alerts_Outbox (Requirement 2).
4. WHEN a requested path is disallowed by robots.txt, THE Notifier SHALL open an Incident of the `robots_disallowed` kind scoped to that path and emit a warning of Alert_Kind `admin` to the Admin_Destination through the Alerts_Outbox (Requirement 15).
5. WHEN the Notifier emits any admin warning or recovery notice, THE Notifier SHALL emit it as an Alert of Alert_Kind `admin` through the Alerts_Outbox so that the warning receives the same durable, retried, and deduplicated delivery as a product Alert (Requirement 6, Requirement 10).
6. WHEN an admin warning for a Scope_Key is produced, THE Notifier SHALL send that warning immediately as an Alert of Alert_Kind `admin` IF that Scope_Key's `admin_scope_state.last_warning_at` (criterion 14.23) is absent OR at least 1 hour old, and sending it SHALL set that Scope_Key's `admin_scope_state.last_warning_at` to the send time; OTHERWISE THE Notifier SHALL park that warning in the admin_notices store for the current admin-warning rate-limit window rather than sending or silently dropping it, so that at most one admin warning per hour per Scope_Key (one of `retailer:<id>`, `destination:<id>`, or `system`) is sent and every suppressed warning is retained for the next Admin_Digest.
7. WHEN un-digested admin_notices exist for a Scope_Key AND (now − that Scope_Key's `admin_scope_state.last_warning_at`) is at least 1 hour, THE Notifier SHALL emit the Admin_Digest for that Scope_Key as a single Alert of Alert_Kind `admin` that coalesces every parked notice for that Scope_Key, fan that Admin_Digest out to all enabled, healthy Admin_Destinations, set that Scope_Key's `admin_scope_state.last_warning_at` to the send time, and SHALL NOT silently drop any rate-limited warning.
8. WHEN expired-delivery warnings (Requirement 10), permanent-webhook-failure warnings (Requirement 10), or handle-change warnings (Requirement 11) are subject to the hourly admin-warning rate limit, THE Notifier SHALL aggregate those warnings per Scope_Key per window into the Admin_Digest rather than emit one message per event, using destination-scope (`destination:<id>`) for expired-delivery and permanent-webhook-failure warnings tied to a Destination and retailer-scope (`retailer:<id>`) for handle-change warnings.
9. WHEN a condition that opened an Incident of any kind clears, THE Notifier SHALL close that Incident and emit a recovery notice of Alert_Kind `admin` to the Admin_Destination through the Alerts_Outbox, and that recovery notice SHALL be exempt from the hourly admin-warning rate limit, SHALL NOT be coalesced into an Admin_Digest, and SHALL NOT update any Scope_Key's `admin_scope_state.last_warning_at`.
10. THE Notifier SHALL NEVER route a warning ABOUT a Destination THROUGH that same Destination; WHERE a warning concerns an Admin_Destination, THE Notifier SHALL route it only to the OTHER enabled, healthy Admin_Destinations (Requirement 10, criterion 10.13).
11. IF an admin Alert has zero enabled, healthy Admin_Destinations to deliver to, THEN THE Notifier SHALL log that Alert at ERROR level with every webhook URL redacted (Requirement 18) rather than silently drop it, and SHALL keep its notices un-digested, leaving the relevant Scope_Key's `admin_scope_state.last_warning_at` unchanged, until an enabled, healthy Admin_Destination exists to receive them.
12. WHEN an Adapter error occurs, THE Monitor SHALL record the error in the log output.
13. THE Monitor SHALL surface adapter failures through the Admin_Destination rather than discarding them silently.

### Requirement 18: Secrets Handling

**User Story:** As a security-conscious operator, I want secrets kept out of config and logs, so that webhook URLs and tokens are not leaked.

#### Acceptance Criteria

1. THE Config_Loader SHALL read secrets from environment variables rather than from the YAML configuration.
2. THE Monitor SHALL store on each Destination the name of the environment variable that holds its webhook URL rather than the webhook URL itself.
3. THE Monitor SHALL set the `httpx` and `httpcore` loggers to the WARNING level or above.
4. THE Monitor SHALL install a logging filter that redacts webhook URLs and tokens from all log records and from exception text, so that no webhook URL or token is written to the log output on any path.

### Requirement 19: Runtime Environment and Observability

**User Story:** As an operator, I want a typed, containerized service with standard logging and self-healing, so that it is maintainable and easy to deploy.

#### Acceptance Criteria

1. THE Monitor SHALL be implemented in Python 3.12 with type annotations.
2. THE Monitor's code SHALL pass `ruff` lint checking and `mypy --strict` type checking in CI, and the CI build SHALL fail unless both checks pass.
3. THE Monitor SHALL use httpx for HTTP requests, APScheduler for scheduling, and SQLite for persistence.
4. THE Monitor SHALL be deployable via Docker Compose.
5. THE Monitor's Docker Compose service SHALL set a restart policy that restarts the container unless it is explicitly stopped.
6. THE Monitor's container SHALL define a healthcheck that reads the Heartbeat DB record (criterion 14.16) and reports the service as healthy WHEN the scheduler-liveness heartbeat shows activity within a fixed, configurable threshold defaulting to 120 seconds, AND the database is writable, AND the dispatch-liveness heartbeat shows the Notifier dispatch loop has run within a fixed, configurable threshold defaulting to 60 seconds; the healthcheck SHALL count a tick as a scheduled job invocation even when that invocation skipped all of its entities due to backoff or effective due time.
7. THE Monitor SHALL beat the scheduler-liveness heartbeat from an INDEPENDENT periodic liveness beat that runs on a fixed cadence no slower than the scheduler Tick_Interval and does NOT depend on any retailer job running or on any entity being due, so that a deployment with zero enabled retailers, or one in which every retailer is deferred by backoff, still beats the scheduler-liveness heartbeat and does not trip the Watchdog; THE Monitor SHALL additionally beat the scheduler-liveness heartbeat between successive entities within a poll as supplementary beats rather than as the sole source of that heartbeat.
8. THE Monitor SHALL beat the dispatch-liveness heartbeat on each dispatch round and, during a webhook POST attempt, at every await boundary of that attempt — before issuing the POST, after receiving the response, and before and after each in-request retry sleep — so that a single delivery's in-request budget (Delivery_Request_Budget, default 45 seconds) can never elapse without a dispatch-liveness beat and therefore cannot exceed the 60-second dispatch-liveness threshold without a beat.
9. THE Monitor SHALL maintain the scheduler-liveness and dispatch-liveness heartbeats as in-memory monotonic heartbeats for the Watchdog and SHALL persist them to the Heartbeat DB record (criterion 14.16) at most every 5 seconds for the external healthcheck to read.
10. WHILE the Monitor runs and is NOT in Once_Mode, THE Monitor SHALL run the Watchdog on a dedicated daemon thread that reads only the in-memory monotonic heartbeats (and SHALL NOT read the database), and, after a configurable startup grace period defaulting to 120 seconds, SHALL compare the scheduler-liveness heartbeat against a fixed threshold defaulting to 120 seconds and the dispatch-liveness heartbeat against a fixed threshold defaulting to 60 seconds; IF either in-memory heartbeat is stale beyond its fixed threshold, THEN THE Monitor SHALL log the stall and hard-exit the process with a non-zero status so that the container restart policy (Requirement 19, criterion 5) restarts the process, noting that the restart policy restarts on process exit rather than on mere healthcheck unhealthiness.
11. WHERE Once_Mode is enabled, THE Monitor SHALL disable the Watchdog and SHALL instead bound the run by the Once_Mode_Drain_Timeout (Requirement 20).
12. WHEN the Monitor receives SIGTERM, THE Monitor SHALL stop the Watchdog FIRST, before cancelling in-flight polls or deliveries, so that the graceful-shutdown quiet period cannot trip the Watchdog.
13. THE Monitor SHALL keep the Watchdog's in-memory monotonic heartbeats distinct from the Heartbeat DB record (criterion 14.16): the in-memory heartbeats are the signal the Watchdog reads, while the Heartbeat DB record, written at most every 5 seconds, is the signal the external container healthcheck reads.
14. THE Monitor SHALL write log output to stdout.

### Requirement 20: Operational Modes

**User Story:** As an operator, I want a dry-run mode and a single-cycle mode, so that I can validate configuration and alert rendering without posting to Discord and can run one bounded cycle on demand.

#### Acceptance Criteria

1. WHERE Dry_Run_Mode is enabled, THE Monitor SHALL run polling and state changes normally.
2. WHERE Dry_Run_Mode is enabled, THE Notifier SHALL replace every webhook POST with a log line containing the rendered Alert content and SHALL issue no webhook POST.
3. WHILE Dry_Run_Mode is enabled, THE Notifier SHALL ensure that no dry-run log line contains any webhook URL, consistent with the redaction of webhook URLs from all log output (Requirement 18).
4. WHERE Once_Mode is enabled, THE Monitor SHALL run exactly one poll cycle for all enabled retailers, treating every enabled Discovery_Feed and Listing as due once for that single cycle by ignoring its Next_Due_At and its backoff, and then drain the Alerts_Outbox by delivering all pending Alert_Deliveries rows.
5. WHERE Once_Mode is enabled, THE Monitor SHALL, before draining, reclaim stale `sending` leases to `pending` and SHALL treat all pending Alert_Deliveries as due once by ignoring next_attempt_at while draining.
6. WHERE Once_Mode is enabled, THE Monitor SHALL honour a `Retry-After` response only within the Delivery_Request_Budget while draining.
7. WHERE Once_Mode is enabled, THE Monitor SHALL stop draining after a configurable Once_Mode_Drain_Timeout defaulting to 120 seconds.
8. WHERE Once_Mode is enabled, THE Monitor SHALL exit with status 0 ONLY IF no `pending` or `sending` Alert_Deliveries remain, and OTHERWISE SHALL exit with a non-zero status and log the count of remaining `pending` and `sending` deliveries.
9. WHERE Dry_Run_Mode is enabled, THE Monitor SHALL perform the drain immediately and SHALL issue no real webhook POST.

### Requirement 21: Test Coverage with Fixtures

**User Story:** As a developer, I want deterministic tests for diffing, adapters, and the delivery pipeline, so that I can change the code with confidence and without network dependence.

#### Acceptance Criteria

1. THE Monitor SHALL include pytest tests covering the state-diffing logic.
2. THE Monitor SHALL include pytest tests covering the Shopify_Adapter.
3. THE Monitor SHALL drive adapter tests from saved JSON and HTML fixtures.
4. THE Monitor's test suite SHALL fail any test that attempts real network access.
5. WHEN a new product is added to a fixture and the diff is evaluated, THE test SHALL confirm that exactly one new-product Alert is produced.
6. WHEN a fixture Variant flips from unavailable to available and the diff is evaluated, THE test SHALL confirm that exactly one restock Alert is produced with no repeat inside the Cooldown (Requirement 4).
7. WHEN a fixture price changes and the diff is evaluated, THE test SHALL confirm that exactly one price-change Alert is produced.
8. THE Monitor SHALL include tests confirming that the first successful poll of a feed or listing produces no Alert and sets Seeded_At (Baseline_Seeding).
9. THE Monitor SHALL include tests confirming that a trailing price-change Alert is emitted exactly once when a Cooldown expires and the observed price still differs from the last_alerted_price (Requirement 9).
10. THE Monitor SHALL include tests confirming that pagination terminates on a short or empty page and respects the configurable maximum page count.
11. THE Monitor SHALL include tests confirming that a State_Transition, its Alerts_Outbox row, its Alert_History row, and its Alert_Deliveries rows are written atomically and that undelivered Alert_Deliveries rows survive a restart.
12. THE Monitor SHALL include tests confirming retry and exponential-backoff behaviour, including honouring `Retry-After`, using a mocked httpx transport.
13. THE Monitor SHALL include tests confirming that configuration validation fails fast and reports all errors in a single pass.
14. THE Monitor SHALL include tests confirming that include and exclude keyword matching is Unicode-normalised, accent-insensitive, and case-insensitive.
15. THE Monitor SHALL include tests confirming that delivered Alerts set `allowed_mentions` so that only the configured role may be pinged.
16. THE Monitor SHALL include a test confirming that no webhook URL appears in captured log output, including on error paths (Requirement 18).
17. THE Monitor SHALL include a test confirming that a partial multi-destination delivery followed by a retry delivers to the previously failed Destination without re-sending to a Destination that already succeeded (Requirement 10).
18. THE Monitor SHALL include tests confirming the price-change Cooldown rule, including an A→B→A price-flapping sequence that reverts within the Cooldown and produces no Alert, and the single trailing price-change Alert when the price still differs after the Cooldown (Requirement 9).
19. THE Monitor SHALL include a test confirming that a first poll returning HTTP 404 stores existence as false and that a later transition to existing produces exactly one Page_Live_Event (Requirement 7).
20. THE Monitor SHALL include a test confirming soft-block detection, where a 2xx response returning HTML for a feed expected to be JSON is treated as Blocked (Requirement 16).
21. THE Monitor SHALL include a test confirming that a price read as Minor_Units from the `.js` endpoint and the same price read as a decimal string from a feed normalise to the same Decimal and produce no false price-change Alert (Requirement 11).
22. THE Monitor SHALL include a test confirming that re-enabling a previously disabled entity clears its Seeded_At and re-baselines it with no Alert on the next complete poll (Requirement 13).
23. THE Monitor SHALL include a test confirming that rows whose Source is `bot` survive a configuration reload unchanged (Requirement 13).
24. THE Monitor SHALL include a test confirming that a restock, followed by a sell-out and a second restock both separated by more than the Cooldown, produces exactly two restock Alerts (Requirement 4).
25. THE Monitor SHALL include a test confirming that a stock flap (out of stock to in stock and back) occurring within the Cooldown produces no restock Alert (Requirement 4).
26. THE Monitor SHALL include a test confirming that a restock observed within the Cooldown sets restock_pending and that the deferred restock Alert fires exactly once once the Cooldown has elapsed while the Listing is still in stock (Requirement 4).
27. THE Monitor SHALL include a test confirming that a single transient HTTP 404 (one 404 followed by an existing result) stores no change on the first 404 for a previously existing page and produces no Page_Live_Event (Requirement 7).
28. THE Monitor SHALL include a test confirming that two Discovery_Feeds for different games on the same retailer each independently emit a new-product Alert for the same product (the same External_Id), because the game id is part of the Seen_Products key (Requirement 3, Requirement 14).
29. THE Monitor SHALL include a test confirming that WHERE Dry_Run_Mode is enabled, the Monitor issues no webhook POST, logs the rendered Alert content, and writes no webhook URL to the captured log output (Requirement 20, Requirement 18).
30. THE Monitor SHALL include a test confirming that WHERE Once_Mode is enabled, the Monitor runs exactly one poll cycle, drains the Alerts_Outbox, and exits the process (Requirement 20).

### Requirement 22: MVP Scope — Northern Card Gaming

**User Story:** As a system owner, I want a single Shopify retailer wired end to end first, so that the pipeline is proven before expanding.

#### Acceptance Criteria

1. THE Monitor's MVP SHALL target Northern Card Gaming at `https://www.northerncardgaming.com` using the Shopify_Adapter.
2. THE Monitor's MVP SHALL require only the Adapter interface, the Adapter_Registry, and the Shopify_Adapter; the Sitemap_Adapter, Generic_HTML_Adapter, Custom_Adapter, and Playwright fallback are Phase 2 and SHALL NOT be built in this spec's tasks.
3. THE Monitor's MVP SHALL watch the `/collections/pokemon/products.json` and `/collections/pre-orders-1/products.json` feeds.
4. WHEN detecting new products from Northern Card Gaming, THE Watcher_Core SHALL compare products by (retailer id, game id, External_Id) against the Seen_Products store without assuming feed ordering.
5. WHEN detecting restocks from Northern Card Gaming, THE Shopify_Adapter SHALL evaluate the `available` flag of each Variant.
6. WHEN detecting price changes from Northern Card Gaming, THE Shopify_Adapter SHALL compute the price as the lowest `price` among available Variants, or the lowest `price` across all Variants when no Variant is available.
7. THE Monitor's MVP configuration SHALL seed the game `pokemon`, both the `/collections/pokemon/products.json` and `/collections/pre-orders-1/products.json` feeds as Discovery_Feeds, at least two real product listings, and at least one Admin_Destination.
8. THE Monitor's MVP SHALL include saved JSON fixtures captured from Northern Card Gaming for use in tests.
9. THE Monitor's MVP SHALL deliver alerts to one product Destination while the destinations data model supports multiple Discord servers.
10. THE Monitor's MVP SHALL poll Northern Card Gaming at a 60-second Poll_Interval with Jitter.
11. THE Monitor's MVP configuration SHALL seed the `pokemon` feed with the exclude keywords `plush`, `playmat`, `sleeves`, and `binder`, and these exclude keywords SHALL be configurable.
12. THE Monitor's MVP configuration SHALL seed the `pre-orders-1` feed with the include keyword `pokemon` and the same configurable exclude keywords as the `pokemon` feed (`plush`, `playmat`, `sleeves`, and `binder`).
13. THE Monitor's MVP configuration SHALL apply the default Max_Page_Count of 20 pages to each Discovery_Feed, configurable per feed (Requirement 12).

### Requirement 23: Scope Boundaries

**User Story:** As a stakeholder, I want the boundaries of this specification stated clearly, so that excluded work is not assumed to be delivered here.

#### Acceptance Criteria

1. THE Monitor SHALL exclude the Discord bot and slash commands from this specification.
2. THE Monitor SHALL exclude dashboards and price-history charts from this specification.
3. THE Monitor SHALL exclude per-user watchlists from this specification.
4. THE Monitor SHALL exclude automated purchasing from this specification.

## Project Notes

- After the MVP for Northern Card Gaming is complete and wired end to end, the project pauses for review before adding the Pokémon Center and Smyths retailers. This is a process checkpoint rather than a system behaviour, so it is captured here instead of as an acceptance criterion.
- The later Discord bot process shares this database. The `source` column (`config` vs `bot`) partitions ownership: the Config_Loader governs `config` rows only and never touches `bot` rows, which are reserved for entities the bot will create.
- Feed URLs are kept free of `sort_by` parameters and `+`-joined tag filters because the retailer's robots.txt disallows those paths; see Requirement 15.
- Retention and pruning is now a firm requirement rather than a note; see Requirement 14. Terminal alerts_outbox, alert_deliveries, and alert_history rows older than the configurable retention (default 14 days) are pruned, while seen_products and alert_state are never pruned so that dedupe and cooldown guarantees are preserved.
