# Marketing & Growth Data Pipeline — Questions & Answers Guide

> **Audience:** Data engineers, marketing analysts, engineering managers, and business stakeholders reviewing or evaluating this pipeline.
> **Format:** Question → Possible Response (short + detailed variants where appropriate)

---

## Table of Contents

1. [Architecture & Design Questions](#1-architecture--design-questions)
2. [Tech Stack Questions](#2-tech-stack-questions)
3. [Data Sources & Extraction Questions](#3-data-sources--extraction-questions)
4. [Transformation & dbt Questions](#4-transformation--dbt-questions)
5. [Mixpanel Integration Questions](#5-mixpanel-integration-questions)
6. [Data Quality & Reliability Questions](#6-data-quality--reliability-questions)
7. [Performance & Scalability Questions](#7-performance--scalability-questions)
8. [Security Questions](#8-security-questions)
9. [Business & Marketing Analytics Questions](#9-business--marketing-analytics-questions)
10. [Operational & Maintenance Questions](#10-operational--maintenance-questions)

---

## 1. Architecture & Design Questions

---

**Q: Why did you choose an ELT pattern instead of ETL?**

> **Short:** The heavy transformation logic lives in dbt (SQL in BigQuery), which is cheaper, faster, and more maintainable than doing it in Python before the load.

> **Detailed:** We use a "thin ETL, heavy ELT" hybrid. Python extractors do only the minimum necessary pre-load transformations: platform-specific field renaming, cost normalisation (e.g. `cost_micros ÷ 1,000,000`), and appending `_extracted_at`. The analytical transformations — KPI derivation, channel attribution, funnel aggregation — all live in dbt. This means: (1) you can reprocess the entire Gold layer from raw without re-hitting APIs; (2) SQL models are easier to code-review and test than pandas logic; (3) BigQuery does the heavy lifting, not your application server.

---

**Q: What is the Medallion Architecture and why did you use it?**

> **Short:** Three data tiers — Bronze (raw API payloads), Silver (cleaned views), Gold (business-ready aggregated tables). Each tier serves a different consumer and can be independently queried or rebuilt.

> **Detailed:**
> - **Bronze (`raw`)** is the append-only source of truth. Every API response lands here verbatim. If a downstream model has a bug, you can drop it and rerun dbt — you never need to re-call the API.
> - **Silver (`staging`)** is where data gets trusted. dbt views apply type casting, deduplication, and channel normalisation. Data science teams query Silver for exploratory work.
> - **Gold (`marts`)** is optimised for speed. Pre-aggregated, partitioned, clustered tables that BI tools like Looker and Power BI query directly. Analysts never touch Bronze or Silver directly in production dashboards.

---

**Q: Why are staging models `view` and mart models `table`?**

> **Short:** Views avoid data duplication in Silver (no storage cost, always fresh). Tables in Gold are pre-materialised for fast BI queries (avoiding repeated aggregation on large datasets).

> **Detailed:** The `staging` layer is cheap to recompute because it's a SELECT with type casts — no aggregation. Making it a view means analysts always see the freshest data and we pay zero storage cost. The `marts` layer involves complex multi-table joins, window functions, and aggregations. Materialising these as tables means a Looker dashboard query runs in 0.5 seconds instead of 45 seconds, and you don't pay BigQuery scan costs for every dashboard page load.

---

**Q: How would you add a new data source to this pipeline?**

> **Detailed answer:**
> 1. **Create extractor:** write a new class in `etl/extractors/` inheriting `BaseExtractor`, implement `validate_config()` and `extract()`.
> 2. **Add credentials:** add env var fields to `config/settings.py` (new config dataclass + field on `Settings`), and add template lines to `.env.example`.
> 3. **Declare fields:** add a new entry in `config/sources.yaml` with raw table name and API field list.
> 4. **Export class:** add the new extractor class to `etl/extractors/__init__.py`.
> 5. **Create dbt staging model:** `dbt/models/staging/stg_{source}.sql` + entry in `schema.yml`.
> 6. **Create dbt mart models:** add to relevant mart or create new mart file.
> 7. **Add Airflow task:** create a new DAG or add a `PythonOperator` task to an existing DAG.
> 8. **Add to CLI:** add entry to `SOURCE_MAP` in `scripts/run_pipeline.py`.
> 9. **Add data quality checks:** add freshness threshold and row count minimum in `pipeline_monitor.py`.
> 10. **Write tests:** add mock-based tests in `tests/` following existing patterns.

---

**Q: Why does each data source have its own Airflow DAG instead of one big DAG?**

> **Short:** Isolation — a failure in HubSpot should not block the GA4 pipeline. Each domain runs independently, can be paused individually, and has its own retry configuration.

> **Detailed:** A monolithic DAG creates implicit coupling between unrelated data sources. If Shopify's API is down, you don't want your paid media ingestion to wait. Separate DAGs allow: (1) different schedules per source (CRM runs at 05:00, paid media at 06:00 after overnight budget resets); (2) independent failure and alerting without cross-source pollution; (3) easier debugging — you see the HubSpot DAG's runs separately from Google Ads runs; (4) can pause one source for maintenance without disrupting others.

---

## 2. Tech Stack Questions

---

**Q: Why Apache Airflow instead of Prefect, Dagster, or a simple cron job?**

> **Short:** Airflow's DAG model, task dependency graph, built-in retry logic, web UI, and large ecosystem of providers (Google, Slack) fit well for a multi-source marketing pipeline. It's the industry standard.

> **Detailed:** For a pipeline with 7 sources, 5 DAGs, and interdependent task sequences (extract → dbt run → dbt test), a simple cron job provides no visibility into failures, no retry handling, and no dependency management. Airflow gives us: task-level retry with backoff, a web UI showing run history and logs, Slack alerting via providers, and native integration with BigQuery. Prefect and Dagster are strong alternatives — Dagster in particular has excellent dbt integration — but Airflow's maturity, self-hosted Docker Compose deployment, and provider ecosystem made it the practical choice here. Migrating to Dagster is a realistic upgrade path as the team grows.

---

**Q: Why dbt instead of writing SQL directly or using pandas for all transformations?**

> **Short:** dbt gives you version-controlled, tested, documented SQL with dependency management, lineage graphs, and automatic schema documentation.

> **Detailed:** Without dbt, transformation SQL gets scattered across Airflow `BashOperator` scripts with no tests, no lineage, and no documentation. dbt provides: (1) `ref()` — declare dependencies between models so dbt figures out execution order; (2) built-in `schema.yml` tests (`not_null`, `unique`, `accepted_values`, `accepted_range`) that run automatically; (3) `dbt docs generate` creates a data catalogue with column descriptions and lineage DAG; (4) `dbt compile` in CI catches SQL syntax errors before production; (5) Jinja templating allows reusable macros (e.g. `dbt_utils.generate_surrogate_key`). For pure pandas, you lose SQL pushdown (BigQuery can scan 1TB in seconds; pandas on your laptop can't). dbt keeps the compute in the warehouse where it belongs.

---

**Q: Why Python 3.11 specifically?**

> **Short:** 3.11 offers significant performance improvements (10–60% faster than 3.10 in benchmarks) and has mature support from all key libraries in the stack (pandas 2.x, pyarrow 16.x, google-ads 24.x, Airflow 2.9.x).

---

**Q: Why BigQuery over Snowflake or Redshift?**

> **Short:** BigQuery is serverless (no cluster to manage), has native integration with the Google ecosystem (GA4, Google Ads, Looker), charges per-query rather than per-hour, and scales automatically.

> **Detailed:** The pipeline is already pulling from Google Ads and GA4, so BigQuery's native connectors and Looker BI integration reduce integration overhead. BigQuery's partitioning and clustering directly maps to our access patterns (query by date, filter by channel/campaign). Snowflake would be a reasonable alternative with better cross-cloud support; Redshift is more operational overhead for a team without dedicated DBA resources. For a marketing-first stack on GCP, BigQuery is the natural choice.

---

**Q: What is `tenacity` and why is it used?**

> **Short:** `tenacity` is a Python retry library. Every outbound API call is wrapped with exponential backoff to handle transient network errors and API rate limits without crashing the pipeline.

> **Detailed:** Marketing APIs (Meta, Google Ads, Shopify) have rate limits and occasionally return `429 Too Many Requests` or `500 Internal Server Error`. Without retry logic, a single failed HTTP call would fail the entire DAG task. `tenacity`'s `@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))` decorator retries up to 3 times with 2s → 4s → 8s waits (exponential backoff). This handles the vast majority of transient failures transparently.

---

## 3. Data Sources & Extraction Questions

---

**Q: How does the pipeline handle API pagination?**

> **Short:** Each extractor loops until the API returns no more pages. Cursor-based APIs track the next cursor; page-based APIs increment until the page is empty.

> **Detailed:** Different APIs use different pagination patterns:
> - **Meta Ads:** Returns a `paging.cursors.after` token; the extractor loops `while next_cursor:`.
> - **HubSpot:** Returns `paging.next.after`; loops until no `after` token.
> - **Shopify:** Uses `Link` header with `?page_info=...` cursor.
> - **Google Ads GAQL:** Uses `google-ads` SDK which handles pagination automatically.
> - **Mixpanel Export API:** NDJSON stream — no pagination needed; the response is a continuous stream of newline-delimited JSON objects, read line by line.

---

**Q: How does the pipeline avoid duplicate data on re-runs?**

> **Short:** The `MERGE` write mode upserts rows using natural primary keys — inserting new rows and updating changed ones. Running the same pipeline window twice produces the same result.

> **Detailed:** For mutable records (Google Ads campaigns change retroactively, Shopify orders get updated), we use `write_mode="merge"` with defined `merge_keys`. `BigQueryLoader` loads new data into a temp table, then executes a `MERGE` statement: `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`. The merge keys are source-specific: Google Ads uses `(campaign_id, ad_group_id, date, device)`, Shopify uses `(order_id, object_type)`, Mixpanel events use `(insert_id, event_date)`. For truly immutable event streams (GA4 sessions, Mixpanel raw events), we use `append` mode with the `_extracted_at` timestamp to identify when rows were loaded.

---

**Q: What happens if an API is down during a scheduled run?**

> **Short:** The task retries 3 times with exponential backoff. If all retries fail, the task is marked failed in Airflow and a Slack alert fires. The next day's run will pick up the missing data via the lookback window.

> **Detailed:** Each extractor has `retries: 3` and `retry_delay: timedelta(minutes=5)` at the DAG level. The `tenacity` decorator adds per-request retry on top of that. If the API is down for an extended period and all Airflow retries are exhausted: (1) the task turns red in the Airflow UI; (2) Slack alert fires via `on_failure_callback`; (3) the engineer can manually trigger a backfill via `python scripts/run_pipeline.py --source google_ads --start-date YYYY-MM-DD` once the API recovers. The `lookback_days=30` configuration means the next scheduled run will also re-pull recent data and update any rows that were missed.

---

**Q: How does the Mixpanel extraction handle large volumes of raw events?**

> **Short:** NDJSON streaming with `requests` `stream=True` — events are parsed line-by-line and never fully buffered in memory.

> **Detailed:** The Mixpanel Data Export API streams responses as newline-delimited JSON (one event per line). We use `requests.get(..., stream=True)` and `response.iter_lines()`. This means a 10-million-event day is processed event-by-event without ever loading the full response into RAM. Events are batched into pandas DataFrames of configurable chunk size and loaded to BigQuery in batches. The extractor also chunks date ranges into 1-day windows (`chunk_days=1`) to stay within Mixpanel's API limits per request.

---

**Q: Why does the Mixpanel extractor support both Service Account and legacy API secret auth?**

> **Short:** Older Mixpanel projects use API secrets; newer projects use Service Accounts. Supporting both prevents breaking changes when migrating between auth methods.

> **Detailed:** Mixpanel introduced Service Accounts (project-scoped, non-expiring, rotatable) as the recommended auth method. However, many existing projects still use the legacy API secret. Both use HTTP Basic auth with base64 encoding — Service Accounts use `username:secret`, legacy uses `api_secret:` (empty password). The `_auth_header` property checks for service account credentials first and falls back to legacy. This means existing users can adopt the pipeline without changing their Mixpanel auth setup, and migrate to Service Accounts at their own pace.

---

## 4. Transformation & dbt Questions

---

**Q: What dbt tests are enforced and where are they defined?**

> **Short:** Tests are defined in `schema.yml` files in `staging/` and `marts/`. They cover not_null, unique, accepted_values, and accepted_range assertions and run in CI on every PR.

> **Examples by layer:**

| Layer | Model | Tests |
|-------|-------|-------|
| Staging | `stg_mixpanel_events` | `event_id`: not_null + unique; `event_name`: not_null; `channel_normalized`: accepted_values |
| Staging | `stg_shopify_orders` | `order_id`: not_null + unique; `gross_revenue` ≥ 0 |
| Marts | `fct_ad_spend_roi` | `performance_tier`: accepted_values (4 tiers); `cost_usd` ≥ 0; ROAS ≥ 0 |
| Marts | `fct_funnel_conversion` | `lead_to_mql_rate`: accepted_range [0,1]; `overall_conversion_rate`: [0,1] |
| Marts | `fct_mixpanel_funnel` | `step_conversion_rate`: [0,1]; `cumulative_drop_off_rate`: [0,1] |

---

**Q: How does the pipeline handle division by zero in KPI calculations?**

> **Short:** BigQuery's `SAFE_DIVIDE(numerator, denominator)` returns `NULL` instead of throwing an error when the denominator is zero. In Python, a custom `safe_divide()` helper uses `numpy.where`.

> **SQL pattern:**
> ```sql
> safe_divide(SUM(clicks), NULLIF(SUM(impressions), 0)) AS ctr
> ```

> **Python pattern:**
> ```python
> @staticmethod
> def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
>     return numerator.where(denominator != 0, other=0) / denominator.where(denominator != 0, other=1)
> ```

---

**Q: What does channel normalisation do and why does it matter?**

> **Short:** Different platforms use different terminology for the same channel type. Normalisation maps all of them to a consistent 6-category taxonomy so you can compare channels across Google Ads, Meta, GA4, and Mixpanel in a single dashboard.

> **Detailed:** Google Ads calls it `SEARCH`; GA4 calls it `google / cpc`; Mixpanel has `utm_medium=cpc` and `utm_source=google`. Without normalisation, a dashboard grouping by "channel" would show three separate rows for the same channel. All extractors and staging models apply the same normalisation logic:

| Input | Normalised to |
|-------|--------------|
| `SEARCH`, `cpc` + `google/bing` | `paid_search` |
| `cpc` + `facebook/instagram/linkedin` | `paid_social` |
| `email`, `Email Marketing` | `email` |
| `organic`, `google` + no medium | `organic` |
| `direct`, no source/medium | `direct` |
| Anything else | `other` |

---

**Q: How do you handle late-arriving data (e.g. Google Ads retroactive conversion updates)?**

> **Short:** The `lookback_days` parameter (default 30) re-pulls the past 30 days of data on every run. Combined with `MERGE` write mode, retroactively changed rows get updated in BigQuery automatically.

> **Detailed:** Google Ads and Meta Ads regularly update conversion counts for past dates as attribution windows close (typically 7–30 day click attribution windows). Setting `lookback_days=30` means every daily run re-extracts the past 30 days. The `MERGE` write mode then upserts those rows — if a conversion was added retroactively, the existing BigQuery row is updated via `WHEN MATCHED THEN UPDATE SET`. The `lookback_days` is configurable per source in `config/sources.yaml` so you can set it to 7 for sources with short attribution windows and 60 for those with longer ones.

---

**Q: How does `fct_mixpanel_campaign_attribution.sql` connect Mixpanel to ad spend?**

> **Short:** It joins Mixpanel UTM-tagged events to `fct_ad_spend_roi` on `(event_date, utm_campaign, channel_normalized)`, combining behavioral conversion counts with paid media spend to produce cross-channel attributed CPA and funnel rates.

> **Detailed flow:**
> 1. Filter `fct_mixpanel_events` to rows with a non-null `utm_campaign` (these are trackable paid media sessions)
> 2. Pivot event names into conversion columns: `Order Completed → orders`, `Lead Form Submitted → leads`, etc.
> 3. Join to `fct_ad_spend_roi` on `event_date + campaign_name + channel`
> 4. Compute Mixpanel-attributed CPA: `total_spend / orders`
> 5. Compute on-site funnel rates: `checkout_starts / add_to_carts`, `orders / checkout_starts`
>
> This lets marketers answer: "For this Google paid search campaign, our CPA from the ad platform says $45 — but Mixpanel shows the cart-to-checkout rate is only 40%, so the real cost per completed checkout is $112."

---

## 5. Mixpanel Integration Questions

---

**Q: What additional insights does Mixpanel provide that GA4 doesn't?**

> **Short:** Mixpanel captures product-level behavioral events (Add to Cart, Checkout Started, Feature Used, In-App Actions) with user identity across sessions. GA4 is session-centric; Mixpanel is user-centric with richer event properties and funnel analysis at the individual-user level.

> **Detailed comparison:**

| Capability | GA4 | Mixpanel |
|-----------|-----|---------|
| Session-level web analytics | ✅ Strong | ✅ Good |
| User identity (cross-session) | Limited (client_id) | ✅ Strong (distinct_id) |
| Product event tracking | Limited | ✅ Primary use case |
| Funnel analysis with time-to-convert | Basic | ✅ Step-by-step with timing |
| User segmentation by event history | Limited | ✅ Rich cohort analysis |
| Raw event export | Limited (BigQuery export) | ✅ Data Export API |
| Custom event properties | Limited | ✅ Unlimited |

---

**Q: What is the `insert_id` merge key in the Mixpanel events table?**

> **Short:** Mixpanel's `insert_id` is a client-generated unique ID for each event that Mixpanel itself uses for deduplication. We reuse it as our MERGE key to ensure re-extracting the same events doesn't create duplicates.

> **Detailed:** Mixpanel documents that setting `$insert_id` in your tracking calls enables their ingestion pipeline to deduplicate events. In our pipeline, we use `(insert_id, event_date)` as the MERGE key in `BigQueryLoader` — if an event with the same `insert_id` and `event_date` already exists in the table, it's updated rather than duplicated. For events without an `insert_id` (some legacy tracking implementations), the `stg_mixpanel_events.sql` model generates a surrogate key using `dbt_utils.generate_surrogate_key` from the combination of `distinct_id + event_name + event_time`.

---

**Q: What does `MIXPANEL_REGION=EU` do?**

> **Short:** Routes all Mixpanel API calls to `eu.data.mixpanel.com` and `eu.mixpanel.com` instead of US endpoints, ensuring raw event data is processed in the EU for GDPR data residency compliance.

> **Detailed:** Mixpanel provides EU infrastructure for customers under GDPR who require personal data to not leave the EU. The `_MixpanelBase` class reads `MIXPANEL_REGION` and selects the appropriate base URLs:
> - `region=EU`: `https://eu.data.mixpanel.com/api/2.0/export` (events) and `https://eu.mixpanel.com/api/2.0` (query/funnels)
> - `region=US` (default): standard US endpoints
>
> This has zero impact on the data structure — the same NDJSON format is returned from both regions.

---

**Q: How would you use the Mixpanel funnel data for a business decision?**

> **Short:** The `fct_mixpanel_funnel` model shows exactly where users drop off in a conversion flow, with per-step conversion rates and average time spent at each step. This directly informs A/B test priorities and UX investment decisions.

> **Example business scenario:**
> The funnel for "Checkout" has: `Page View (100%) → Add to Cart (35%) → Checkout Started (18%) → Order Completed (9%)`. The biggest drop is Page View → Add to Cart. A/B testing the product page or pricing is the highest-ROI experiment. The `avg_time_to_convert_hrs` column shows users who do convert take 2.3 hours on average — suggesting email retargeting within 2 hours of abandonment could capture near-converters.

---

## 6. Data Quality & Reliability Questions

---

**Q: How do you know the data in BigQuery is correct?**

> **Short:** Three layers of validation: (1) dbt schema tests run automatically after every mart build; (2) `MarketingDataValidator` runs SQL-based domain checks (ROAS bounds, CTR limits, funnel ordering); (3) `PipelineMonitor` checks freshness and row counts with Slack alerts.

> **Detailed breakdown:**

| Layer | When | What it checks |
|-------|------|---------------|
| dbt schema tests | After every `dbt run` | Row-level constraints: not_null, unique, accepted_values, value ranges |
| `MarketingDataValidator` | Daily at 09:00 UTC | Domain logic: negative spend, impossible ROAS, funnel ordering violations |
| `PipelineMonitor` | Daily at 09:00 UTC | Operational: data freshness, row count minimums, null rate maximums |
| CI: dbt compile | Every PR | SQL syntax validity before production |
| CI: pytest | Every PR | Python extraction logic, transformer math, loader behaviour |

---

**Q: What is a ROAS of 1000x and why does the validator catch it?**

> **Short:** ROAS > 1,000x on campaigns with real spend usually indicates bad data — typically a mismatch between the currency of spend and the currency of conversion value, or a tracking pixel firing multiple times per conversion.

> **Detailed:** If conversion tracking is misconfigured (e.g. a purchase fires 50 conversion events due to a JavaScript bug), the conversion value sum becomes inflated while cost stays the same, producing ROAS of 500x on campaigns spending thousands per day. This is real revenue data you'd otherwise report to executives as "ROAS is 500x today" — which is obviously wrong. The validator flags any row where `roas > 1000 AND cost_usd > 1` and raises a data quality failure in the daily DAG. The filter `cost_usd > 1` avoids false positives on test campaigns with $0.01 spend.

---

**Q: How do you handle schema changes when a source API adds or removes fields?**

> **Short:** New fields land in Bronze automatically (pandas loads extra columns without error). If a required field disappears, the dbt model referencing it will fail with a clear SQL error caught in CI.

> **Detailed:** The BigQuery loader uses `WRITE_APPEND` with `autodetect=True` for schema evolution, allowing new columns to be added without breaking existing queries. The `staging` dbt models `SELECT` explicitly named columns — if an API removes `impressions`, the `stg_google_ads.sql` view will fail in the next dbt run with an error like `Column impressions not found`. This failure is surfaced in CI (via `dbt compile`) before it reaches production. For planned field additions, you update the `schema.yml` source definition and staging model together in one PR.

---

**Q: What is `_extracted_at` and why is it on every raw table row?**

> **Short:** It's the UTC timestamp of when the row was extracted from the source API. Used for freshness monitoring (`MAX(_extracted_at)` tells you when data last arrived) and for filtering incremental loads by date.

> **Detailed:** Without `_extracted_at`, you have no way to know if a table was loaded today or last week. The `PipelineMonitor` runs:
> ```sql
> SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_extracted_at), HOUR) as hours_since_load
> FROM `project.raw.google_ads_campaigns`
> ```
> If `hours_since_load > 6`, a freshness alert fires. It also allows replaying pipeline runs for specific windows: `WHERE DATE(_extracted_at) = '2026-04-15'` isolates exactly what was loaded on a given Airflow run.

---

## 7. Performance & Scalability Questions

---

**Q: How is BigQuery query cost controlled?**

> **Short:** Day-partitioned tables on the `date` field combined with clustering on high-cardinality columns. BI tool queries filtered by date only scan the partitions they need, dramatically reducing bytes billed.

> **Detailed:**
> - **Partitioning by day** (`partition_by: {field: date, granularity: day}`) means a query filtering `WHERE date = '2026-04-20'` scans only 1/365th of the table data.
> - **Clustering** on `(channel_normalized, campaign_id, event_name)` co-locates related rows in storage. Queries filtering on these columns skip most blocks even within a partition.
> - **Staging as views** means there's no storage cost for Silver — only Gold tables are materialised.
> - **`dbt_utils.accepted_range` tests** with `min_value: 0` prevent null-heavy columns from inflating scan sizes.

---

**Q: How would you scale this pipeline to 10x the data volume?**

> **Short:** Switch Airflow `LocalExecutor` to `KubernetesExecutor`, add more workers, increase Mixpanel chunk parallelism, and convert high-volume mart models to dbt incremental materialisation.

> **Detailed scale-up path:**
> 1. **Airflow:** Replace `LocalExecutor` with `CeleryExecutor` (Redis broker) or `KubernetesExecutor`. Update `docker-compose.yml` to add worker nodes.
> 2. **Mixpanel events:** Add parallelism — split date range into N parallel chunks, each loaded by a separate worker.
> 3. **dbt incremental models:** Convert `fct_mixpanel_events` and `fct_campaign_performance` from `table` (full rebuild) to `incremental` (only process new partitions). BigQuery already has the partitioning structure needed for this.
> 4. **BigQuery Storage API:** Already included (`google-cloud-bigquery-storage 2.25.0`) — enables high-throughput reads for large intermediate results.
> 5. **Partitioned writes:** Use partition decorators (`table$YYYYMMDD`) for parallel loads without locking.

---

**Q: How long does the full pipeline take to run daily?**

> **Short:** Each DAG typically completes in 8–20 minutes. The data quality DAG runs last at 09:00 UTC; all data is available to BI tools by approximately 09:30 UTC.

> **Detailed timeline (UTC):**
> - **05:00** — CRM DAG starts (HubSpot): ~10–15 min
> - **06:00** — Marketing paid media DAG (Google Ads + Meta + LinkedIn): ~15–20 min
> - **07:00** — Ecommerce DAG (Shopify + GA4): ~10–12 min
> - **07:30** — Mixpanel DAG (Events + Segments + Funnels): ~12–18 min
> - **08:00** — GitHub Actions CD: dbt run (all marts): ~8–12 min
> - **09:00** — Data quality DAG: ~5–8 min
> - **~09:30** — All marts fully refreshed, dashboards show current data

---

## 8. Security Questions

---

**Q: How are API credentials stored and protected?**

> **Short:** All credentials live in `.env` (excluded from git via `.gitignore`). The GCP service account key is mounted as a Docker volume, never present in the image or source code.

> **Detailed security posture:**
> - `.env` contains all `API_KEY`, `ACCESS_TOKEN`, `CLIENT_SECRET` values. It is listed in `.gitignore` and never committed.
> - `.env.example` contains only variable names and descriptions — no real values — and is safe to commit.
> - `credentials/service_account.json` is mounted as a read-only Docker volume (`/opt/airflow/credentials:ro`), never built into the container image.
> - Airflow encrypts connection passwords stored in its Postgres metadata DB using the `FERNET_KEY`.
> - All config dataclasses are `frozen=True` — credentials cannot be mutated at runtime.
> - The GCP service account is granted only `bigquery.dataEditor` — not `Owner` or `Editor` at project level.

---

**Q: How do you handle credential rotation?**

> **Short:** Update `MIXPANEL_SERVICE_ACCOUNT_SECRET` (or the relevant env var) in `.env`, restart the Airflow containers. No code changes required.

> **Detailed:** Because credentials are strictly managed through environment variables (not hardcoded), rotation requires: (1) generate new key in the source platform (Mixpanel, Google Cloud Console, Meta Business Manager); (2) update the corresponding variable in `.env` on the server; (3) restart the Airflow webserver and scheduler containers (`docker-compose restart airflow-webserver airflow-scheduler`). The `@lru_cache` on `get_settings()` is at module level — the container restart clears the cache and loads the new credentials. For production environments, use a secrets manager (GCP Secret Manager, AWS Secrets Manager, or Doppler) to inject env vars automatically on container startup.

---

**Q: Is there a risk of SQL injection in the data quality checks?**

> **Short:** No. The only user-controlled input to SQL queries is `execution_date`, which is validated as a Python `date` object (via `datetime.strptime` in the CLI) before being formatted into the SQL string. No free-text user input reaches the query.

> **Detailed:** The `execution_date` parameter in `MarketingDataValidator` originates from Airflow's `{{ ds }}` macro (formatted YYYY-MM-DD, validated by Airflow's scheduling engine) or from the CLI argument parser which enforces `datetime.strptime(s, "%Y-%m-%d")`. Both ensure only valid ISO date strings reach the SQL template. Table names and column names in the SQL are hardcoded — no user input constructs table identifiers. For any future check that takes free-text input, use BigQuery's parameterised query API (`query_parameters`) instead of string formatting.

---

## 9. Business & Marketing Analytics Questions

---

**Q: What marketing questions can this pipeline answer?**

> **Short answer list:**

| Business question | Answered by |
|------------------|-------------|
| Which campaigns have the highest ROAS? | `fct_ad_spend_roi` — performance_tier + roas by campaign |
| What is my cost per acquired customer? | `fct_ad_spend_roi.cpa` joined to `fct_funnel_conversion.customers` |
| Where do leads drop off in the funnel? | `fct_funnel_conversion` — stage conversion rates by channel |
| Which channel drives the most revenue? | `fct_attribution` — last-touch and linear attribution |
| What do users do after clicking a Google Ads campaign? | `fct_mixpanel_campaign_attribution` — UTM events + spend join |
| Which product actions predict purchase? | `fct_mixpanel_events` — `Add to Cart`, `Checkout Started` counts |
| How long does it take users to convert? | `fct_mixpanel_funnel.avg_time_to_convert_hrs` |
| What is the overall brand awareness reach? | `fct_campaign_performance.impressions` across all channels |
| Where do buyers come from? (LinkedIn vs. Meta?) | `fct_campaign_performance` + `fct_attribution` by channel |

---

**Q: How do you define and track ROAS, and what's a good ROAS target?**

> **Calculation:** `ROAS = conversion_value / ad_spend`

> **Performance tiers used in this pipeline:**

| Tier | ROAS | Meaning |
|------|------|---------|
| `high_performer` | ≥ 4.0x | $4+ revenue per $1 spent — strong positive ROI |
| `mid_performer` | 2.0–3.9x | Profitable, room to scale |
| `break_even` | 1.0–1.9x | Covering cost, not growing |
| `underperformer` | < 1.0x | Losing money — review or pause |

> **Context:** ROAS targets vary by industry and margin. A high-margin SaaS product might target 3x; a low-margin e-commerce business might need 8–10x to be profitable after COGS. The `min_spend_threshold` dbt var (`$0.01`) filters out test campaigns from ROI calculations.

---

**Q: What is the difference between last-touch and linear attribution, and which should I use?**

> **Short:** Last-touch gives 100% credit to the final channel a user touched before converting. Linear splits credit equally across all touchpoints. Neither is perfectly accurate — use both and compare.

> **Detailed:**
> - **Last-touch** (`fct_attribution` with `model = 'last_touch'`): Overvalues bottom-of-funnel channels (retargeting, branded search) and undervalues top-of-funnel channels (display, content). Good for optimising conversion campaigns.
> - **Linear** (`model = 'linear'`): Gives equal credit to every touchpoint. Better for understanding the full customer journey and justifying investment in awareness channels.
> - **Mixpanel-attributed** (`fct_mixpanel_campaign_attribution`): Provides a third view — conversion events (Add to Cart, Order Completed) attributed to the last UTM-tagged session prior to the event. More direct than GA4 attribution for product conversions.
>
> Best practice: report all three side by side. When they agree, you have high confidence. When they diverge (e.g. LinkedIn looks bad in last-touch but good in linear), that signals LinkedIn is a top-of-funnel channel that deserves brand budget rather than a direct-response budget.

---

**Q: How would a marketing analyst access this data?**

> **Short:** Analysts connect a BI tool (Looker, Power BI, Zoho Analytics) directly to the `marts` BigQuery dataset. All mart tables are documented and ready to query — no SQL knowledge required for dashboards.

> **Access layers:**
> - **BI tools (no SQL):** Connect to `project.marts.*` tables. Recommended for: campaign performance dashboards, ROAS tracking, funnel reporting.
> - **BigQuery Console (SQL):** Analysts can query any layer directly. Silver (`staging`) is safe for ad-hoc exploration; Gold (`marts`) for fixed reports.
> - **Google Sheets + BigQuery connector:** Pull mart data into Sheets for stakeholder reports. Query: `SELECT * FROM project.marts.fct_ad_spend_roi WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`.
> - **CLI (`scripts/run_pipeline.py`):** For data engineers who want to manually re-run a source for a specific date range.

---

## 10. Operational & Maintenance Questions

---

**Q: How do you perform a historical backfill if a new source is added?**

> **Short:** Run the CLI with a historical date range. The MERGE write mode handles upserts safely, so the command is idempotent.

```bash
# Backfill Mixpanel events from January to April 2026
python scripts/run_pipeline.py \
  --source mixpanel_events \
  --start-date 2026-01-01 \
  --end-date 2026-04-20
```

> **Detailed:** For large historical backfills (e.g. 2 years of Google Ads), break the range into monthly chunks and run sequentially to respect API rate limits. The MERGE write mode ensures re-running the same date range produces the same result (idempotent). After the raw backfill, run dbt to rebuild mart models: `dbt run --select marts.+ --full-refresh`.

---

**Q: What should I check first when a Slack alert fires for a freshness breach?**

> **Triage checklist:**
> 1. **Check Airflow UI** (http://localhost:8080) — is the relevant DAG's most recent run green, red, or still running?
> 2. **Check task logs** — if a task failed, the logs show the exact error (API 429, auth failure, empty response).
> 3. **Check source API status page** — Meta, Google Ads, HubSpot, Shopify all have public status pages.
> 4. **Manual trigger** — if the DAG failed and the API is now healthy, manually trigger the DAG from the Airflow UI.
> 5. **CLI fallback** — if Airflow itself is down, run manually: `python scripts/run_pipeline.py --source meta_ads --start-date YYYY-MM-DD`.
> 6. **Escalate if > 24 hours** — if data is stale beyond the `error_after` threshold, flag to stakeholders that dashboards may show incomplete data.

---

**Q: How do you deploy a code change to production?**

> **Short:** Open a pull request → CI runs automatically (tests + lint + dbt compile) → merge to `main` → CD workflow runs `dbt run` for the next scheduled Airflow run.

> **Detailed release flow:**
> 1. Branch from `main`: `git checkout -b feat/new-extractor`
> 2. Make changes, run locally: `pytest tests/` + `ruff check etl/`
> 3. Open PR to `main` → GitHub Actions CI runs automatically (must pass)
> 4. Peer review → merge
> 5. For Python/DAG changes: `git pull && docker-compose restart airflow-scheduler` on the server
> 6. For dbt changes: CD workflow runs `dbt run` at 08:00 UTC automatically, OR manually `dbt run --select changed_model+` for emergency deploys

---

**Q: How would you add a new KPI to the pipeline?**

> **Step-by-step:**
> 1. **Define it first** — add the KPI to `schemas/kpi_definitions.yaml` with formula, data type, and source table.
> 2. **Add to dbt model** — update the relevant mart SQL file (e.g. `fct_ad_spend_roi.sql`) with the new column calculation.
> 3. **Add schema test** — add the new column to `dbt/models/marts/schema.yml` with appropriate tests (e.g. `accepted_range: {min_value: 0}`).
> 4. **Document it** — add to `docs/data_dictionary.md`.
> 5. **Deploy** — submit PR, pass CI, merge. The CD workflow will run `dbt run` in the next scheduled cycle.
> 6. **Update BI tool** — add the new field to the relevant dashboard/table in Looker or Power BI.

> **Example:** Adding "Brand Impression Share" from Google Ads:
> - Source already present in `raw.google_ads_campaigns` (if Google Ads extractor is updated to pull `metrics.search_impression_share`)
> - Add `search_impression_share` to `stg_google_ads.sql` and `fct_campaign_performance.sql`
> - Add `not_null` and `accepted_range: {min_value: 0, max_value: 1}` test in schema.yml
> - Add definition in `kpi_definitions.yaml`
