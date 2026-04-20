# Marketing & Growth Data Pipeline — Technical Guide

> **Version:** 1.0 | **Last Updated:** April 20, 2026 | **Owner:** Data Engineering

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Tech Stack Reference](#2-tech-stack-reference)
3. [Data Sources & APIs](#3-data-sources--apis)
4. [Step-by-Step: How the Pipeline Is Built](#4-step-by-step-how-the-pipeline-is-built)
   - [Step 1 — Project Scaffolding & Configuration](#step-1--project-scaffolding--configuration)
   - [Step 2 — Extraction Layer (ETL Extractors)](#step-2--extraction-layer-etl-extractors)
   - [Step 3 — Transformation Layer](#step-3--transformation-layer)
   - [Step 4 — Loading into BigQuery (Bronze Layer)](#step-4--loading-into-bigquery-bronze-layer)
   - [Step 5 — dbt Staging Models (Silver Layer)](#step-5--dbt-staging-models-silver-layer)
   - [Step 6 — dbt Mart Models (Gold Layer)](#step-6--dbt-mart-models-gold-layer)
   - [Step 7 — Orchestration with Apache Airflow](#step-7--orchestration-with-apache-airflow)
   - [Step 8 — Data Quality Layer](#step-8--data-quality-layer)
   - [Step 9 — CI/CD with GitHub Actions](#step-9--cicd-with-github-actions)
   - [Step 10 — Mixpanel Behavioral Analytics Integration](#step-10--mixpanel-behavioral-analytics-integration)
5. [Medallion Architecture Deep-Dive](#5-medallion-architecture-deep-dive)
6. [KPI Catalog](#6-kpi-catalog)
7. [Security & Secrets Management](#7-security--secrets-management)
8. [Scalability & Performance Design](#8-scalability--performance-design)
9. [Deployment Guide](#9-deployment-guide)
10. [Monitoring & Alerting](#10-monitoring--alerting)

---

## 1. Architecture Overview

The pipeline follows an **ELT** (Extract → Load → Transform) pattern on Google BigQuery, with a thin ETL preprocessing step for platform-specific normalisations before the raw load.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             DATA SOURCES (7)                                 │
│                                                                               │
│  ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌─────┐ ┌─────────┐ ┌───────────┐  │
│  │Google Ads│ │Meta Ads │ │LinkedIn  │ │ GA4 │ │HubSpot  │ │  Shopify  │  │
│  │  REST    │ │Marketing│ │   Ads    │ │Data │ │  CRM    │ │  Orders   │  │
│  │   API    │ │   API   │ │   API    │ │ API │ │   API   │ │    API    │  │
│  └────┬─────┘ └────┬────┘ └────┬─────┘ └──┬──┘ └────┬────┘ └─────┬─────┘  │
│       └────────────┴───────────┴──────────┴──────────┴────────────┘        │
│                                    +                                         │
│                         ┌──────────────────┐                                │
│                         │    Mixpanel      │                                │
│                         │ Events/Segments/ │                                │
│                         │    Funnels       │                                │
│                         └────────┬─────────┘                                │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │  Python 3.11 Extractors
                                   │  (tenacity retries, pagination)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION — Apache Airflow 2.9.1                      │
│                                                                               │
│   marketing_pipeline_dag   │  crm_pipeline_dag   │  ecommerce_analytics_dag │
│   (Google Ads+Meta+LinkedIn│  (HubSpot, 05:00)   │  (Shopify+GA4, 07:00)   │
│    06:00 UTC daily)        │                     │                          │
│                            │  mixpanel_analytics_dag  (07:30 UTC daily)     │
│                         data_quality_dag (09:00 UTC daily)                  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  BigQuery Python SDK (google-cloud-bigquery)
                                   │  MERGE / APPEND / OVERWRITE write modes
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   GOOGLE BIGQUERY — DATA WAREHOUSE                           │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  BRONZE — raw dataset (partitioned tables, append/merge)            │    │
│  │  google_ads_campaigns │ meta_ads_insights │ linkedin_ads_analytics  │    │
│  │  ga4_sessions │ hubspot_objects │ shopify_raw                       │    │
│  │  mixpanel_events │ mixpanel_segmentation │ mixpanel_funnel_conversions│  │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
│                                 │  dbt run (views, type-safe)               │
│  ┌──────────────────────────────▼──────────────────────────────────────┐    │
│  │  SILVER — staging dataset (dbt views)                               │    │
│  │  stg_google_ads │ stg_meta_ads │ stg_ga4_sessions                   │    │
│  │  stg_hubspot_contacts │ stg_shopify_orders                          │    │
│  │  stg_mixpanel_events │ stg_mixpanel_segmentation │ stg_mixpanel_funnels│ │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
│                                 │  dbt run (partitioned tables)             │
│  ┌──────────────────────────────▼──────────────────────────────────────┐    │
│  │  GOLD — marts dataset (dbt partitioned tables)                      │    │
│  │  marketing/  fct_campaign_performance │ fct_ad_spend_roi            │    │
│  │              fct_mixpanel_events │ fct_mixpanel_campaign_attribution │   │
│  │  growth/     fct_funnel_conversion │ fct_mixpanel_funnel            │    │
│  │  revenue/    fct_revenue │ fct_attribution                          │    │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                         ┌─────────▼──────────┐
                         │  BI / ANALYTICS     │
                         │  Looker │ Power BI  │
                         │  Zoho   │ G. Sheets │
                         └────────────────────┘
```

---

## 2. Tech Stack Reference

### Core Language & Runtime

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Language | Python | 3.11 | All ETL logic, data transformations |
| Package management | pip + requirements.txt | — | Dependency locking |
| Config pattern | `dataclasses` + `python-dotenv` | 1.0.1 | Typed, immutable config via env vars |
| Retry logic | `tenacity` | 8.3.0 | Exponential backoff for all API calls |
| Logging | `structlog` | 24.1.0 | Structured JSON logs for observability |

### Orchestration

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Workflow orchestrator | Apache Airflow | 2.9.1 | DAG scheduling, task dependencies, retry handling |
| Executor | LocalExecutor | — | Single-node; swap to KubernetesExecutor for scale |
| Metadata DB | PostgreSQL | 15 | Airflow state store |
| Containerisation | Docker + Docker Compose | — | Reproducible local dev and server deployment |
| Airflow web UI | Airflow Webserver | — | DAG visibility, logs, manual triggering |

### Data Warehouse

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Warehouse | Google BigQuery | — | Cloud-native columnar storage and SQL engine |
| Python SDK | `google-cloud-bigquery` | 3.20.0 | Table creation, MERGE writes, query execution |
| Storage SDK | `google-cloud-bigquery-storage` | 2.25.0 | High-throughput reads for large result sets |
| Auth | Google Service Account JSON + `google-auth` | 2.29.0 | Credential management |
| Arrow bridge | `pyarrow` | 16.1.0 | Efficient DataFrame → BigQuery serialisation |

### Transformation Layer

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| SQL transformation | dbt-core | 1.8.2 | Modular, version-controlled SQL models |
| BigQuery adapter | dbt-bigquery | 1.8.2 | dbt dialect for BigQuery SQL |
| dbt packages | `dbt_utils`, `audit_helper`, `dbt_date` | — | Surrogate keys, test macros, date spines |
| Python transforms | `pandas` | 2.2.2 | In-memory KPI computation pre-load |
| Numeric processing | `numpy` | 1.26.4 | NaN-safe arithmetic |

### API Clients (Data Sources)

| Source | Library | Version |
|--------|---------|---------|
| Google Ads | `google-ads` | 24.1.0 |
| Meta / Facebook Ads | `facebook-business` | 20.0.2 |
| Google Analytics 4 | `google-analytics-data` | 0.18.9 |
| HubSpot CRM | `hubspot-api-client` | 8.2.1 |
| Salesforce CRM | `simple-salesforce` | 1.12.5 |
| Shopify | `ShopifyAPI` | 12.6.0 |
| LinkedIn Ads | `requests` (REST) | 2.31.0 |
| Mixpanel | `requests` (REST, NDJSON streaming) | 2.31.0 |

### Data Quality

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Quality framework | `great-expectations` | 0.18.15 | Expectation suites for schemas |
| Custom SQL checks | `MarketingDataValidator` (in-house) | — | Domain-specific assertions (ROAS bounds, funnel order) |
| Pipeline monitoring | `PipelineMonitor` (in-house) | — | Freshness, row count, null rate checks |
| Alerting | Slack Webhook (via `requests`) | — | Real-time Slack alerts on threshold breach |

### CI/CD

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Version control | Git + GitHub | Source of truth; branch-based workflow |
| CI | GitHub Actions | pytest + ruff lint + dbt compile on every PR |
| CD | GitHub Actions (scheduled) | Daily dbt production run (08:00 UTC) |
| Code quality | `ruff` | Fast Python linter; enforces style and security patterns |
| Test runner | `pytest` | Unit tests with `pytest-mock`, `responses`, `freezegun` |
| Coverage | `pytest-cov` + Codecov | Prevents regression on ETL logic |

---

## 3. Data Sources & APIs

### Google Ads — GAQL (Google Ads Query Language)
- **API type:** REST (gRPC under the hood via `google-ads` SDK)
- **Auth:** OAuth2 refresh token + developer token
- **Extraction pattern:** Date-ranged GAQL queries per customer ID
- **Key metrics:** impressions, clicks, cost_micros (÷1M = USD), conversions, conversion_value
- **Lookback:** 30 days (configurable)

### Meta Ads — Marketing API
- **API type:** Graph API REST (paginated cursors)
- **Auth:** Long-lived access token
- **Extraction pattern:** Async Insights jobs at campaign/adset/ad level with breakdowns
- **Key metrics:** spend, impressions, clicks, reach, frequency, action_types (purchase, lead, add_to_cart)

### LinkedIn Ads — Marketing Developer Platform
- **API type:** REST v2
- **Auth:** OAuth2 access token
- **Extraction pattern:** Analytics Finder by campaign with date range
- **Key metrics:** costInLocalCurrency, impressions, clicks, conversions, pivot analytics

### Google Analytics 4 (GA4) — Data API
- **API type:** REST (google-analytics-data SDK)
- **Auth:** Service Account
- **Extraction pattern:** RunReport with dimensions + metrics
- **Key metrics:** sessions, activeUsers, newUsers, bounceRate, totalRevenue, conversions

### HubSpot — CRM API v3
- **API type:** REST (hubspot-api-client)
- **Auth:** Private App API key
- **Objects extracted:** Contacts (lifecycle stages), Deals (pipeline/amount), Companies
- **Key use:** Funnel conversion rates (Lead → MQL → SQL → Opportunity → Customer)

### Shopify — Admin API
- **API type:** REST + GraphQL
- **Auth:** Shop domain + API key/secret
- **Objects extracted:** Orders, Customers, Abandoned checkouts
- **Key metrics:** gross_revenue, net_revenue, order_count, AOV, LTV

### Mixpanel — Data Export & Query APIs
- **APIs used:**
  - **Data Export API** (`data.mixpanel.com/api/2.0/export`) — raw event-level NDJSON stream
  - **Query/Segmentation API** (`mixpanel.com/api/2.0/segmentation`) — aggregated event counts by property
  - **Funnels API** (`mixpanel.com/api/2.0/funnels`) — step-by-step funnel conversion
- **Auth:** Service Account (Basic base64) or legacy API secret
- **EU residency:** Supported via `MIXPANEL_REGION=EU` routed to `eu.data.mixpanel.com`
- **Key data:** user events (UTMs, device, geo), segment event counts, funnel conversion rates

---

## 4. Step-by-Step: How the Pipeline Is Built

### Step 1 — Project Scaffolding & Configuration

**What happens:** The project is initialised with a clean directory structure, dependency manifest, and a secure configuration layer.

```
marketinggrowth/
├── config/
│   ├── settings.py       ← Typed config dataclasses; reads from env vars
│   ├── sources.yaml      ← Declarative API field + table mappings
│   └── __init__.py
├── .env.example          ← Safe template (never commit .env)
├── .gitignore            ← Excludes .env, credentials/, *.json keys
└── requirements.txt      ← Pinned dependency versions
```

**Key design decisions:**
- All credentials live in `.env` (never in code). `python-dotenv` loads them at runtime.
- `config/settings.py` uses **frozen dataclasses** (`@dataclass(frozen=True)`) so settings are immutable after startup. A single `get_settings()` function with `@lru_cache` ensures only one config instance exists.
- `config/sources.yaml` separates *what fields to extract* from *how to extract them*, making it easy to add new metrics without touching Python code.

```python
# Pattern: single cached settings object
@lru_cache(maxsize=None)
def get_settings() -> Settings:
    return Settings()

# Usage across all modules
cfg = get_settings().google_ads
```

---

### Step 2 — Extraction Layer (ETL Extractors)

**What happens:** Each data source has a dedicated extractor class that inherits from `BaseExtractor`. The base class enforces the interface; each subclass handles the platform-specific API logic.

```
etl/extractors/
├── base_extractor.py         ← Abstract base: validate_config(), extract(), run()
├── google_ads_extractor.py   ← GAQL queries, cost_micros → USD conversion
├── meta_ads_extractor.py     ← Graph API pagination, action_types parsing
├── linkedin_ads_extractor.py ← Analytics Finder endpoint, cursor pagination
├── google_analytics_extractor.py ← GA4 RunReport, date dimension parsing
├── hubspot_extractor.py      ← Objects API, lifecycle stage mapping
├── shopify_extractor.py      ← Admin REST API, order status filtering
└── mixpanel_extractor.py     ← NDJSON streaming, segmentation, funnels
```

**Contract enforced by `BaseExtractor`:**
```python
class BaseExtractor(ABC):
    @abstractmethod
    def validate_config(self) -> None: ...   # raises ValueError if creds missing
    
    @abstractmethod
    def extract(self, start_date, end_date) -> pd.DataFrame: ...
    
    def run(self, start_date, end_date) -> pd.DataFrame:
        self.validate_config()
        return self.extract(start_date, end_date)  # called by Pipeline
```

**Reliability patterns built into every extractor:**
- **Tenacity retries:** `@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))` on every outbound API call
- **Pagination:** all cursor/page-based APIs loop until exhausted
- **Date chunking:** large date ranges split into day/week chunks to stay within API rate limits
- **`_extracted_at` column:** every row gets a UTC timestamp added on extraction for freshness tracking

**Mixpanel NDJSON streaming example — memory-safe large event exports:**
```python
response = requests.get(url, params=params, headers=auth, stream=True)
for line in response.iter_lines():
    if line:
        records.append(json.loads(line))
```

---

### Step 3 — Transformation Layer

**What happens:** A thin Python transformation step runs *before* the raw load for platform-normalisation. The heavy analytical transformations happen in dbt (Step 5 & 6).

```
etl/transformers/
├── base_transformer.py        ← Abstract: transform(df) → df; safe_divide helper
├── campaign_transformer.py    ← Unified KPI derivation + channel normalisation
└── attribution_transformer.py ← Last-touch, first-touch, linear attribution
```

**`CampaignTransformer` does three things:**
1. **Column renaming** — maps platform-specific names (`cost_micros`, `spend`, `costInLocalCurrency`) to the canonical `cost` field
2. **KPI derivation** — computes CTR, CPC, CPM, CVR, CPA, ROAS using NaN-safe division
3. **Channel normalisation** — maps raw `channel_type` / `placement` values to a 6-category `channel_normalized` taxonomy: `paid_search`, `paid_social`, `display`, `video`, `email`, `organic`

```python
# KPIs computed in Python before the raw load
df["ctr"]  = safe_divide(df["clicks"],            df["impressions"])
df["cpc"]  = safe_divide(df["cost"],              df["clicks"])
df["cpm"]  = safe_divide(df["cost"],              df["impressions"]) * 1000
df["cvr"]  = safe_divide(df["conversions"],       df["clicks"])
df["cpa"]  = safe_divide(df["cost"],              df["conversions"])
df["roas"] = safe_divide(df["conversion_value"],  df["cost"])
```

> **Note:** Mixpanel, GA4, HubSpot, and Shopify extractors do **not** run through `CampaignTransformer` — they do platform-specific normalisation in-extractor and their analytical transformations are handled entirely in dbt.

---

### Step 4 — Loading into BigQuery (Bronze Layer)

**What happens:** `BigQueryLoader` writes the transformed DataFrame to the `raw` BigQuery dataset using one of three write modes.

**Write modes:**

| Mode | When used | Mechanism |
|------|----------|-----------|
| `append` | Immutable event streams (Mixpanel events, GA4 sessions) | `WriteDisposition.WRITE_APPEND` |
| `overwrite` | Daily snapshot tables | `WriteDisposition.WRITE_TRUNCATE` |
| `merge` | Mutable records (ads, contacts, orders) | MERGE via temp table + MERGE SQL |

**MERGE strategy (upsert pattern):**
```sql
-- BigQueryLoader generates this dynamically for any table:
MERGE `project.raw.google_ads_campaigns` T
USING `project.raw._tmp_google_ads_campaigns_20260420` S
ON T.campaign_id = S.campaign_id
   AND T.ad_group_id = S.ad_group_id
   AND T.date = S.date
   AND T.device = S.device
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**Partitioning:** All raw tables are day-partitioned on the `date` or `event_date` field. This makes BigQuery scans cheaper and enables fast freshness queries.

---

### Step 5 — dbt Staging Models (Silver Layer)

**What happens:** dbt reads from raw tables and produces clean, typed, deduplicated **views** in the `staging` dataset. No data is duplicated — only SELECT + type casting + light enrichment.

```
dbt/models/staging/
├── schema.yml                  ← Source freshness declarations + model tests
├── stg_google_ads.sql          ← Cleaned ad performance with UTM taxonomy
├── stg_meta_ads.sql            ← Normalised Meta ad insights
├── stg_ga4_sessions.sql        ← Typed GA4 sessions + channel normalisation
├── stg_hubspot_contacts.sql    ← Lifecycle stage booleans (is_mql, is_sql, ...)
├── stg_shopify_orders.sql      ← Net/gross revenue, order status filter
├── stg_mixpanel_events.sql     ← Event deduplication + channel_normalized
├── stg_mixpanel_segmentation.sql ← Aggregated segment event counts
└── stg_mixpanel_funnels.sql    ← Funnel step data + time-to-convert columns
```

**Materialisation:** `view` (Silver views are never stored — they're recomputed on demand, keeping storage costs low)

**Example — `stg_mixpanel_events.sql` channel normalisation:**
```sql
channel_normalized = CASE
    WHEN utm_medium IN ('cpc','ppc','paid')
         AND utm_source IN ('google','bing')  THEN 'paid_search'
    WHEN utm_medium IN ('cpc','ppc','paid')
         AND utm_source IN ('facebook','instagram','linkedin','twitter') THEN 'paid_social'
    WHEN utm_medium = 'email'                 THEN 'email'
    WHEN utm_source IN ('google','bing')
         AND utm_medium = 'organic'           THEN 'organic'
    ELSE 'other'
END
```

**dbt source freshness checks (in `schema.yml`):**
```yaml
- name: mixpanel_events
  loaded_at_field: _extracted_at
  freshness:
    warn_after: {count: 12, period: hour}
    error_after: {count: 24, period: hour}
```

---

### Step 6 — dbt Mart Models (Gold Layer)

**What happens:** dbt builds business-ready **fact tables** in the `marts` dataset. These are the tables BI tools and analysts query directly. All mart models are:
- Partitioned by date (BigQuery partition pruning = faster queries and lower cost)
- Clustered on high-cardinality dimensions (channel, campaign, event_name)
- Built as `table` (materialised; not views — ensures predictable query performance)

```
dbt/models/marts/
├── schema.yml                               ← Model-level tests + descriptions
├── marketing/
│   ├── fct_campaign_performance.sql         ← Unified Google Ads + Meta by campaign/date/device
│   ├── fct_ad_spend_roi.sql                 ← ROAS, ROI, performance tier by channel/date
│   ├── fct_mixpanel_events.sql              ← Event counts by name/channel/campaign/country/device/date
│   └── fct_mixpanel_campaign_attribution.sql ← UTM-joined spend attribution + funnel rates
├── growth/
│   ├── fct_funnel_conversion.sql            ← Weekly Lead→MQL→SQL→Opp→Customer rates
│   └── fct_mixpanel_funnel.sql              ← Step conversion rates + avg time-to-convert
└── revenue/
    ├── fct_revenue.sql                      ← Daily Shopify gross/net revenue + AOV
    └── fct_attribution.sql                  ← Last-touch/linear channel attribution
```

**Key mart — `fct_ad_spend_roi.sql` performance tiers:**
```sql
CASE
    WHEN roas >= 4.0 THEN 'high_performer'
    WHEN roas >= 2.0 THEN 'mid_performer'
    WHEN roas >= 1.0 THEN 'break_even'
    ELSE                   'underperformer'
END AS performance_tier
```

**Key mart — `fct_funnel_conversion.sql` (HubSpot CRM data):**
```sql
lead_to_mql_rate         = mqls / total_leads
mql_to_sql_rate          = sqls / mqls
sql_to_opp_rate          = opportunities / sqls
opp_to_customer_rate     = customers / opportunities
overall_conversion_rate  = customers / total_leads
```

---

### Step 7 — Orchestration with Apache Airflow

**What happens:** Five Airflow DAGs schedule and monitor the end-to-end pipeline. Each DAG owns one logical domain and runs on a defined schedule.

| DAG | Schedule | Sources | dbt models triggered |
|-----|----------|---------|---------------------|
| `marketing_pipeline_dag` | `0 6 * * *` (06:00 UTC) | Google Ads, Meta Ads, LinkedIn Ads | `fct_campaign_performance`, `fct_ad_spend_roi` |
| `crm_pipeline_dag` | `0 5 * * *` (05:00 UTC) | HubSpot, Salesforce | `fct_funnel_conversion` |
| `ecommerce_analytics_dag` | `0 7 * * *` (07:00 UTC) | Shopify, GA4 | `fct_revenue`, `fct_attribution` |
| `mixpanel_analytics_dag` | `30 7 * * *` (07:30 UTC) | Mixpanel Events/Segments/Funnels | `fct_mixpanel_events`, `fct_mixpanel_funnel`, `fct_mixpanel_campaign_attribution` |
| `data_quality_dag` | `0 9 * * *` (09:00 UTC) | All tables | Freshness + row count + null rate checks |

**DAG task dependency pattern:**
```
[extract_task_1, extract_task_2, extract_task_3]  ← parallel extraction
        └──────────────────────┬──────────────────
                               ▼
                    dbt_run_models (BashOperator)
                               │
                               ▼
                    dbt_test_models (BashOperator)
```

**Reliability features:**
- `retries: 3` with `retry_delay: timedelta(minutes=5)` on every task
- `catchup=False` — does not backfill missed runs automatically
- `max_active_runs=1` — prevents overlapping DAG runs
- Slack alerts via `on_failure_callback` when any task fails

**Docker Compose services:**
```yaml
services:
  postgres:        # Airflow metadata database
  airflow-init:    # One-time DB migration + admin user creation
  airflow-webserver: (port 8080)
  airflow-scheduler:
```

---

### Step 8 — Data Quality Layer

**What happens:** A dedicated quality layer runs validation checks and monitoring independently from the main pipeline, surfacing issues before analysts encounter bad data.

**Two components:**

#### `MarketingDataValidator` — SQL-based assertions
Each check queries BigQuery and asserts a `bad_rows` count equals 0:

| Check | Table | What it catches |
|-------|-------|-----------------|
| `check_no_negative_spend` | `fct_campaign_performance` | Negative cost values (API bugs, sign errors) |
| `check_no_future_dates` | `google_ads_campaigns` | Events timestamped in the future |
| `check_ctr_bounds` | `fct_campaign_performance` | CTR outside [0, 1] — impossible values |
| `check_roas_not_astronomical` | `fct_campaign_performance` | ROAS > 1,000x with real spend (data anomaly) |
| `check_funnel_stage_order` | `fct_funnel_conversion` | MQLs > total leads (impossible stage ordering) |
| `check_revenue_not_negative` | `fct_revenue` | Negative gross/net revenue |
| `check_duplicate_campaign_rows` | `fct_campaign_performance` | Duplicate grain violations |
| `check_mixpanel_event_counts` | `mixpanel_events` | < 50 events loaded (pipeline failure signal) |
| `check_no_future_mixpanel_events` | `mixpanel_events` | Future-dated events (clock skew) |

#### `PipelineMonitor` — Operational health checks
Runs freshness, row count, and null rate checks:

| Monitor | Threshold | Alert trigger |
|---------|-----------|---------------|
| Data freshness | Per-table max hours | Timestamp of `_extracted_at` > threshold |
| Row count minimum | Per-table minimum | Row count below expected minimum for a date |
| Null rate maximum | Per-column maximum | Null % exceeds acceptable level |

---

### Step 9 — CI/CD with GitHub Actions

**Two workflows:**

#### `.github/workflows/ci.yml` — Runs on every pull request
```
Job 1: Python Tests
  → pip install -r requirements.txt
  → pytest tests/ --cov=etl --cov=data_quality --cov-report=xml
  → Upload coverage to Codecov

Job 2: Lint (ruff)
  → ruff check etl/ dags/ data_quality/ config/ scripts/ tests/

Job 3: dbt Compile Check
  → dbt compile (validates all SQL without executing)
```

#### `.github/workflows/dbt_run.yml` — Runs daily at 08:00 UTC
```
→ dbt run --select marts.*   (refresh all Gold tables)
→ dbt test                   (run all schema.yml tests)
```

**Testing approach (`tests/test_extractors.py`):**
- `pytest-mock` — patches all outbound API calls; no real credentials needed in CI
- `responses` — intercepts HTTP requests; simulates API success and error responses
- `freezegun` — freezes `datetime.now()` for deterministic date-range calculations

---

### Step 10 — Mixpanel Behavioral Analytics Integration

**What happens:** Mixpanel provides the **behavioral layer** — it answers *what users actually do* inside the product, complementing the *how users are acquired* data from paid media sources.

**Three API surfaces are integrated:**

| Extractor | API | Grain | Key use case |
|-----------|-----|-------|-------------|
| `MixpanelEventExtractor` | Data Export API | One row per raw event | Funnel drop-off analysis, path analysis |
| `MixpanelSegmentExtractor` | Query/Segmentation API | Aggregated event counts per segment | Event trends by UTM, country, device |
| `MixpanelFunnelExtractor` | Funnels API | One row per funnel step per date | Conversion rates; time-to-convert KPIs |

**Authentication flexibility:**
```python
# Preferred: Service Account (project-scoped)
MIXPANEL_SERVICE_ACCOUNT_USERNAME=service-account-xxx@developer.mixpanel.com
MIXPANEL_SERVICE_ACCOUNT_SECRET=secret_xxx

# Legacy: API Secret (older projects)
MIXPANEL_API_SECRET=abc123

# EU data residency
MIXPANEL_REGION=EU   # routes to eu.data.mixpanel.com
```

**Business insight enabled by Mixpanel + paid media join (`fct_mixpanel_campaign_attribution.sql`):**
```sql
-- Mixpanel-attributed CPA (events showing purchase intent)
cpa_orders  = total_spend / orders
cpa_signups = total_spend / signups

-- On-site funnel rates from Mixpanel events
cart_to_checkout_rate   = checkout_starts / add_to_carts
checkout_to_order_rate  = orders / checkout_starts
```

---

## 5. Medallion Architecture Deep-Dive

The pipeline uses a three-tier **Medallion Architecture** (Bronze → Silver → Gold) which provides clear separation of concerns and allows independent access at each tier.

```
BRONZE (raw)           SILVER (staging)          GOLD (marts)
─────────────────      ────────────────────       ────────────────────────
• Exact API payload    • Type-safe views          • Pre-aggregated tables
• Schema-on-write      • Deduplication applied    • Business KPIs computed
• Never deleted        • Channel normalised        • Partitioned by date
• Merge on natural     • dbt tests enforced        • Clustered for BI tools
  primary keys         • No data duplication       • Ready for dashboards
• _extracted_at        • Freshness validated        • dbt tested + documented
  timestamp on every   
  row                  
```

**Why three tiers?**
- **Bronze** is the audit trail — you can always re-run dbt from raw if a transformation bug is found
- **Silver** is safe for data science teams — clean data, no raw API quirks
- **Gold** is optimised for BI tools — pre-aggregated, partitioned, fast query times, low BigQuery scan cost

---

## 6. KPI Catalog

All KPIs are defined in `schemas/kpi_definitions.yaml` as the single source of truth.

### Paid Media KPIs

| KPI | Formula | Source table |
|-----|---------|-------------|
| CTR | `clicks / impressions` | `fct_campaign_performance` |
| CPC | `cost / clicks` | `fct_campaign_performance` |
| CPM | `(cost / impressions) × 1000` | `fct_campaign_performance` |
| CVR | `conversions / clicks` | `fct_campaign_performance` |
| CPA | `cost / conversions` | `fct_campaign_performance` |
| ROAS | `conversion_value / cost` | `fct_ad_spend_roi` |
| ROI | `(revenue − cost) / cost` | `fct_ad_spend_roi` |

### Funnel / CRM KPIs (HubSpot)

| KPI | Formula | Source table |
|-----|---------|-------------|
| Lead → MQL rate | `mqls / total_leads` | `fct_funnel_conversion` |
| MQL → SQL rate | `sqls / mqls` | `fct_funnel_conversion` |
| SQL → Opp rate | `opportunities / sqls` | `fct_funnel_conversion` |
| Opp → Customer rate | `customers / opportunities` | `fct_funnel_conversion` |
| Overall conversion | `customers / total_leads` | `fct_funnel_conversion` |

### Revenue KPIs (Shopify)

| KPI | Formula | Source table |
|-----|---------|-------------|
| Gross Revenue | `SUM(subtotal_price)` | `fct_revenue` |
| Net Revenue | `gross − discounts − refunds` | `fct_revenue` |
| AOV | `revenue / order_count` | `fct_revenue` |
| Repeat Rate | `repeat_customers / total_customers` | `fct_revenue` |

### Behavioral KPIs (Mixpanel)

| KPI | Formula | Source table |
|-----|---------|-------------|
| Events per User | `event_count / unique_users` | `fct_mixpanel_events` |
| Step Conversion Rate | `users_converted_at_step / users_entered` | `fct_mixpanel_funnel` |
| Cumulative Drop-Off Rate | `1 − (step_n_users / step_0_users)` | `fct_mixpanel_funnel` |
| Avg Time to Convert | `avg_time_to_convert_sec / 3600` | `fct_mixpanel_funnel` |
| Overall Funnel Conversion | `final_step_users / entry_step_users` | `fct_mixpanel_funnel` |

---

## 7. Security & Secrets Management

| Practice | Implementation |
|----------|---------------|
| No secrets in code | All credentials in `.env`; `.gitignore` excludes `.env` and `credentials/` |
| No secrets in source control | `GOOGLE_APPLICATION_CREDENTIALS` points to a mounted volume, not an inline key |
| Immutable config | `frozen=True` dataclasses prevent runtime mutation of credentials |
| Service account principle of least privilege | BigQuery Editor role only (no Owner or Admin) |
| Airflow Fernet key | `AIRFLOW__CORE__FERNET_KEY` encrypts connection passwords in Postgres |
| Mixpanel auth | Service Account preferred over legacy API secret; key never logged |
| API credentials scoped per environment | `PIPELINE_ENV=development|staging|production` controls dataset routing |

---

## 8. Scalability & Performance Design

| Concern | Solution |
|---------|---------|
| Large event volumes (Mixpanel) | NDJSON streaming with `stream=True` + `iter_lines()` — never loads full response into memory |
| API rate limits | `tenacity` exponential backoff; date chunking (`chunk_days=1` for export API) |
| BigQuery scan cost | Day-partitioned tables on date field; clustered on campaign/channel/event_name |
| Airflow scaling | `LocalExecutor` for dev; swap to `KubernetesExecutor` + Celery for multi-node production |
| DAG parallelism | All extraction tasks within a DAG run in parallel (no inter-extractor dependencies) |
| dbt performance | Staging models as views (no storage cost); mart tables materialised for BI speed |
| Incremental loads | MERGE write mode prevents full table rewrites on every run |
| Lookback window | Configurable `lookback_days` (default 30) ensures late-arriving data is captured |

---

## 9. Deployment Guide

### Local Development

```bash
# 1. Clone the repository
git clone https://github.com/arvinbalinado/marketinggrowth.git
cd marketinggrowth

# 2. Create and populate .env
cp .env.example .env
# Edit .env with your credentials

# 3. Place GCP service account key at:
mkdir -p credentials/
# Copy your_service_account.json → credentials/service_account.json

# 4. Start Airflow (first run only — initialises DB and admin user)
docker-compose up airflow-init
docker-compose up -d

# 5. Access Airflow UI
open http://localhost:8080
# Login: admin / admin

# 6. Run a single pipeline manually
python scripts/run_pipeline.py --source google_ads --start-date 2026-04-01 --end-date 2026-04-20

# 7. Run dbt transformations
cd dbt/
dbt deps                    # install packages
dbt run --select staging    # Silver layer
dbt run --select marts      # Gold layer
dbt test                    # run all schema tests
```

### Available CLI Sources

```bash
python scripts/run_pipeline.py --source google_ads        --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source meta_ads          --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source linkedin_ads      --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source ga4               --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source hubspot           --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source shopify           --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source mixpanel_events   --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source mixpanel_segments --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source mixpanel_funnels  --start-date YYYY-MM-DD
python scripts/run_pipeline.py --source all               --start-date YYYY-MM-DD
```

---

## 10. Monitoring & Alerting

### Slack Alerts

The `PipelineMonitor` sends Slack messages to the configured webhook on:
- **Freshness breach:** table not updated within threshold hours
- **Row count drop:** fewer rows than expected for a date
- **Null rate spike:** critical column null rate exceeds maximum
- **Airflow task failure:** via `on_failure_callback`

Set `SLACK_WEBHOOK_URL` in `.env` to enable.

### Data Freshness Thresholds

| Table | Warn after | Error after |
|-------|-----------|------------|
| `raw.google_ads_campaigns` | 6 hours | 24 hours |
| `raw.meta_ads_insights` | 6 hours | 24 hours |
| `raw.ga4_sessions` | 6 hours | 24 hours |
| `raw.hubspot_objects` | 12 hours | 48 hours |
| `raw.shopify_raw` | 6 hours | 24 hours |
| `raw.mixpanel_events` | 12 hours | 24 hours |
| `raw.mixpanel_segmentation` | 12 hours | 24 hours |
| `raw.mixpanel_funnel_conversions` | 24 hours | 48 hours |

### Minimum Row Counts (per execution date)

| Table | Minimum rows |
|-------|-------------|
| `raw.google_ads_campaigns` | 10 |
| `raw.meta_ads_insights` | 10 |
| `raw.ga4_sessions` | 100 |
| `raw.mixpanel_events` | 50 |
