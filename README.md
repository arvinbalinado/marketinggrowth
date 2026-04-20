# Marketing & Growth Data Architecture

A production-grade, open-source ETL/ELT data pipeline for marketing and growth analytics. Ingests data from 6 sources, transforms it using **dbt**, loads it to **BigQuery**, and serves BI tools like Looker, Power BI, and Zoho Analytics.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│  Google Ads │ Meta Ads │ LinkedIn Ads │ GA4 │ HubSpot │ Shopify     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Python ETL extractors (REST APIs)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION — Apache Airflow                      │
│   marketing_pipeline_dag │ crm_pipeline_dag │ ecommerce_dag         │
│   data_quality_dag                                                   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              BIGQUERY DATA WAREHOUSE                                 │
│                                                                      │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────────────────┐   │
│  │  raw     │ →  │   staging   │ →  │        marts             │   │
│  │ (Bronze) │    │  (Silver)   │    │       (Gold)             │   │
│  │          │    │ dbt views   │    │  marketing/ growth/      │   │
│  │ raw API  │    │ cleaned &   │    │  revenue/ attribution    │   │
│  │ payloads │    │ typed data  │    │  dbt tables              │   │
│  └──────────┘    └─────────────┘    └──────────────────────────┘   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BI / ANALYTICS LAYER                             │
│       Looker │ Power BI │ Zoho Analytics │ Google Sheets            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
marketinggrowth/
├── config/               # Environment settings and source definitions
├── dags/                 # Airflow DAGs (4 pipelines)
├── etl/
│   ├── extractors/       # 6 source extractors (Google Ads, Meta, LinkedIn, GA4, HubSpot, Shopify)
│   ├── transformers/     # Campaign, attribution, and funnel transformers
│   └── loaders/          # BigQuery loader with append/overwrite/merge
├── dbt/                  # dbt project
│   └── models/
│       ├── staging/      # Cleaned source views
│       └── marts/        # Business-ready fact tables
│           ├── marketing/    fct_campaign_performance, fct_ad_spend_roi
│           ├── growth/       fct_funnel_conversion
│           └── revenue/      fct_revenue, fct_attribution
├── data_quality/
│   ├── validators/       # SQL-based data quality checks
│   └── monitors/         # Freshness, row count, null rate monitors
├── schemas/              # BigQuery JSON schemas + KPI definitions
└── .github/workflows/    # CI/CD (pytest + dbt CI)
```

---

## Quick Start

### 1. Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Google Cloud project with BigQuery enabled
- Service account with BigQuery Editor role

### 2. Clone and configure
```bash
git clone https://github.com/arvinbalinado/marketinggrowth.git
cd marketinggrowth
cp .env.example .env
# Edit .env with your actual credentials
```

### 3. Start Airflow
```bash
docker-compose up airflow-init
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
```

### 4. Set up dbt
```bash
pip install dbt-bigquery
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your project details
cd dbt && dbt deps && dbt debug
```

### 5. Run a manual pipeline
```bash
python scripts/run_pipeline.py --source google_ads --start-date 2026-01-01 --end-date 2026-04-20
```

---

## Data Sources & Tables

| Source | Raw Table | Frequency | Lookback |
|--------|-----------|-----------|----------|
| Google Ads | `raw.google_ads_campaigns` | Daily 06:00 UTC | 2 days |
| Meta Ads | `raw.meta_ads_insights` | Daily 06:00 UTC | 2 days |
| LinkedIn Ads | `raw.linkedin_ads_analytics` | Daily 06:00 UTC | 2 days |
| Google Analytics 4 | `raw.ga4_sessions` | Daily 07:00 UTC | 3 days |
| HubSpot | `raw.hubspot_objects` | Daily 05:00 UTC | 7 days |
| Shopify | `raw.shopify_raw` | Daily 07:00 UTC | 3 days |

---

## Key Mart Tables

| Table | Description | Primary Use |
|-------|-------------|-------------|
| `marts.fct_campaign_performance` | Unified paid media KPIs | Campaign dashboards |
| `marts.fct_ad_spend_roi` | ROAS/ROI by channel & campaign | Budget optimization |
| `marts.fct_funnel_conversion` | Lead→MQL→SQL→Customer rates | Growth reporting |
| `marts.fct_revenue` | Shopify order revenue with MoM growth | Finance dashboards |
| `marts.fct_attribution` | Channel revenue attribution | Attribution modelling |

---

## Connecting BI Tools

### Looker / Power BI
Connect directly to BigQuery using a service account. Use the `marts.*` dataset as the primary data source.

### Google Sheets (via BigQuery Data Transfer)
1. In BigQuery, schedule a query to export `fct_campaign_performance` to Sheets
2. Alternatively, use the BigQuery connector in Google Sheets

### Zoho Analytics
Use the BigQuery integration under *Data Sources → Cloud Databases → BigQuery*.

---

## Data Quality

Quality checks run daily at **09:00 UTC** via `data_quality_dag`:
- No negative spend
- CTR within 0–100%
- No future-dated records
- Funnel stage order consistency
- Slack alerts on failure

---

## Environment Variables

See [.env.example](.env.example) for the full list of required variables.

---

## Contributing

1. Branch from `main`
2. Run `pytest tests/` before pushing
3. dbt models require `dbt test` to pass
4. See [docs/pipeline_guide.md](docs/pipeline_guide.md) for detailed developer docs