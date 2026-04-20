# Data Dictionary — Marketing & Growth Pipeline

## Raw Layer (`raw` dataset)

### `raw.google_ads_campaigns`
| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | STRING | Google Ads campaign ID |
| `campaign_name` | STRING | Campaign display name |
| `campaign_status` | STRING | ENABLED / PAUSED / REMOVED |
| `channel_type` | STRING | SEARCH / DISPLAY / VIDEO / etc. |
| `ad_group_id` | STRING | Ad group ID |
| `ad_group_name` | STRING | Ad group display name |
| `date` | DATE | Performance date |
| `device` | STRING | DESKTOP / MOBILE / TABLET |
| `impressions` | INTEGER | Times ad was shown |
| `clicks` | INTEGER | Number of clicks |
| `cost` | FLOAT | Spend in USD (converted from micros) |
| `conversions` | FLOAT | Conversion count |
| `conversion_value` | FLOAT | Conversion revenue value |
| `_extracted_at` | TIMESTAMP | When row was extracted |

### `raw.meta_ads_insights`
| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | STRING | Meta campaign ID |
| `adset_id` | STRING | Ad set ID |
| `ad_id` | STRING | Individual ad ID |
| `date_start` | DATE | Report date |
| `impressions` | INTEGER | Total impressions |
| `clicks` | INTEGER | Link clicks |
| `spend` | FLOAT | Amount spent (USD) |
| `reach` | INTEGER | Unique users reached |
| `frequency` | FLOAT | Avg impressions per user |
| `action_purchase` | FLOAT | Purchase conversions |
| `action_lead` | FLOAT | Lead form completions |

### `raw.hubspot_objects`
| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | HubSpot object ID |
| `object_type` | STRING | contact / deal / company |
| `email` | STRING | Contact email |
| `lifecyclestage` | STRING | lead / mql / sql / opportunity / customer |
| `hs_analytics_source` | STRING | Original traffic source |
| `createdate` | TIMESTAMP | Record creation timestamp |

---

## Staging Layer (`staging` dataset)

### `staging.stg_google_ads`
Cleaned view of `raw.google_ads_campaigns` with derived KPIs:
- `ctr` = clicks / impressions
- `cpc` = cost / clicks
- `cpm` = (cost / impressions) × 1000
- `cvr` = conversions / clicks
- `cpa` = cost / conversions
- `roas` = conversion_value / cost

### `staging.stg_hubspot_contacts`
Cleaned contacts with boolean stage flags: `is_mql`, `is_sql`, `is_opportunity`, `is_customer`.

---

## Mart Layer (`marts` dataset)

### `marts.fct_campaign_performance`
**Grain:** One row per campaign + ad_group + date + device + channel_source

Key columns:
| Column | Description |
|--------|-------------|
| `channel_source` | google_ads / meta_ads / linkedin_ads |
| `cost_usd` | Ad spend |
| `roas` | Revenue / spend ratio |
| `performance_tier` | high_performer / mid_performer / break_even / underperformer |

### `marts.fct_funnel_conversion`
**Grain:** One row per week_start + channel

Key columns:
| Column | Description |
|--------|-------------|
| `total_leads` | New contacts created |
| `mqls` | Marketing Qualified Leads |
| `sqls` | Sales Qualified Leads |
| `customers` | Converted customers |
| `lead_to_mql_rate` | MQL conversion rate |
| `overall_conversion_rate` | Lead → customer rate |

### `marts.fct_revenue`
**Grain:** One row per order_date + currency + traffic_source + order_type

Key columns:
| Column | Description |
|--------|-------------|
| `gross_revenue` | Total order value |
| `net_revenue` | Revenue minus tax |
| `avg_order_value` | Average order size |
| `revenue_mom_growth_rate` | Month-over-month growth |

### `marts.fct_attribution`
**Grain:** One row per date + channel + campaign

Key columns:
| Column | Description |
|--------|-------------|
| `attributed_revenue` | Revenue attributed to channel |
| `attributed_roas` | Attributed revenue / spend |
| `sessions` | Traffic sessions per channel |
