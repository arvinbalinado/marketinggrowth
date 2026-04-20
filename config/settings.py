"""
config/settings.py — Central configuration loader.
Reads from environment variables (populated via .env).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


# ── BigQuery ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class BigQueryConfig:
    project_id: str = field(default_factory=lambda: os.environ["GCP_PROJECT_ID"])
    region: str = field(default_factory=lambda: os.getenv("GCP_REGION", "us-central1"))
    credentials_path: str = field(
        default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    )
    dataset_raw: str = field(default_factory=lambda: os.getenv("BIGQUERY_DATASET_RAW", "raw"))
    dataset_staging: str = field(
        default_factory=lambda: os.getenv("BIGQUERY_DATASET_STAGING", "staging")
    )
    dataset_marts: str = field(
        default_factory=lambda: os.getenv("BIGQUERY_DATASET_MARTS", "marts")
    )


# ── Google Ads ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class GoogleAdsConfig:
    developer_token: str = field(
        default_factory=lambda: os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
    )
    client_id: str = field(default_factory=lambda: os.getenv("GOOGLE_ADS_CLIENT_ID", ""))
    client_secret: str = field(
        default_factory=lambda: os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
    )
    refresh_token: str = field(
        default_factory=lambda: os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
    )
    login_customer_id: str = field(
        default_factory=lambda: os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
    )


# ── Meta / Facebook Ads ───────────────────────────────────────────────────────
@dataclass(frozen=True)
class MetaAdsConfig:
    app_id: str = field(default_factory=lambda: os.getenv("META_APP_ID", ""))
    app_secret: str = field(default_factory=lambda: os.getenv("META_APP_SECRET", ""))
    access_token: str = field(default_factory=lambda: os.getenv("META_ACCESS_TOKEN", ""))
    ad_account_id: str = field(default_factory=lambda: os.getenv("META_AD_ACCOUNT_ID", ""))


# ── Google Analytics 4 ────────────────────────────────────────────────────────
@dataclass(frozen=True)
class GA4Config:
    property_id: str = field(default_factory=lambda: os.getenv("GA4_PROPERTY_ID", ""))
    service_account_email: str = field(
        default_factory=lambda: os.getenv("GA4_SERVICE_ACCOUNT_EMAIL", "")
    )


# ── HubSpot ───────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class HubSpotConfig:
    api_key: str = field(default_factory=lambda: os.getenv("HUBSPOT_API_KEY", ""))
    portal_id: str = field(default_factory=lambda: os.getenv("HUBSPOT_PORTAL_ID", ""))


# ── Salesforce ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SalesforceConfig:
    username: str = field(default_factory=lambda: os.getenv("SALESFORCE_USERNAME", ""))
    password: str = field(default_factory=lambda: os.getenv("SALESFORCE_PASSWORD", ""))
    security_token: str = field(
        default_factory=lambda: os.getenv("SALESFORCE_SECURITY_TOKEN", "")
    )
    domain: str = field(default_factory=lambda: os.getenv("SALESFORCE_DOMAIN", "login"))


# ── Shopify ───────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ShopifyConfig:
    shop_url: str = field(default_factory=lambda: os.getenv("SHOPIFY_SHOP_URL", ""))
    api_key: str = field(default_factory=lambda: os.getenv("SHOPIFY_API_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.getenv("SHOPIFY_API_SECRET", ""))
    access_token: str = field(default_factory=lambda: os.getenv("SHOPIFY_ACCESS_TOKEN", ""))
    api_version: str = field(
        default_factory=lambda: os.getenv("SHOPIFY_API_VERSION", "2024-01")
    )


# ── LinkedIn Ads ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class LinkedInAdsConfig:
    client_id: str = field(default_factory=lambda: os.getenv("LINKEDIN_CLIENT_ID", ""))
    client_secret: str = field(
        default_factory=lambda: os.getenv("LINKEDIN_CLIENT_SECRET", "")
    )
    access_token: str = field(
        default_factory=lambda: os.getenv("LINKEDIN_ACCESS_TOKEN", "")
    )
    ad_account_id: str = field(
        default_factory=lambda: os.getenv("LINKEDIN_AD_ACCOUNT_ID", "")
    )


# ── Mixpanel ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MixpanelConfig:
    project_id: str = field(default_factory=lambda: os.getenv("MIXPANEL_PROJECT_ID", ""))
    service_account_username: str = field(
        default_factory=lambda: os.getenv("MIXPANEL_SERVICE_ACCOUNT_USERNAME", "")
    )
    service_account_secret: str = field(
        default_factory=lambda: os.getenv("MIXPANEL_SERVICE_ACCOUNT_SECRET", "")
    )
    # Optional: used when exporting raw events via the Data Export API
    api_secret: str = field(
        default_factory=lambda: os.getenv("MIXPANEL_API_SECRET", "")
    )
    region: str = field(
        default_factory=lambda: os.getenv("MIXPANEL_REGION", "US")  # US or EU
    )


# ── Slack Alerts ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SlackConfig:
    webhook_url: str = field(default_factory=lambda: os.getenv("SLACK_WEBHOOK_URL", ""))
    alert_channel: str = field(
        default_factory=lambda: os.getenv("SLACK_ALERT_CHANNEL", "#data-alerts")
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class PipelineConfig:
    env: str = field(default_factory=lambda: os.getenv("PIPELINE_ENV", "development"))
    default_lookback_days: int = field(
        default_factory=lambda: int(os.getenv("DEFAULT_LOOKBACK_DAYS", "30"))
    )
    max_retry_attempts: int = field(
        default_factory=lambda: int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    )
    timezone: str = field(default_factory=lambda: os.getenv("PIPELINE_TIMEZONE", "UTC"))


@dataclass(frozen=True)
class Settings:
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    google_ads: GoogleAdsConfig = field(default_factory=GoogleAdsConfig)
    meta_ads: MetaAdsConfig = field(default_factory=MetaAdsConfig)
    ga4: GA4Config = field(default_factory=GA4Config)
    hubspot: HubSpotConfig = field(default_factory=HubSpotConfig)
    salesforce: SalesforceConfig = field(default_factory=SalesforceConfig)
    shopify: ShopifyConfig = field(default_factory=ShopifyConfig)
    linkedin_ads: LinkedInAdsConfig = field(default_factory=LinkedInAdsConfig)
    mixpanel: MixpanelConfig = field(default_factory=MixpanelConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
