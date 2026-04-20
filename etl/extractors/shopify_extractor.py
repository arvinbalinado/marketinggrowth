"""
etl/extractors/shopify_extractor.py
Extracts orders, customers, and products from Shopify Admin REST API.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class ShopifyExtractor(BaseExtractor):
    source_name = "shopify"

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().shopify

    def validate_config(self) -> None:
        cfg = self._cfg
        missing = [
            name
            for name, val in {
                "shop_url": cfg.shop_url,
                "access_token": cfg.access_token,
            }.items()
            if not val
        ]
        if missing:
            raise ValueError(f"Missing Shopify credentials: {missing}")

    @property
    def _base_url(self) -> str:
        cfg = self._cfg
        return (
            f"https://{cfg.shop_url}/admin/api/{cfg.api_version}"
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {"X-Shopify-Access-Token": self._cfg.access_token}

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        orders_df = self._extract_orders(start_date, end_date)
        customers_df = self._extract_customers(start_date, end_date)

        orders_df["object_type"] = "order"
        customers_df["object_type"] = "customer"

        return pd.concat(
            [orders_df, customers_df], ignore_index=True, sort=False
        )

    def _extract_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        params = {
            "status": "any",
            "created_at_min": f"{start_date.isoformat()}T00:00:00Z",
            "created_at_max": f"{end_date.isoformat()}T23:59:59Z",
            "limit": 250,
            "fields": (
                "id,created_at,updated_at,email,financial_status,"
                "fulfillment_status,total_price,subtotal_price,"
                "total_tax,total_discounts,currency,customer,"
                "line_items,discount_codes,source_name,"
                "referring_site,landing_site,cancelled_at"
            ),
        }
        records = self._paginate(f"{self._base_url}/orders.json", "orders", params)

        rows: list[dict] = []
        for order in records:
            base = {
                "order_id": order.get("id"),
                "created_at": order.get("created_at"),
                "updated_at": order.get("updated_at"),
                "email": order.get("email"),
                "financial_status": order.get("financial_status"),
                "fulfillment_status": order.get("fulfillment_status"),
                "total_price": float(order.get("total_price") or 0),
                "subtotal_price": float(order.get("subtotal_price") or 0),
                "total_tax": float(order.get("total_tax") or 0),
                "total_discounts": float(order.get("total_discounts") or 0),
                "currency": order.get("currency"),
                "customer_id": (order.get("customer") or {}).get("id"),
                "source_name": order.get("source_name"),
                "referring_site": order.get("referring_site"),
                "landing_site": order.get("landing_site"),
                "cancelled_at": order.get("cancelled_at"),
                "item_count": len(order.get("line_items") or []),
                "discount_codes": ",".join(
                    [d.get("code", "") for d in (order.get("discount_codes") or [])]
                ),
            }
            rows.append(base)

        df = pd.DataFrame(rows)
        if not df.empty:
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df

    def _extract_customers(self, start_date: date, end_date: date) -> pd.DataFrame:
        params = {
            "created_at_min": f"{start_date.isoformat()}T00:00:00Z",
            "created_at_max": f"{end_date.isoformat()}T23:59:59Z",
            "limit": 250,
            "fields": (
                "id,email,first_name,last_name,created_at,updated_at,"
                "orders_count,total_spent,accepts_marketing,"
                "tags,state,currency"
            ),
        }
        records = self._paginate(
            f"{self._base_url}/customers.json", "customers", params
        )
        df = pd.DataFrame(records)
        if not df.empty:
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df

    def _paginate(
        self, url: str, key: str, params: dict
    ) -> list[dict]:
        """Follow Shopify cursor-based pagination, return all records."""
        records: list[dict] = []
        next_url: str | None = url
        current_params: dict | None = params

        while next_url:
            response = requests.get(
                next_url,
                headers=self._headers,
                params=current_params,
                timeout=30,
            )
            response.raise_for_status()
            records.extend(response.json().get(key, []))

            # Cursor pagination via Link header
            link_header = response.headers.get("Link", "")
            next_url = None
            current_params = None
            if 'rel="next"' in link_header:
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        next_url = part.split(";")[0].strip().strip("<>")
                        break

        return records
