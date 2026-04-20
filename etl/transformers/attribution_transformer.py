"""
etl/transformers/attribution_transformer.py
Builds last-touch, first-touch, and linear multi-touch attribution models
from GA4 session data and order data.
"""
from __future__ import annotations

import pandas as pd

from etl.transformers.base_transformer import BaseTransformer


class AttributionTransformer(BaseTransformer):
    source_name = "attribution"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expects a merged DataFrame with columns:
          customer_id, session_date, channel, source, medium,
          campaign, order_id, order_value, is_conversion
        """
        df = df.copy()

        if df.empty:
            return df

        df["session_date"] = pd.to_datetime(df.get("session_date"), errors="coerce")
        df = df.sort_values(["customer_id", "session_date"])

        conversions = df[df.get("is_conversion", False) == True].copy()

        # ── Last-touch attribution ────────────────────────────────────────────
        last_touch = (
            df.groupby("customer_id")
            .last()
            .reset_index()[["customer_id", "channel", "source", "medium", "campaign"]]
            .rename(
                columns={
                    "channel": "last_touch_channel",
                    "source": "last_touch_source",
                    "medium": "last_touch_medium",
                    "campaign": "last_touch_campaign",
                }
            )
        )

        # ── First-touch attribution ───────────────────────────────────────────
        first_touch = (
            df.groupby("customer_id")
            .first()
            .reset_index()[["customer_id", "channel", "source", "medium", "campaign"]]
            .rename(
                columns={
                    "channel": "first_touch_channel",
                    "source": "first_touch_source",
                    "medium": "first_touch_medium",
                    "campaign": "first_touch_campaign",
                }
            )
        )

        # ── Linear attribution (equal credit per touchpoint) ──────────────────
        touch_counts = df.groupby("customer_id")["session_date"].count().rename("touchpoint_count")
        linear = (
            df.merge(touch_counts, on="customer_id")
            .assign(linear_credit=lambda x: 1.0 / x["touchpoint_count"])
        )

        linear_summary = (
            linear.groupby(["customer_id", "channel"])["linear_credit"]
            .sum()
            .reset_index()
            .rename(columns={"linear_credit": "linear_attribution_credit"})
        )

        # ── Merge all models ──────────────────────────────────────────────────
        result = conversions.merge(last_touch, on="customer_id", how="left")
        result = result.merge(first_touch, on="customer_id", how="left")

        return result


class FunnelTransformer(BaseTransformer):
    source_name = "funnel"

    # Define the ordered funnel stages
    FUNNEL_STAGES = [
        "visitor",
        "lead",
        "mql",       # Marketing Qualified Lead
        "sql",       # Sales Qualified Lead
        "opportunity",
        "customer",
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expects a DataFrame with:
          customer_id, stage, event_date, channel, campaign
        """
        df = df.copy()
        if df.empty:
            return df

        df["event_date"] = pd.to_datetime(df.get("event_date"), errors="coerce")
        df["stage_order"] = df["stage"].map(
            {stage: i for i, stage in enumerate(self.FUNNEL_STAGES)}
        )

        # Stage entry counts per period
        stage_counts = (
            df.groupby(["event_date", "stage", "channel"])["customer_id"]
            .nunique()
            .reset_index()
            .rename(columns={"customer_id": "unique_visitors"})
        )

        # Conversion rates between adjacent stages
        stage_pivot = (
            df.groupby(["customer_id"])["stage_order"].max().reset_index()
        )
        stage_pivot["max_stage"] = stage_pivot["stage_order"].map(
            {i: s for i, s in enumerate(self.FUNNEL_STAGES)}
        )

        conversion_rates: list[dict] = []
        for i in range(len(self.FUNNEL_STAGES) - 1):
            from_stage = self.FUNNEL_STAGES[i]
            to_stage = self.FUNNEL_STAGES[i + 1]
            from_count = (stage_pivot["stage_order"] >= i).sum()
            to_count = (stage_pivot["stage_order"] >= i + 1).sum()
            conversion_rates.append(
                {
                    "from_stage": from_stage,
                    "to_stage": to_stage,
                    "from_count": int(from_count),
                    "to_count": int(to_count),
                    "conversion_rate": float(to_count / from_count) if from_count else 0.0,
                }
            )

        return pd.DataFrame(conversion_rates)
