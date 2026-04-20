"""
etl/extractors/salesforce_extractor.py
Extracts Leads and Opportunities from Salesforce using SOQL via simple-salesforce.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_LEAD_SOQL = """
    SELECT
        Id, FirstName, LastName, Email, Phone, Company,
        Title, LeadSource, Status, Rating, Industry,
        NumberOfEmployees, AnnualRevenue, OwnerId,
        IsConverted, ConvertedDate, ConvertedContactId,
        ConvertedAccountId, ConvertedOpportunityId,
        CreatedDate, LastModifiedDate
    FROM Lead
    WHERE LastModifiedDate >= {start_dt}
      AND LastModifiedDate <= {end_dt}
    ORDER BY LastModifiedDate ASC
"""

_OPPORTUNITY_SOQL = """
    SELECT
        Id, Name, AccountId, OwnerId, StageName,
        Amount, Probability, CloseDate, LeadSource,
        Type, IsClosed, IsWon, ForecastCategory,
        CreatedDate, LastModifiedDate
    FROM Opportunity
    WHERE LastModifiedDate >= {start_dt}
      AND LastModifiedDate <= {end_dt}
    ORDER BY LastModifiedDate ASC
"""


class SalesforceExtractor(BaseExtractor):
    source_name = "salesforce"

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().salesforce

    def validate_config(self) -> None:
        cfg = self._cfg
        missing = [
            name
            for name, val in {
                "username": cfg.username,
                "password": cfg.password,
                "security_token": cfg.security_token,
            }.items()
            if not val
        ]
        if missing:
            raise ValueError(f"Missing Salesforce credentials: {missing}")

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        cfg = self._cfg
        try:
            sf = Salesforce(
                username=cfg.username,
                password=cfg.password,
                security_token=cfg.security_token,
                domain=cfg.domain,
            )
        except SalesforceAuthenticationFailed as exc:
            logger.error("Salesforce authentication failed: %s", exc)
            raise

        start_dt = f"{start_date.isoformat()}T00:00:00Z"
        end_dt = f"{end_date.isoformat()}T23:59:59Z"

        leads_df = self._query(sf, _LEAD_SOQL.format(start_dt=start_dt, end_dt=end_dt))
        leads_df["object_type"] = "lead"

        opps_df = self._query(
            sf, _OPPORTUNITY_SOQL.format(start_dt=start_dt, end_dt=end_dt)
        )
        opps_df["object_type"] = "opportunity"

        return pd.concat([leads_df, opps_df], ignore_index=True, sort=False)

    @staticmethod
    def _query(sf: Salesforce, soql: str) -> pd.DataFrame:
        result = sf.query_all(soql)
        records = [
            {k: v for k, v in r.items() if k != "attributes"}
            for r in result.get("records", [])
        ]
        df = pd.DataFrame(records)
        for col in ("CreatedDate", "LastModifiedDate", "CloseDate", "ConvertedDate"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        return df
