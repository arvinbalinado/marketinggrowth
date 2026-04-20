"""
etl/loaders/bigquery_loader.py
Loads transformed DataFrames into Google BigQuery with schema enforcement,
partitioning, and upsert (MERGE) support.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SchemaField, WriteDisposition

from config.settings import get_settings

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """
    Handles all writes to BigQuery.

    Supported write modes:
      - "append"   : INSERT rows (idempotent for event tables)
      - "overwrite": Truncate + replace (daily snapshots)
      - "merge"    : MERGE on a primary key (incremental updates)
    """

    def __init__(self) -> None:
        cfg = get_settings().bigquery
        self._project = cfg.project_id
        self._dataset_raw = cfg.dataset_raw
        self._dataset_staging = cfg.dataset_staging
        self._dataset_marts = cfg.dataset_marts
        self._client = bigquery.Client(project=self._project)

    # ── Public API ────────────────────────────────────────────────────────────

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset: str | None = None,
        write_mode: str = "append",
        partition_field: str | None = "date",
        merge_keys: list[str] | None = None,
        schema: list[SchemaField] | None = None,
    ) -> None:
        if df.empty:
            logger.warning("Skipping load — empty DataFrame for %s", table_name)
            return

        dataset = dataset or self._dataset_raw
        table_ref = f"{self._project}.{dataset}.{table_name}"

        logger.info(
            "Loading %d rows → %s (mode=%s)", len(df), table_ref, write_mode
        )

        df = self._prepare_dataframe(df)

        if write_mode == "merge" and merge_keys:
            self._merge_load(df, table_ref, merge_keys, partition_field)
        else:
            self._direct_load(df, table_ref, write_mode, partition_field, schema)

        logger.info("Load complete → %s", table_ref)

    def ensure_dataset(self, dataset_id: str) -> None:
        """Create dataset if it does not already exist."""
        dataset_ref = bigquery.Dataset(f"{self._project}.{dataset_id}")
        dataset_ref.location = get_settings().bigquery.region
        self._client.create_dataset(dataset_ref, exists_ok=True)
        logger.info("Dataset ready: %s", dataset_id)

    def run_query(self, sql: str) -> pd.DataFrame:
        """Execute arbitrary SQL and return results as a DataFrame."""
        return self._client.query(sql).to_dataframe()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _direct_load(
        self,
        df: pd.DataFrame,
        table_ref: str,
        write_mode: str,
        partition_field: str | None,
        schema: list[SchemaField] | None,
    ) -> None:
        disposition_map = {
            "append": WriteDisposition.WRITE_APPEND,
            "overwrite": WriteDisposition.WRITE_TRUNCATE,
        }
        job_config = LoadJobConfig(
            write_disposition=disposition_map.get(write_mode, WriteDisposition.WRITE_APPEND),
            autodetect=schema is None,
            schema=schema,
        )
        if partition_field and partition_field in df.columns:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )

        job = self._client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion; raises on error

    def _merge_load(
        self,
        df: pd.DataFrame,
        table_ref: str,
        merge_keys: list[str],
        partition_field: str | None,
    ) -> None:
        """
        Stage data into a temp table then MERGE into the target.
        Avoids full-table overwrites while maintaining idempotency.
        """
        temp_table = f"{table_ref}_tmp_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        self._direct_load(df, temp_table, "overwrite", partition_field, None)

        non_key_cols = [c for c in df.columns if c not in merge_keys]
        key_conditions = " AND ".join(
            [f"T.{k} = S.{k}" for k in merge_keys]
        )
        update_set = ", ".join([f"T.{c} = S.{c}" for c in non_key_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"S.{c}" for c in df.columns])

        merge_sql = f"""
        MERGE `{table_ref}` T
        USING `{temp_table}` S
        ON {key_conditions}
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals});
        DROP TABLE IF EXISTS `{temp_table}`;
        """
        self._client.query(merge_sql).result()

    @staticmethod
    def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Coerce types that BigQuery cannot infer safely."""
        df = df.copy()
        # Convert object columns holding Python dicts/lists to JSON strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).where(df[col].notna(), other=None)
        return df
