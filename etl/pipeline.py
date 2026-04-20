"""
etl/pipeline.py
Orchestrates the extract → transform → load flow for a single source.
Used by both Airflow DAGs and the standalone CLI runner.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from etl.extractors.base_extractor import BaseExtractor, ExtractionError
from etl.loaders.bigquery_loader import BigQueryLoader
from etl.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    source: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    success: bool = False
    error: Optional[str] = None


class Pipeline:
    """
    Generic ETL pipeline: Extract → (optional) Transform → Load.
    """

    def __init__(
        self,
        extractor: BaseExtractor,
        loader: BigQueryLoader,
        transformer: Optional[BaseTransformer] = None,
        table_name: str = "",
        dataset: str | None = None,
        write_mode: str = "append",
        merge_keys: list[str] | None = None,
        partition_field: str = "date",
    ) -> None:
        self.extractor = extractor
        self.loader = loader
        self.transformer = transformer
        self.table_name = table_name or extractor.source_name
        self.dataset = dataset
        self.write_mode = write_mode
        self.merge_keys = merge_keys
        self.partition_field = partition_field

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> PipelineResult:
        result = PipelineResult(source=self.extractor.source_name)
        try:
            # ── Extract ───────────────────────────────────────────────────────
            df: pd.DataFrame = self.extractor.run(start_date, end_date)
            result.rows_extracted = len(df)

            if df.empty:
                logger.warning("No data extracted from %s", self.extractor.source_name)
                result.success = True
                return result

            # ── Transform ─────────────────────────────────────────────────────
            if self.transformer:
                df = self.transformer.run(df)

            # ── Load ──────────────────────────────────────────────────────────
            self.loader.load(
                df,
                table_name=self.table_name,
                dataset=self.dataset,
                write_mode=self.write_mode,
                partition_field=self.partition_field,
                merge_keys=self.merge_keys,
            )
            result.rows_loaded = len(df)
            result.success = True

        except ExtractionError as exc:
            result.error = str(exc)
            logger.error("Extraction failed for %s: %s", self.extractor.source_name, exc)
        except Exception as exc:  # noqa: BLE001
            result.error = str(exc)
            logger.exception("Pipeline failed for %s", self.extractor.source_name)

        return result
