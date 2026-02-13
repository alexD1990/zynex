from __future__ import annotations

from typing import Optional, Dict, Any

from zynex.rules.base import Rule
from zynex.core.report import RuleResult


def _quote_ident(part: str) -> str:
    # Spark SQL identifier quoting. Backticks inside identifiers are escaped as double backticks.
    return f"`{part.replace('`', '``')}`"


def _quote_table_name(name: str) -> str:
    """
    Supports Databricks / Spark identifiers:
      - table
      - schema.table
      - catalog.schema.table (Unity Catalog)
    """
    parts = [p.strip() for p in name.split(".") if p.strip()]
    if not (1 <= len(parts) <= 3):
        raise ValueError(
            f"Unsupported table name '{name}'. Expected table, schema.table, or catalog.schema.table."
        )
    return ".".join(_quote_ident(p) for p in parts)


class SmallFileRule(Rule):
    """
    Analyzes Delta table metadata via 'DESCRIBE DETAIL' to identify file fragmentation.
    Flags inefficiencies based on file count relative to total dataset size.

    Note: This is a metadata-only check and does not scan actual data rows.
    """
    name = "small_files"

    def __init__(self, table_name: str | None = None):
        self.table_name = table_name

    def apply(self, df, context: Optional[Dict[str, Any]] = None) -> RuleResult:
        # 1) Skip if no table context is provided
        if not self.table_name:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={
                    "rating": "not_applicable",
                    "reason": "no_table_name",
                    "target": "<not provided>",
                    "recommendation": "Provide table_name to enable metadata-based file checks.",
                },
                message="No table_name provided. Cannot run file density analysis.",
            )

        # 2) Build a safe, Databricks-friendly identifier (Unity Catalog compatible)
        try:
            quoted = _quote_table_name(self.table_name)
        except Exception as e:
            return RuleResult(
                name=self.name,
                status="error",
                metrics={
                    "rating": "unknown",
                    "reason": "invalid_table_name",
                    "target": self.table_name,
                    "recommendation": "Use table, schema.table, or catalog.schema.table (Unity Catalog).",
                },
                message=f"Invalid table name: {type(e).__name__}: {e}",
            )

        # 3) Fetch metadata
        try:
            detail = df.sparkSession.sql(f"DESCRIBE DETAIL {quoted}").collect()[0]
        except Exception as e:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={
                    "rating": "not_applicable",
                    "reason": "metadata_fetch_failed",
                    "target": self.table_name,
                    "recommendation": "Verify table exists and is Delta (try: DESCRIBE DETAIL <table>).",
                },
                message=(
                    f"Metadata fetch failed for '{self.table_name}' ({type(e).__name__}). "
                    "Cannot run file density analysis."
                ),
            )

        # 4) Calculate metrics
        num_files = int(detail["numFiles"])
        total_bytes = int(detail["sizeInBytes"])

        total_size_gb = (float(total_bytes) / (1024 ** 3)) if total_bytes else 0.0
        total_size_mb = (float(total_bytes) / (1024 ** 2)) if total_bytes else 0.0

        avg_file_size_mb = 0.0
        if num_files > 0:
            avg_file_size_mb = (float(total_bytes) / float(num_files)) / (1024 ** 2)

        # Metric: Files per GB (High density indicates fragmentation)
        eps = 1e-9
        files_per_gb = (float(num_files) / max(total_size_gb, eps)) if num_files else 0.0

        # 5) Evaluate health (heuristics)
        # Small datasets (<256MB) are excluded from strict density checks.
        if total_size_mb < 256:
            rating = "not_applicable"
            status = "ok"
            message = "Dataset is too small for file density analysis."
            recommendation = "No action required."
            files_per_gb = 0.0  # Reset for clarity in reports

        elif num_files <= 100:
            rating = "optimal"
            status = "ok"
            message = "File density is optimal."
            recommendation = "No action required."

        elif files_per_gb > 2000 and num_files > 500:
            rating = "high_risk"
            status = "warning"
            message = "Critical fragmentation detected (High file-to-size ratio)."
            recommendation = "Action: Run OPTIMIZE to reduce query planning overhead."

        elif files_per_gb > 200 or num_files > 1000 or avg_file_size_mb < 16:
            rating = "suboptimal"
            status = "warning"
            message = "File density is suboptimal; average file size is low."
            recommendation = "Action: Monitor query performance. Consider OPTIMIZE if latency increases."

        else:
            rating = "optimal"
            status = "ok"
            message = "File layout is healthy."
            recommendation = "No action required."

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "num_files": float(num_files),
                "total_size_gb": float(total_size_gb),
                "avg_file_size_mb": float(avg_file_size_mb),
                "files_per_gb": float(files_per_gb),
                "rating": rating,
                "recommendation": recommendation,
            },
            message=message,
        )