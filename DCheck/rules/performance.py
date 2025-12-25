from __future__ import annotations

import re
from typing import Dict, Tuple

from pyspark.sql import functions as F

from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult


class SmallFileRule(Rule):
    """
    Checks Delta table file layout using DESCRIBE DETAIL and flags potentially inefficient
    file density (many files relative to total table size).

    This rule is advisory only. It does not stop execution or run optimization commands.
    """
    name = "small_files"

    def __init__(self, table_name: str | None = None):
        self.table_name = table_name

    def apply(self, df, context=None) -> RuleResult:
        if not self.table_name:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={
                    "num_files": 0.0,
                    "total_size_gb": 0.0,
                    "avg_file_size_mb": 0.0,
                    "files_per_gb": 0.0,
                    "rating": "optimal",
                    "recommendation": "Skipped (no table_name provided).",
                },
                message="Small file check skipped (no table_name provided).",
            )

        # Accept formats:
        #   schema.table
        #   catalog.schema.table
        # This is a minimal guardrail to avoid unexpected SQL inputs.
        if not re.fullmatch(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){1,2}", self.table_name):
            return RuleResult(
                name=self.name,
                status="warning",
                metrics={
                    "num_files": 0.0,
                    "total_size_gb": 0.0,
                    "avg_file_size_mb": 0.0,
                    "files_per_gb": 0.0,
                    "rating": "invalid_input",
                    "recommendation": "Provide table_name as schema.table or catalog.schema.table.",
                },
                message="Invalid table_name format for DESCRIBE DETAIL.",
            )
        try:
            detail = df.sparkSession.sql(f"DESCRIBE DETAIL {self.table_name}").collect()[0]
        except Exception as e:
            return RuleResult(
                name=self.name,
                status="error",
                metrics={
                    "num_files": 0.0,
                    "total_size_gb": 0.0,
                    "avg_file_size_mb": 0.0,
                    "files_per_gb": 0.0,
                    "rating": "unknown",
                    "recommendation": "Verify the table exists, is a Delta table, and that you have permissions.",
                },
                message=f"Small file check failed when reading table metadata: {type(e).__name__}",
            )

        num_files = int(detail["numFiles"])
        total_bytes = int(detail["sizeInBytes"])

        total_size_gb = (float(total_bytes) / (1024 ** 3)) if total_bytes else 0.0
        total_size_mb = (float(total_bytes) / (1024 ** 2)) if total_bytes else 0.0
        avg_file_size_mb = (
            (float(total_bytes) / float(num_files) / (1024 ** 2)) if num_files else 0.0
        )

        # File density is the primary signal: files per GB of data.
        # Use a small epsilon to avoid division by zero for tiny tables.
        eps = 1e-9
        files_per_gb = (float(num_files) / max(total_size_gb, eps)) if num_files else 0.0

        # Heuristic rating (no config yet). We explicitly guard small tables:
        # tiny datasets are rarely operationally costly even if average file size is small.
        if total_size_mb < 256 or num_files <= 100:
            rating = "optimal"
        elif files_per_gb > 2000 and num_files > 500:
            rating = "high_risk"
        elif files_per_gb > 200 or num_files > 1000 or avg_file_size_mb < 16:
            rating = "suboptimal"
        else:
            rating = "optimal"

        # Advisory mapping: DCheck does not enforce execution control.
        status = "warning" if rating in ("suboptimal", "high_risk") else "ok"

        if rating == "high_risk":
            message = (
                "Small file density is high relative to dataset size. "
                "This commonly increases planning overhead and slows full scans."
            )
            recommendation = (
                "Consider compaction or OPTIMIZE to reduce the number of files "
                "before running heavy analytical workloads."
            )
        elif rating == "suboptimal":
            message = (
                "Small file density is higher than recommended. "
                "This may reduce performance and compute efficiency."
            )
            recommendation = (
                "Consider compaction or OPTIMIZE if you observe slow reads or excessive task counts."
            )
        else:
            message = "Small file density is within a healthy range."
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


class IqrOutlierRule(Rule):
    """
    Detects outliers in numeric columns using IQR bounds.

    Notes:
    - Uses approxQuantile per column to estimate Q1/Q3 (can be costly on very wide tables).
    - Counts outliers for all numeric columns in a single aggregation pass.
    """
    name = "iqr_outliers"

    def apply(self, df, context=None) -> RuleResult:
        numeric_cols = [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float")]

        if not numeric_cols:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={"total_outliers": 0, "per_column": {}},
                message="No numeric columns to check.",
            )

        bounds: Dict[str, Tuple[float, float]] = {}

        for c in numeric_cols:
            q = df.approxQuantile(c, [0.25, 0.75], 0.01)
            if not q or len(q) != 2:
                continue
            q1, q3 = q
            if q1 is None or q3 is None:
                continue

            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            bounds[c] = (lower, upper)

        if not bounds:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={"total_outliers": 0, "per_column": {}},
                message="Could not compute IQR bounds for numeric columns.",
            )

        agg_exprs = []
        for c, (lower, upper) in bounds.items():
            agg_exprs.append(
                F.sum(
                    F.when(
                        (F.col(c) < F.lit(lower)) | (F.col(c) > F.lit(upper)),
                        1,
                    ).otherwise(0)
                ).alias(c)
            )

        row = df.agg(*agg_exprs).collect()[0].asDict()
        per_column = {c: {"outliers": int(v)} for c, v in row.items() if v and int(v) > 0}
        total_outliers = int(sum(int(v) for v in row.values() if v is not None))

        status = "warning" if total_outliers > 0 else "ok"
        message = "Outlier values detected (IQR)." if total_outliers > 0 else "No outliers detected (IQR)."

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "total_outliers": total_outliers,
                "per_column": per_column,
            },
            message=message,
        )
