from __future__ import annotations

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from zynex.rules.base import Rule
from zynex.core.report import RuleResult


class SkewnessRule(Rule):
    """
    Fast statistical analysis of numeric distributions.

    Computes min, max, avg, and stddev for all numeric columns in a single Spark job.
    Flags columns where values deviate significantly from the mean (Outlier detection via Z-Score).
    """
    name = "extreme_values"

    def __init__(self, threshold_stddev: float = 5.0):
        self.threshold = threshold_stddev

    def apply(self, df: DataFrame, context: Optional[Dict[str, Any]] = None) -> RuleResult:
        # Identify numeric columns using Spark's type system (supports all numeric types, incl. decimal)
        numeric_cols = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, NumericType)
        ]

        if not numeric_cols:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={},
                message="No numeric columns to check.",
            )

        # Optimization: Construct a single large aggregation expression.
        exprs = []
        for c in numeric_cols:
            exprs.extend([
                F.min(c).alias(f"{c}_min"),
                F.max(c).alias(f"{c}_max"),
                F.avg(c).alias(f"{c}_avg"),
                F.stddev(c).alias(f"{c}_std"),
            ])

        # Execute 1 job for the entire table
        row = df.agg(*exprs).collect()[0].asDict()

        flagged_columns = {}
        total_extreme_cols = 0

        for c in numeric_cols:
            c_min = row.get(f"{c}_min")
            c_max = row.get(f"{c}_max")

            c_avg = row.get(f"{c}_avg")
            c_std = row.get(f"{c}_std")

            # Skip columns with null stats (all-null) or constant values
            if c_min is None or c_max is None or c_std in (None, 0):
                continue

            # Coerce to float for stable math across DecimalType / numeric mixes
            try:
                f_min = float(c_min)
                f_max = float(c_max)
                f_avg = float(c_avg) if c_avg is not None else 0.0
                f_std = float(c_std)
            except Exception:
                # If anything is non-numeric, skip the column rather than crashing the module
                continue

            if f_std == 0.0:
                continue

            # Calculate Z-Scores (Distance from mean)
            z_max = (f_max - f_avg) / f_std
            z_min = (f_avg - f_min) / f_std

            if z_max > self.threshold or z_min > self.threshold:
                # Cast to float for JSON-friendly metrics when possible
                def _f(x):
                    try:
                        return float(x)
                    except Exception:
                        return x

                flagged_columns[c] = {
                    "min": _f(c_min),
                    "max": _f(c_max),
                    "avg": _f(c_avg),
                    "std": _f(c_std),
                    "max_sigma": float(round(max(z_max, z_min), 1)),
                }
                total_extreme_cols += 1

        status = "warning" if total_extreme_cols > 0 else "ok"
        message = (
            f"Extreme values detected in {total_extreme_cols} columns (>{self.threshold} stddev)"
            if total_extreme_cols > 0
            else "No extreme values detected"
        )

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "flagged_columns": flagged_columns,
                "threshold_stddev": float(self.threshold),
            },
            message=message,
        )