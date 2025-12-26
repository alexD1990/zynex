from __future__ import annotations

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult


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
        # Identify numeric columns
        numeric_cols = [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float")]

        if not numeric_cols:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={},
                message="No numeric columns to check.",
            )

        # Optimization: Construct a single large aggregation expression.
        # This avoids launching separate Spark jobs per column, which is significantly faster 
        # than looping or using approxQuantile.
        exprs = []
        for c in numeric_cols:
            exprs.extend([
                F.min(c).alias(f"{c}_min"),
                F.max(c).alias(f"{c}_max"),
                F.avg(c).alias(f"{c}_avg"),
                F.stddev(c).alias(f"{c}_std")
            ])

        # Execute 1 job for the entire table
        row = df.agg(*exprs).collect()[0].asDict()

        flagged_columns = {}
        total_extreme_cols = 0

        for c in numeric_cols:
            c_min = row[f"{c}_min"]
            c_max = row[f"{c}_max"]
            c_avg = row[f"{c}_avg"] or 0
            c_std = row[f"{c}_std"] or 0

            # Avoid division by zero (e.g., constant values where stddev is 0)
            if c_std == 0:
                continue

            # Calculate Z-Scores (Distance from mean)
            z_max = (c_max - c_avg) / c_std
            z_min = (c_avg - c_min) / c_std

            if z_max > self.threshold or z_min > self.threshold:
                flagged_columns[c] = {
                    "min": float(c_min),
                    "max": float(c_max),
                    "avg": float(c_avg),
                    "std": float(c_std),
                    "max_sigma": float(round(max(z_max, z_min), 1))  # Magnitude of deviation
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
            metrics={"flagged_columns": flagged_columns},
            message=message,
        )