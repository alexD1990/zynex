from zynex.rules.base import Rule
from zynex.core.report import RuleResult
from pyspark.sql import functions as F

class NullRatioRule(Rule):
    name = "null_ratio"

    def apply(self, df, context=None):
        # Single pass aggregation: one Spark job, not one per column
        if not df.columns:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={"total_nulls": 0, "per_column": {}},
                message="No columns to check",
            )

        agg_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]
        row = df.agg(*agg_exprs).collect()[0].asDict()

        per_column = {c: {"nulls": int(v)} for c, v in row.items() if v and int(v) > 0}
        total_nulls = int(sum(int(v) for v in row.values() if v is not None))

        metrics = {
            "total_nulls": total_nulls,
            "per_column": per_column,
        }

        status = "warning" if total_nulls > 0 else "ok"
        message = "Null values detected" if total_nulls > 0 else "No null values detected"

        return RuleResult(
            name=self.name,
            status=status,
            metrics=metrics,
            message=message,
        )

