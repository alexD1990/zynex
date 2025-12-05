from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult
from pyspark.sql import functions as F

class NullRatioRule(Rule):
    name = "null_ratio"

    def apply(self, df):
        rows = df.count()
        total_nulls = sum(
            df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).first().asDict().values()
        )
        null_ratio = total_nulls / (rows * len(df.columns)) if rows > 0 else 0.0

        status = "warning" if null_ratio > 0.1 else "ok"
        message = "High null ratio detected" if status == "warning" else "Null ratio within acceptable range"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "rows": float(rows),
                "total_nulls": float(total_nulls),
                "null_ratio": float(null_ratio),
            },
            message=message,
        )
