from zynex.rules.base import Rule
from zynex.core.report import RuleResult


class DuplicateRowRule(Rule):
    name = "duplicate_rows"

    def apply(self, df, context=None):
        ctx = context or {}

        # Use provided rowcount (may legitimately be 0); only fall back to df.count() if missing.
        if "rows" in ctx and ctx["rows"] is not None:
            total_rows = int(ctx["rows"])
        else:
            total_rows = int(df.count())

        unique_rows = df.dropDuplicates().count()
        duplicate_rows = total_rows - unique_rows

        status = "warning" if duplicate_rows > 0 else "ok"
        message = "Duplicate full rows detected" if duplicate_rows > 0 else "No duplicate full rows"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "total_rows": float(total_rows),
                "unique_rows": float(unique_rows),
                "duplicate_rows": float(duplicate_rows),
            },
            message=message,
        )