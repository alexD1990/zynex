from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult


class SmallFileRule(Rule):
    name = "small_files"

    def __init__(self, table_name=None, min_avg_mb=64):
        self.table_name = table_name
        self.min_avg_mb = min_avg_mb

    def apply(self, df):
        if not self.table_name:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={
                    "num_files": 0.0,
                    "avg_file_size_mb": 0.0,
                },
                message="Small file check skipped (no table name provided)",
            )

        detail = df.sparkSession.sql(
            f"DESCRIBE DETAIL {self.table_name}"
        ).collect()[0]

        num_files = detail["numFiles"]
        total_bytes = detail["sizeInBytes"]

        if num_files == 0:
            avg_file_size_mb = 0.0
        else:
            avg_file_size_mb = (total_bytes / num_files) / (1024 * 1024)

        if avg_file_size_mb < 16:
            status = "warning"
            message = "Severe small file problem detected"
        elif avg_file_size_mb < self.min_avg_mb:
            status = "warning"
            message = "Small file problem detected"
        else:
            status = "ok"
            message = "No small file problem detected"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "num_files": float(num_files),
                "avg_file_size_mb": float(avg_file_size_mb),
            },
            message=message,
        )

class IqrOutlierRule(Rule):
    name = "iqr_outliers"

    def apply(self, df):
        total_outliers = 0
        numeric_columns = [col for col, dtype in df.dtypes if dtype in ('int', 'double')]

        for col in numeric_columns:
            q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
            iqr = q3 - q1
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr

            outliers = df.filter((df[col] < lower) | (df[col] > upper)).count()
            total_outliers += outliers

        outlier_ratio = total_outliers / df.count()
        status = "warning" if total_outliers > 0 else "ok"
        message = "IQR outliers detected" if status == "warning" else "No IQR outliers detected"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={"total_outliers": float(total_outliers), "outlier_ratio": outlier_ratio},
            message=message,
        )
