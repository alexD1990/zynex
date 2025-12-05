from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult

class SmallFileRule(Rule):
    name = "small_files"

    def apply(self, df):
        input_files = df.inputFiles()
        num_files = len(input_files)

        if num_files == 0:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={"num_files": 0.0, "avg_file_size_mb": 0.0},
                message="No input files detected"
            )

        total_bytes = sum([df.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(df.sparkSession._jsc.hadoopConfiguration())
                           .getFileStatus(df.sparkSession._jvm.org.apache.hadoop.fs.Path(f)).getLen() for f in input_files])
        avg_file_size_mb = (total_bytes / num_files) / (1024 * 1024)

        status = "warning" if avg_file_size_mb < 64 else "ok"
        message = "Small file problem detected" if status == "warning" else "No small file problem detected"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={"num_files": float(num_files), "avg_file_size_mb": float(avg_file_size_mb)},
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
