from DCheck.rules.structural import DuplicateRowRule
from DCheck.rules.quality import NullRatioRule
from DCheck.rules.performance import SmallFileRule, IqrOutlierRule
from DCheck.core.report import ValidationReport


def run_engine(df, table_name=None):
    rules = [
        DuplicateRowRule(),
        NullRatioRule(),
        IqrOutlierRule(),
        SmallFileRule(table_name=table_name),
    ]

    report = ValidationReport(
        rows=df.count(),
        columns=len(df.columns),
        column_names=df.columns,
    )

    for rule in rules:
        result = rule.apply(df)
        report.results.append(result)

    return report
