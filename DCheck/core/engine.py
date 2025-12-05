from DCheck.rules.structural import DuplicateRowRule
from DCheck.rules.quality import NullRatioRule
from DCheck.rules.performance import SmallFileRule, IqrOutlierRule
from DCheck.core.report import ValidationReport

RULES = [
    DuplicateRowRule(),
    NullRatioRule(),
    IqrOutlierRule(),
    SmallFileRule(),
]

def run_engine(df):
    report = ValidationReport(
        rows=df.count(),
        columns=len(df.columns),
        column_names=df.columns,
    )

    for rule in RULES:
        result = rule.apply(df)
        report.results.append(result)

    return report
