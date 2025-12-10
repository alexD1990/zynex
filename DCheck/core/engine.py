from DCheck.rules.structural import DuplicateRowRule
from DCheck.rules.quality import NullRatioRule
from DCheck.rules.performance import SmallFileRule, IqrOutlierRule
from DCheck.core.report import ValidationReport


def run_engine(df, table_name=None):
    report = ValidationReport(
        rows=df.count(),
        columns=len(df.columns),
        column_names=df.columns,
    )

    # =========================================
    # PRE-FLIGHT: PERFORMANCE 
    # =========================================
    preflight_rules = [
        SmallFileRule(table_name=table_name),
    ]

    for rule in preflight_rules:
        result = rule.apply(df)
        report.results.append(result)

    # Instant feedback in notebook 
    try:
        from DCheck.core.report import render_report
        print("\n[PREFLIGHT CHECK COMPLETED]\n")
        render_report(
            ValidationReport(
                rows=report.rows,
                columns=report.columns,
                column_names=report.column_names,
                results=report.results.copy()
            )
        )
    except Exception:
        pass

    # =========================================
    # CORE DATA CHECKS 
    # =========================================
    core_rules = [
        DuplicateRowRule(),
        NullRatioRule(),
        IqrOutlierRule(),
    ]

    for rule in core_rules:
        result = rule.apply(df)
        report.results.append(result)

    return report
