from typing import Union, Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from dcheck.orchestrator.engine import run_orchestrator
from dcheck.orchestrator.adapters import report_to_validation_report
from dcheck.core.report import render_report, ValidationReport


def check(
    source: Union[str, DataFrame],
    table_name: Optional[str] = None,
    render: bool = True,
    cache: bool = False,
    modules: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
):
    """
    Primary entry point for dcheck validation.

    Usage:
      1. check("catalog.schema.table") -> Auto-loads table + runs preflight if table_name is available.
      2. check(df) -> Validates DataFrame.
      3. check(df, table_name="...") -> Validates DataFrame + runs preflight.

    Modules:
      - Default: ["core_quality"]
      - Enterprise: e.g. ["core_quality", "gdpr"] (requires plugin installed)
    """

    df: Optional[DataFrame] = None
    real_table_name: Optional[str] = None

    # 1) Input Resolution
    if isinstance(source, str):
        real_table_name = source
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                print("Error: No active SparkSession found.")
                return None

            print(f"Loading table '{real_table_name}'...")
            df = spark.table(real_table_name)

        except Exception as e:
            print(f"Error: Could not load table '{real_table_name}': {e}")
            return None

    elif isinstance(source, DataFrame):
        df = source
        real_table_name = table_name

    else:
        raise ValueError("Input must be a Spark DataFrame or a table name string.")

    # Track whether we already rendered the pre-flight small_files box
    preflight_ran = False

    # 2) Define Callback (Immediate UI Feedback)
    def _on_preflight(result):
        nonlocal preflight_ran
        preflight_ran = True

        if not render:
            return

        print("\nRunning pre-flight check...")

        # Convert single CheckResult -> old ValidationReport for rendering
        mini_report = ValidationReport(
            rows=1,
            columns=max(1, len(df.columns)),
            column_names=df.columns,
            results=[],
        )

        # Adapt result shape to RuleResult expected by renderer
        # "core_quality.small_files" -> "small_files"
        name = result.check_id.split(".", 1)[1] if "." in result.check_id else result.check_id
        mini_report.results.append(
            __import__("dcheck.core.report", fromlist=["RuleResult"]).RuleResult(
                name=name,
                status=result.status,
                metrics=result.metrics or {},
                message=result.message,
            )
        )

        # print_header=False suppresses the dataset summary for pre-flight
        render_report(mini_report, verbose=True, print_header=False)

        print("Proceeding with full data scan...")

    # 3) Prepare module selection + config
    selected_modules = modules or ["core_quality"]

    merged_config: Dict[str, Any] = dict(config or {})
    # Keep existing explicit param behavior: cache argument wins
    merged_config["cache"] = cache

    # 4) Execute Orchestrator
    new_report = run_orchestrator(
        df,
        table_name=real_table_name,
        modules=selected_modules,
        config=merged_config,
        on_preflight_done=_on_preflight,
    )

    # 5) Render Final Report (keep existing renderer)
    old_report = report_to_validation_report(new_report)

    if render:
        # small_files should only be shown in pre-flight
        if preflight_ran:
            old_report.results = [r for r in old_report.results if r.name != "small_files"]

        render_report(old_report)
        return None

    return old_report


# Short ergonomic alias for notebooks
dc = check
