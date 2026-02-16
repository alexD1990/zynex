from typing import Union, Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
import difflib

from zynex.orchestrator.engine import run_orchestrator
from zynex.orchestrator.adapters import report_to_validation_report
from zynex.core.report import render_report, ValidationReport

def _suggest_table_names(spark, schema: str, wanted: str, limit: int = 5) -> list[str]:
    try:
        rows = spark.sql(f"SHOW TABLES IN {schema}").select("tableName").collect()
        names = [r["tableName"] for r in rows]
        return difflib.get_close_matches(wanted, names, n=limit, cutoff=0.5)
    except Exception:
        return []

def check(
    source: Union[str, DataFrame],
    table_name: Optional[str] = None,
    render: bool = True,
    cache: bool = False,
    modules: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
):
    """
    Primary entry point for zynex validation.

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

        spark = SparkSession.getActiveSession()
        if not spark:
            print("Error: No active SparkSession found.")
            return None

        print(f"Loading table '{real_table_name}'...")

        try:
            df = spark.table(real_table_name)

        except Exception as e:
            msg = str(e)

            # Common Spark error: TABLE_OR_VIEW_NOT_FOUND / cannot be found
            if ("TABLE_OR_VIEW_NOT_FOUND" in msg) or ("cannot be found" in msg and "table or view" in msg.lower()):
                print(f"Error: Table not found: '{real_table_name}'")

                parts = [p.strip() for p in real_table_name.split(".") if p.strip()]
                if len(parts) == 2:
                    schema, wanted = parts[0], parts[1]
                    suggestions = _suggest_table_names(spark, schema, wanted)
                    if suggestions:
                        print("Did you mean:")
                        for s in suggestions:
                            print(f"  - {schema}.{s}")
                    print(f"Hint: try: SHOW TABLES IN {schema}")
                else:
                    print("Hint: try: SHOW TABLES (or qualify name as schema.table)")

                return None

            # Any other load error
            print(f"Error: Could not load table '{real_table_name}': {type(e).__name__}: {e}")
            return None

    elif isinstance(source, DataFrame):
        df = source
        real_table_name = table_name

    else:
        raise ValueError("Input must be a Spark DataFrame or a table name string.")

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
            __import__("zynex.core.report", fromlist=["RuleResult"]).RuleResult(
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
zx = check
dc = check
