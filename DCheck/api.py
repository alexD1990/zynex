# DCheck/api.py
from typing import Union, Optional
from pyspark.sql import DataFrame, SparkSession

from DCheck.core.engine import run_engine
from DCheck.core.report import render_report, ValidationReport


def check(
    source: Union[str, DataFrame],
    table_name: Optional[str] = None,
    render: bool = True,
):
    """
    Primary entry point for DCheck validation.

    Usage:
      1. check("catalog.schema.table") -> Auto-loads table + runs metadata check.
      2. check(df) -> Validates DataFrame (skips metadata check).
      3. check(df, table_name="...") -> Validates DataFrame + runs metadata check.
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

        mini_report = ValidationReport(
            rows=1,
            columns=max(1, len(df.columns)),
            column_names=df.columns,
            results=[result],
        )

        # NOTE: print_header=False suppresses the dataset summary for pre-flight
        render_report(mini_report, verbose=True, print_header=False)

        print("Proceeding with full data scan...")

    # 3) Execute Engine
    report = run_engine(
        df,
        table_name=real_table_name,
        on_preflight_done=_on_preflight,
    )

    # 4) Render Final Report
    if render:
        # small_files should only be shown in pre-flight
        if preflight_ran:
            report.results = [r for r in report.results if r.name != "small_files"]

        render_report(report)

        # Avoid Databricks/Jupyter printing the object's repr at the end of the cell
        return None

    return report
