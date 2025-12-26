# tests/test_basic.py
import io
import contextlib
import pytest

from pyspark.sql import SparkSession
from DCheck.api import check


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("DCheckTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_check_runs_and_returns_report_when_render_false(spark):
    df = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["col1", "col2"])

    report = check(df, render=False)  # IMPORTANT: render=False => returns ValidationReport

    s = report.summary()
    assert s["rows"] == 3
    assert s["columns"] == 2
    assert s["rules_run"] >= 1
    assert isinstance(report.results, list)


def test_check_df_has_no_print_side_effects_when_render_false(spark):
    df = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _ = check(df, render=False)

    # Should not print anything when validating a dataframe programmatically
    assert buf.getvalue() == ""


def test_status_semantics_warning_when_issues_found(spark):
    # Duplicate rows: identical rows
    df_dupes = spark.createDataFrame([(1, 2), (1, 2), (3, 4)], ["a", "b"])
    report_dupes = check(df_dupes, render=False)
    dup = next(r for r in report_dupes.results if r.name == "duplicate_rows")
    assert dup.status.lower() == "warning"

    # Nulls: include nulls in a column
    df_nulls = spark.createDataFrame([(1, None), (2, 2), (3, None)], ["a", "b"])
    report_nulls = check(df_nulls, render=False)
    nullr = next(r for r in report_nulls.results if r.name == "null_ratio")
    assert nullr.status.lower() == "warning"

    # Extreme values (skewness/stddev rule)
    data = [(10.0,) for _ in range(50)] + [(1000.0,)]
    df_skew = spark.createDataFrame(data, ["x"])

    report_skew = check(df_skew, render=False)
    skew_rule = next(r for r in report_skew.results if r.name == "extreme_values")
    assert skew_rule.status.lower() == "warning"
    assert isinstance(skew_rule.metrics, dict)
