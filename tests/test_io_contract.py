import json
from dataclasses import asdict

import pytest
from pyspark.sql import SparkSession

from zynex import zx 


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("zynex-io-contract-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(
        [(1, 10.0), (2, 20.0), (3, 30.0)],
        ["id", "value"],
    )


def test_render_false_returns_report(sample_df):
    report = zx(sample_df, render=False)

    from zynex.core.report import ValidationReport
    assert isinstance(report, ValidationReport)


def test_render_true_returns_none(sample_df, capsys):
    result = zx(sample_df, render=True)
    _ = capsys.readouterr()  
    assert result is None


def test_validation_report_structure(sample_df):
    report = zx(sample_df, render=False)

    assert hasattr(report, "rows")
    assert hasattr(report, "columns")
    assert hasattr(report, "column_names")
    assert hasattr(report, "results")

    assert isinstance(report.rows, int)
    assert isinstance(report.columns, int)
    assert isinstance(report.column_names, list)
    assert isinstance(report.results, list)

    assert len(report.results) > 0  

    for rule in report.results:
        assert hasattr(rule, "name")
        assert hasattr(rule, "status")
        assert hasattr(rule, "metrics")
        assert hasattr(rule, "message")

        assert isinstance(rule.name, str)
        assert isinstance(rule.status, str)
        assert isinstance(rule.metrics, dict)
        assert isinstance(rule.message, str)


ALLOWED_STATUS = {"ok", "warning", "error", "skipped", "not_applicable"}


def test_status_domain(sample_df):
    report = zx(sample_df, render=False)
    assert len(report.results) > 0

    for rule in report.results:
        assert rule.status in ALLOWED_STATUS


def test_report_is_json_serializable(sample_df):
    report = zx(sample_df, render=False)
    report_dict = asdict(report)
    json.dumps(report_dict)  


def test_report_shape_stable(sample_df):
    report = zx(sample_df, render=False)
    report_dict = asdict(report)

    expected_keys = {"rows", "columns", "column_names", "results"}
    assert set(report_dict.keys()) == expected_keys