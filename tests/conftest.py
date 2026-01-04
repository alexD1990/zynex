# tests/conftest.py
from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped local SparkSession for unit tests.

    Keeps it minimal and deterministic. Avoids Hive support and uses a single local worker.
    """
    # Ensure Spark can run in CI/dev boxes without noisy UI/log behavior
    os.environ.setdefault("PYSPARK_PYTHON", "python3")

    spark = (
        SparkSession.builder
        .appName("dcheck-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark

    spark.stop()
