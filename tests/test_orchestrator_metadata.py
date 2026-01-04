import pytest
from pyspark.sql import functions as F

def test_gdpr_only_report_has_correct_rows_and_columns(spark):
    from dcheck.api import dc

    df = spark.range(0, 10).withColumn("email", F.lit("test0@example.com"))

    rep = dc(df, modules=["gdpr"], render=False)

    assert rep.rows == 10
    assert rep.columns == len(df.columns)