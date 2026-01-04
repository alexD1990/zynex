from pyspark.sql import functions as F

def test_modules_gdpr_only_preserves_dataset_rows_and_cols(spark):
    from dcheck.api import dc

    df = spark.range(0, 10).withColumn("email", F.lit("test@example.com"))

    rep = dc(df, modules=["gdpr"], render=False)

    assert rep.rows == 10
    assert rep.columns == len(df.columns)
