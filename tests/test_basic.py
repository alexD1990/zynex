from DCheck.api import validate_spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("DCheckTest").getOrCreate()

def test_validate_spark():
    df = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["col1", "col2"])
    report = validate_spark(df)
    assert report.summary()["rules_run"] > 0 
