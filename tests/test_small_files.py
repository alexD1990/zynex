from DCheck.rules.performance import SmallFileRule
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("DCheckTest").getOrCreate()

def test_small_files_skips_without_table_name():
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    rule = SmallFileRule(table_name=None)
    res = rule.apply(df, context={"rows": 2, "table_name": None})
    
    assert res.status.lower() == "ok"
    assert res.name == "small_files"