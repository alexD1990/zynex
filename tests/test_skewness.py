from zynex.rules.skewness import SkewnessRule
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("zynex-test").getOrCreate()

def test_skewness_flags_extreme_value():
    # 100 normale verdier rundt 10, og én ekstrem verdi (1000)
    # Dette gir et snitt på ca 20 og stddev på ca 100.
    # 1000 er da ca 10 stddev unna snittet.
    data = [(10.0,) for _ in range(100)] + [(1000.0,)]
    df = spark.createDataFrame(data, ["col1"])

    # Terskel på 5 stddev (default)
    rule = SkewnessRule(threshold_stddev=5.0)
    result = rule.apply(df)

    assert result.status == "warning"
    assert result.name == "extreme_values"
    assert "col1" in result.metrics["flagged_columns"]
    
    # Sjekk at vi fikk med detaljene
    metrics = result.metrics["flagged_columns"]["col1"]
    assert metrics["max"] == 1000.0
    assert metrics["max_sigma"] > 5.0

def test_skewness_is_ok_on_normal_distribution():
    # Bare verdier mellom 10 og 20
    data = [(float(i),) for i in range(10, 21)]
    df = spark.createDataFrame(data, ["col1"])

    rule = SkewnessRule(threshold_stddev=5.0)
    result = rule.apply(df)

    assert result.status == "ok"
    assert "No extreme values detected" in result.message