from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "src")
)

from spark_sql_basics import (
    load_customer_data,
    prepare_customer_data
)


def create_test_spark():
    spark = (
        SparkSession.builder
        .appName("SparkSQLTests")
        .master("local[2]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def test_customer_dataframe_count():
    spark = create_test_spark()

    df = load_customer_data(
        spark,
        "data/customers.csv"
    )

    assert df.count() == 10

    spark.stop()


def test_premium_customer_count():
    spark = create_test_spark()

    df = load_customer_data(
        spark,
        "data/customers.csv"
    )

    premium = df.filter(
        col("segment") == "Premium"
    )

    assert premium.count() == 5

    spark.stop()


def test_with_column_spend_category():
    spark = create_test_spark()

    df = load_customer_data(
        spark,
        "data/customers.csv"
    )

    enriched = prepare_customer_data(df)

    assert "spend_category" in enriched.columns

    categories = (
        enriched
        .select("spend_category")
        .distinct()
        .collect()
    )

    category_values = {
        row["spend_category"]
        for row in categories
    }

    assert category_values == {
        "High",
        "Medium",
        "Low"
    }

    spark.stop()


def test_sql_temporary_view():
    spark = create_test_spark()

    df = load_customer_data(
        spark,
        "data/customers.csv"
    )

    df.createOrReplaceTempView("customers")

    result = spark.sql(
        """
        SELECT COUNT(*) AS total_customers
        FROM customers
        """
    )

    assert result.first()["total_customers"] == 10

    spark.stop()
