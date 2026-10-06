from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum as spark_sum, avg, count


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("CustomerAnalytics")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def load_customers(spark, file_path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )


def build_customer_analytics(df):
    return (
        df.groupBy("city")
        .agg(
            count("*").alias("customer_count"),
            spark_sum("total_spend").alias("total_spend"),
            avg("total_spend").alias("average_spend")
        )
        .withColumn(
            "average_spend",
            round(col("average_spend"), 2)
        )
        .orderBy(
            col("total_spend").desc()
        )
    )


def main():
    spark = create_spark_session()

    customers = load_customers(
        spark,
        "data/customers.csv"
    )

    customers.createOrReplaceTempView("customers")

    print("\n===== CUSTOMER ANALYTICS REPORT =====")

    report = spark.sql(
        """
        SELECT
            city,
            COUNT(*) AS customer_count,
            ROUND(SUM(total_spend), 2) AS total_spend,
            ROUND(AVG(total_spend), 2) AS average_spend
        FROM customers
        GROUP BY city
        ORDER BY total_spend DESC
        """
    )

    report.show()

    spark.stop()


if __name__ == "__main__":
    main()
