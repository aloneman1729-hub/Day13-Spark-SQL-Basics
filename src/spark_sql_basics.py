from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, avg, sum as spark_sum, count


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Day13-Spark-SQL-Basics")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def load_customer_data(spark, file_path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )


def prepare_customer_data(df):
    return (
        df
        .withColumn(
            "spend_category",
            when(col("total_spend") >= 70000, "High")
            .when(col("total_spend") >= 40000, "Medium")
            .otherwise("Low")
        )
        .withColumn(
            "average_order_value",
            round(col("total_spend") / col("orders"), 2)
        )
    )


def main():
    spark = create_spark_session()

    print("\n===== DAY 13 — SPARK SQL BASICS =====")

    # --------------------------------------------------
    # 1. Read CSV into DataFrame
    # --------------------------------------------------

    customers = load_customer_data(
        spark,
        "data/customers.csv"
    )

    print("\n===== SCHEMA =====")
    customers.printSchema()

    # --------------------------------------------------
    # 2. Show DataFrame
    # --------------------------------------------------

    print("\n===== CUSTOMER DATA =====")
    customers.show(truncate=False)

    # --------------------------------------------------
    # 3. Select columns
    # --------------------------------------------------

    print("\n===== SELECT COLUMNS =====")

    customers.select(
        "customer_id",
        "name",
        "city",
        "total_spend"
    ).show()

    # --------------------------------------------------
    # 4. Filter customers
    # --------------------------------------------------

    print("\n===== PREMIUM CUSTOMERS =====")

    premium_customers = customers.filter(
        col("segment") == "Premium"
    )

    premium_customers.select(
        "customer_id",
        "name",
        "segment",
        "total_spend"
    ).show()

    # --------------------------------------------------
    # 5. withColumn expressions
    # --------------------------------------------------

    enriched_customers = prepare_customer_data(customers)

    print("\n===== ENRICHED CUSTOMER DATA =====")

    enriched_customers.select(
        "customer_id",
        "name",
        "total_spend",
        "orders",
        "spend_category",
        "average_order_value"
    ).show()

    # --------------------------------------------------
    # 6. Temporary View
    # --------------------------------------------------

    enriched_customers.createOrReplaceTempView(
        "customers"
    )

    # --------------------------------------------------
    # 7. Spark SQL
    # --------------------------------------------------

    print("\n===== SQL: HIGH VALUE CUSTOMERS =====")

    high_value = spark.sql(
        """
        SELECT
            customer_id,
            name,
            city,
            total_spend,
            spend_category
        FROM customers
        WHERE total_spend >= 70000
        ORDER BY total_spend DESC
        """
    )

    high_value.show()

    # --------------------------------------------------
    # 8. City Analytics
    # --------------------------------------------------

    print("\n===== SQL: CITY ANALYTICS =====")

    city_analytics = spark.sql(
        """
        SELECT
            city,
            COUNT(*) AS customer_count,
            ROUND(SUM(total_spend), 2) AS total_city_spend,
            ROUND(AVG(total_spend), 2) AS average_city_spend
        FROM customers
        GROUP BY city
        ORDER BY total_city_spend DESC
        """
    )

    city_analytics.show()

    # --------------------------------------------------
    # 9. Segment Analytics
    # --------------------------------------------------

    print("\n===== SQL: SEGMENT ANALYTICS =====")

    segment_analytics = spark.sql(
        """
        SELECT
            segment,
            COUNT(*) AS customer_count,
            ROUND(SUM(total_spend), 2) AS total_spend,
            ROUND(AVG(total_spend), 2) AS average_spend
        FROM customers
        GROUP BY segment
        ORDER BY total_spend DESC
        """
    )

    segment_analytics.show()

    # --------------------------------------------------
    # 10. Overall Analytics
    # --------------------------------------------------

    print("\n===== SQL: OVERALL CUSTOMER ANALYTICS =====")

    overall = spark.sql(
        """
        SELECT
            COUNT(*) AS total_customers,
            ROUND(SUM(total_spend), 2) AS total_revenue,
            ROUND(AVG(total_spend), 2) AS average_customer_spend,
            SUM(orders) AS total_orders
        FROM customers
        """
    )

    overall.show()

    # --------------------------------------------------
    # 11. JSON DataFrame demonstration
    # --------------------------------------------------

    print("\n===== JSON DATAFRAME =====")

    json_customers = (
        spark.read
        .option("multiline", False)
        .json("data/customers.json")
    )

    json_customers.printSchema()
    json_customers.show()

    spark.stop()


if __name__ == "__main__":
    main()
