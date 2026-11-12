import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, lit, date_add


DATA_PATH = "/opt/spark/work-dir/data"

def generate_products(spark: SparkSession):
    """Generate the product dimension."""
    return (
        spark.range(1, 1001)
        .withColumnRenamed("id", "product_id")
        .withColumn(
            "category",
            when(col("product_id") % 4 == 0, "Electronics")
            .when(col("product_id") % 4 == 1, "Clothing")
            .when(col("product_id") % 4 == 2, "Home")
            .otherwise("Sports"),
        )
        .withColumn(
            "brand",
            concat(lit("Brand_"), col("product_id") % 20),
        )
    )


def generate_stores(spark: SparkSession):
    """Generate the store dimension."""
    return (
        spark.range(1, 41)
        .withColumnRenamed("id", "store_id")
        .withColumn(
            "region",
            when(col("store_id") % 4 == 0, "North")
            .when(col("store_id") % 4 == 1, "South")
            .when(col("store_id") % 4 == 2, "East")
            .otherwise("West"),
        )
        .withColumn(
            "city",
            concat(lit("City_"), col("store_id") % 10),
        )
    )


def generate_sales(spark: SparkSession, rows: int):
    """Generate synthetic sales fact records."""
    return (
        spark.range(1, rows + 1, 1, 4)
        .withColumnRenamed("id", "sale_id")
        .withColumn(
            "product_id",
            ((col("sale_id") * 17) % 1000) + 1,
        )
        .withColumn(
            "store_id",
            ((col("sale_id") * 13) % 40) + 1,
        )
        .withColumn(
            "sale_date",
            date_add(
                lit("2026-01-01").cast("date"),
                (col("sale_id") % 90).cast("int"),
            ),
        )
        .withColumn(
            "quantity",
            ((col("sale_id") % 5) + 1).cast("int"),
        )
        .withColumn(
            "unit_price",
            (
                (col("product_id") % 90) + 10
            ).cast("decimal(10,2)"),
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be a positive integer")

    spark = (
        SparkSession.builder
        .appName("SparkPerformanceLab-GenerateData")
        .getOrCreate()
    )

    try:
        datasets = {
            "dim_product": generate_products(spark),
            "dim_store": generate_stores(spark),
            "fact_sales": generate_sales(spark, args.rows),
        }

        for name, df in datasets.items():
            path = f"{DATA_PATH}/{name}"
            df.write.mode("overwrite").parquet(path)
            print(f"Generated dataset: {path}")

        print("DATA GENERATION COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
