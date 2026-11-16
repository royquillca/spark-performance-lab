
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_PATH = "/opt/spark/work-dir/data"
OUTPUT_PATH = f"{DATA_PATH}/output/baseline_sales_summary"


def main():
    spark = (
        SparkSession.builder
        .appName("SparkPerformanceLab-BaselineETL")
        .getOrCreate()
    )

    try:
        # Extract: read the fact and dimension tables.
        sales = spark.read.parquet(f"{DATA_PATH}/fact_sales")
        products = spark.read.parquet(f"{DATA_PATH}/dim_product")
        stores = spark.read.parquet(f"{DATA_PATH}/dim_store")

        # Transform: join facts with descriptive dimensions.
        enriched_sales = (
            sales
            .join(products, on="product_id", how="inner")
            .join(stores, on="store_id", how="inner")
        )

        # Aggregate revenue by region and product category.
        summary = (
            enriched_sales
            .groupBy("region", "category")
            .agg(
                F.sum(
                    F.col("quantity") * F.col("unit_price")
                ).alias("total_revenue"),
                F.count("*").alias("total_sales"),
            )
        )

        print("\n=== BASELINE PHYSICAL PLAN ===")
        summary.explain("formatted")

        print("\n=== BASELINE CONFIGURATION ===")
        for key in [
            "spark.sql.adaptive.enabled",
            "spark.sql.autoBroadcastJoinThreshold",
            "spark.sql.shuffle.partitions",
        ]:
            print(f"{key}: {spark.conf.get(key)}")

        # Load: materialize the complete analytical result.
        start = perf_counter()

        summary.write.mode("overwrite").parquet(OUTPUT_PATH)

        elapsed = perf_counter() - start

        print("\n=== BASELINE RESULTS ===")
        print(f"Output path: {OUTPUT_PATH}")
        print(f"Write action elapsed time: {elapsed:.3f} seconds")

        # Validate the persisted output.
        output = spark.read.parquet(OUTPUT_PATH)

        output.orderBy("region", "category").show(
            20, truncate=False
        )

        print(f"Output groups: {output.count()}")
        print("BASELINE ETL COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
