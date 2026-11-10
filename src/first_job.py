from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
            .appName("SparkPerformanceLab-FirstJob")\
            .getOrCreate()

# From 0 to 99_999, increasing with step 1, and 2 partitions
orders = spark.range(0, 100_000, 1, 2)

result = orders.withColumn("category", col("id") % 4)\
            .groupBy("category")\
            .count()

print("=== PHYSICAL EXECUTION PLAN ===")
result.explain("formatted")

print("=== RESULTS ===")
result.show()

spark.stop()