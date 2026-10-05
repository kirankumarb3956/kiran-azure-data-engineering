from pyspark.sql import functions as F
sales = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/sample_sales.csv")
sales.groupBy("store_id").count().orderBy(F.desc("count")).show(20, False)
sales.groupBy("store_id").count().agg(F.max("count").alias("max_records"),F.min("count").alias("min_records"),F.avg("count").alias("avg_records")).show()
# Also inspect Spark UI -> SQL -> Exchange/Shuffle for task duration and input-size imbalance.
