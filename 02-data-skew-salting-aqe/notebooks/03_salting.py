from pyspark.sql import functions as F
NUM_SALTS = 4
sales = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/sample_sales.csv")
store_master = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/store_master.csv")
sales_salted = sales.withColumn("salt", F.pmod(F.hash("order_id"), F.lit(NUM_SALTS)))
salt_values = F.array(*[F.lit(i) for i in range(NUM_SALTS)])
store_salted = store_master.withColumn("salt", F.explode(salt_values))
result = sales_salted.join(store_salted, ["store_id", "salt"], "left")
result.show()
# Hot key S01 is split into S01+0 ... S01+3, allowing multiple shuffle groups/tasks.
