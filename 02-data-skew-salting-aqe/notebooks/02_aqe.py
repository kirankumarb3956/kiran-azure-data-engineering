# AQE can inspect runtime statistics and adapt the physical plan.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
sales = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/sample_sales.csv")
store_master = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/store_master.csv")
result = sales.join(store_master, "store_id", "left")
result.explain("formatted")
