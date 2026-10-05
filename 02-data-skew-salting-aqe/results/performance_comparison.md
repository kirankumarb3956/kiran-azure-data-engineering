# Performance Comparison

Run the workload on Databricks and record actual Spark UI metrics.

| Approach | Expected behavior | Actual runtime |
|---|---|---|
| Normal join | One skewed task may dominate | Fill after run |
| Repartition | Redistributes partitions but may retain hot-key concentration | Fill after run |
| AQE | Spark can split skewed shuffle partitions at runtime | Fill after run |
| Salting | Hot key is deliberately split across salted groups | Fill after run |

Do not claim a percentage improvement until measured on the target cluster.
