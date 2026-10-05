🚀 Data Skew in Azure Databricks — A Real Performance Problem

One interesting Spark performance issue I explored recently was Data Skew.

S01 → 1 Crore records 😰
S02 → 1 Lakh
S03 → 80K

One hot key can cause one Spark task to process much more data while other tasks finish early.

My approach:
✅ Identify skewed keys
✅ Check Spark UI
✅ Use AQE for runtime skew handling
✅ Use Salting when the hot key is predictable and severe

With salting, S01 → 1 Crore can be split roughly across S01_0, S01_1, S01_2 and S01_3.

Simple idea: Split the hot key → distribute the workload → reduce the bottleneck.

#AzureDatabricks #PySpark #DataEngineering #DataSkew #Spark #PerformanceTuning
