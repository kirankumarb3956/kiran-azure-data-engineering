# Data Skew Architecture

Retail Sales -> Azure Databricks -> Check key distribution -> AQE / Salting -> Optimized Join -> Gold/Delta

AQE: runtime adaptation. Salting: deliberately split a predictable hot key. Broadcast: useful when the other join side is genuinely small. Repartition alone does not necessarily split identical hot keys.
