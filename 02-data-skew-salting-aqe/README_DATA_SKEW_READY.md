# 🚀 Data Skew, AQE & Salting — Azure Databricks

A hands-on Azure Databricks performance optimization project demonstrating how to identify and mitigate **Data Skew** in Apache Spark.

---

## 🎯 Business Scenario

Imagine a retail company processing millions of sales records.

Most stores have a normal number of transactions, but one store has a very large number of records:

| Store | Records |
|---|---:|
| S01 | 100M |
| S02 | 1M |
| S03 | 800K |
| S04 | 500K |

`S01` is a **hot key**.

When Spark performs a shuffle or join using `store_id`, a disproportionate amount of data can end up in one partition.

This creates **Data Skew**.

---

## ⚠️ What is Data Skew?

Data Skew occurs when data is distributed unevenly across Spark partitions.

### Example

```text
S01 → 100M records 😰
S02 → 1M records
S03 → 800K records
S04 → 500K records
```

One Spark task may receive a much larger workload than the others.

```text
Partition 1 → 100M records  🔴
Partition 2 → 1M records    🟢
Partition 3 → 800K records  🟢
Partition 4 → 500K records  🟢
```

### Result

- One task runs much longer
- Other tasks finish early
- Cluster resources are underutilized
- Overall Spark job becomes slower

---

## 🔍 How I Identify Data Skew

I use two approaches:

### 1. Check key distribution

```python
sales.groupBy("store_id") \\
    .count() \\
    .orderBy("count", ascending=False) \\
    .show()
```

This helps identify hot keys.

### 2. Check Spark UI

I inspect:

- Shuffle Read
- Shuffle Write
- Partition sizes
- Task execution time
- Long-running / straggler tasks

A large difference between task sizes or execution times is a strong indication of skew.

---

# ⚙️ Solutions

## 1️⃣ Adaptive Query Execution (AQE)

**AQE = Adaptive Query Execution**

AQE allows Spark to use runtime statistics and adapt the execution plan.

Example configuration:

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true"
)
```

For skewed joins, Spark can detect large skewed shuffle partitions and split the skewed work into smaller processing pieces.

### Simple idea

```text
Before

Partition 1 → 100M 🔴
Partition 2 → 1M
Partition 3 → 800K
Partition 4 → 500K


AQE

Large skewed partition
        ↓
Split into smaller pieces
        ↓
25M | 25M | 25M | 25M
```

### Key takeaway

> **AQE = Spark adapts to the problem at runtime.**

---

# 2️⃣ Salting

For predictable and severe hot-key skew, we can use **Salting**.

Instead of keeping:

```text
S01 → 100M records
```

we add a salt value:

```text
S01 + 0 → ~25M
S01 + 1 → ~25M
S01 + 2 → ~25M
S01 + 3 → ~25M
```

### PySpark example

```python
from pyspark.sql import functions as F

NUM_SALTS = 4

sales_salted = sales.withColumn(
    "salt",
    F.pmod(
        F.hash("order_id"),
        F.lit(NUM_SALTS)
    )
)
```

The small lookup table also needs matching salt values:

```python
salt_values = F.array(
    F.lit(0),
    F.lit(1),
    F.lit(2),
    F.lit(3)
)

store_salted = store_master.withColumn(
    "salt",
    F.explode(salt_values)
)
```

Then join using both keys:

```python
result = sales_salted.join(
    store_salted,
    ["store_id", "salt"],
    "left"
)
```

### Key takeaway

> **Salting = We deliberately split the hot key before the shuffle.**

---

# 3️⃣ Broadcast Join

If the other side of the join is genuinely small, broadcasting can be a better solution.

```text
Large Sales Table
        |
        | Join
        |
Small Store Master
        ↓
Broadcast to executors
```

This can avoid an unnecessary shuffle of the large table.

Example:

```python
from pyspark.sql.functions import broadcast

result = sales.join(
    broadcast(store_master),
    "store_id",
    "left"
)
```

### Important

Broadcast is **not always the answer**.

It should be considered when the smaller table is actually small enough to broadcast safely.

---

# 🔄 Repartition vs Salting vs AQE

| Technique | Main Purpose |
|---|---|
| Repartition | Redistribute data |
| AQE | Adapt execution at runtime |
| Salting | Split a predictable hot key |
| Broadcast | Avoid shuffle when one side is small |

### Important point

Simply doing:

```python
df.repartition("store_id")
```

does not necessarily solve severe hot-key skew.

If millions of records have the same key, they can still become concentrated in one partition.

---

# 🏗️ Architecture

```text
              Retail Sales
                   |
                   ↓
          Azure Databricks
                   |
                   ↓
          Check Key Distribution
                   |
          +--------+--------+
          |                 |
          ↓                 ↓
         AQE             Salting
          |                 |
          |          Split Hot Key
          |                 |
          +--------+--------+
                   |
                   ↓
             Optimized Join
                   |
                   ↓
              Delta / Gold
```

---

# 📁 Project Structure

```text
02-data-skew-salting-aqe/
│
├── README.md
├── linkedin-post.md
│
├── architecture/
│   └── data_skew_architecture.md
│
├── notebooks/
│   ├── 01_check_skew.py
│   ├── 02_aqe.py
│   └── 03_salting.py
│
├── sample-data/
│   ├── sample_sales.csv
│   └── store_master.csv
│
└── results/
    └── performance_comparison.md
```

---

# 📊 Performance Measurement

The goal is to compare the execution behavior of different approaches.

| Approach | Expected Behavior |
|---|---|
| Normal Join | Possible skewed task |
| Repartition | Better general distribution, but hot key may remain |
| AQE | Runtime skew handling |
| Salting | Explicitly distributes hot-key workload |
| Broadcast | Avoids large-side shuffle when applicable |

Actual performance should be measured using the **Databricks Spark UI**.

Metrics to capture:

- Job duration
- Stage duration
- Shuffle Read
- Shuffle Write
- Task duration
- Partition size
- Number of tasks

> Performance improvement percentages should only be reported after measuring the workload on an actual Databricks cluster.

---

# 🧠 Key Learning

### Remember this simple rule:

```text
Data Skew
    ↓
One key has too much data
    ↓
One partition/task becomes overloaded
    ↓
Spark job slows down
```

### Solutions

```text
AQE
↓
Spark detects and adapts at runtime

Salting
↓
We split the hot key ourselves

Broadcast
↓
Avoid shuffle when the other table is small
```

---

# 🎤 Interview Explanation

> "I first identify skew by checking key distribution and the Spark UI for uneven partition sizes and long-running tasks. For runtime handling, I use AQE. If I have predictable and severe hot-key skew, I can use salting to distribute that key across multiple partitions. If the other side of the join is small enough, I also consider a broadcast join."

---

## 🛠️ Technologies

- Azure Databricks
- Apache Spark
- PySpark
- Python
- Delta Lake
- Spark SQL
- Adaptive Query Execution
- Data Skew Optimization
- Join Optimization

---

## ⚠️ Portfolio Note

This repository is a hands-on learning and portfolio project.

Performance numbers should be captured from actual Spark UI execution rather than assumed or fabricated.

---

### 👤 Author

**Kiran Kumar Bhaktula**

Azure Data Engineering | Azure SQL | PostgreSQL | Databricks | PySpark
