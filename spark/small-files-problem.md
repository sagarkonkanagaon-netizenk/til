# Fixing the small-files problem on write

**Problem.** A daily job writes 2,000 files of 40 KB each. Every downstream read now
spends more time opening files than reading data.

**Why it happens.** Each task writes its own file per output partition. 200 shuffle
partitions × 10 date partitions = up to 2,000 files.

**Fix 1: let AQE coalesce shuffle partitions** (on by default since Spark 3.2):

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**Fix 2: repartition by the output partition column before writing**, so each date
is written by one task:

```python
(
    df.repartition("order_date")
    .write.mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3://lake/silver/orders")
)
```

Use `repartition(n, "order_date")` if a single day is too big for one task.

**Fix 3 (Delta on Databricks): optimized writes and compaction.**

```sql
ALTER TABLE silver.orders SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);
OPTIMIZE silver.orders;  -- compacts existing small files
```

**Gotcha.** `coalesce(1)` "fixes" it by funnelling everything through one task. Fine for
a tiny export, terrible for a real table.
