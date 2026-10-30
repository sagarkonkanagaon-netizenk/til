# Latest row per key with `ROW_NUMBER`

**Problem.** A table has many versions of each record (CDC, re-sent files) and you need
exactly one: the latest.

**Fix.** Number the rows inside each key, newest first, and keep row 1.

```sql
SELECT *
FROM (
  SELECT
    o.*,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY updated_at DESC, ingest_ts DESC   -- tie-breaker matters
    ) AS rn
  FROM raw_orders o
) t
WHERE rn = 1;
```

Databricks SQL, BigQuery and Snowflake let you skip the subquery with `QUALIFY`:

```sql
SELECT *
FROM raw_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;
```

Same thing in PySpark:

```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("ingest_ts").desc())
latest = df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
```

**Gotchas.**
- Use `ROW_NUMBER`, not `RANK`: `RANK` returns several rows when timestamps tie.
- Always add a tie-breaker column. Without one, which row "wins" a tie can change
  between runs, and your table silently flips.
