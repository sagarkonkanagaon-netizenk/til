# SCD Type 2 with one `MERGE`

**Problem.** A customer moves city and the dimension row is overwritten. Last quarter's
"sales by region" quietly changes every time the report is re-run.

**Fix.** Keep history: expire the old row and insert a new one. The trick that does
both in a single Delta `MERGE` is to send each changed key **twice**: once to match and
expire the current row, once with a `NULL` merge key so it falls through to the insert.

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

tracked = ["city", "tier"]
updates = cdc.withColumn("hash", F.sha2(F.concat_ws("||", *tracked), 256))

dim = DeltaTable.forName(spark, "gold.dim_customer")
current = dim.toDF().where("is_current = true")

# Rows whose tracked columns changed
changed = (
    updates.alias("u")
    .join(current.alias("c"), "customer_id")
    .where("u.hash <> c.hash")
    .select("u.*")
)

staged = (
    updates.withColumn("merge_key", F.col("customer_id"))  # matches → expire / no-op
    .unionByName(changed.withColumn("merge_key", F.lit(None)))  # never matches → insert
)

(
    dim.alias("t")
    .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
    .whenMatchedUpdate(
        condition="t.hash <> s.hash",
        set={"is_current": "false", "valid_to": "s.updated_at"},
    )
    .whenNotMatchedInsert(
        values={
            "customer_id": "s.customer_id",
            "city": "s.city",
            "tier": "s.tier",
            "hash": "s.hash",
            "valid_from": "s.updated_at",
            "valid_to": "null",
            "is_current": "true",
        }
    )
    .execute()
)
```

**Result.** Exactly one current row per customer, every past version kept with its
validity window. Running the same batch twice changes nothing, because unchanged hashes
don't match the update condition.

**Gotcha.** If the CDC batch has several changes for the same key, keep only the latest
per key (window on `updated_at`) before merging, or process them in order.
