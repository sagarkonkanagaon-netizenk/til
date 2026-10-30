# Salting a skewed join

**Problem.** One key owns most of the rows (a giant seller, a default `null` id, a test
account). Every row for that key lands in the same shuffle partition, so one task runs
for an hour while the other 199 finish in seconds.

**Check first.** Before salting, try the cheap fixes:

```python
# 1. Is the other side small? Broadcast it and skip the shuffle entirely.
from pyspark.sql.functions import broadcast
joined = facts.join(broadcast(dims), "customer_id")

# 2. Spark 3+: let AQE split skewed partitions for you.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Find the hot keys.**

```python
facts.groupBy("customer_id").count().orderBy("count", ascending=False).show(10)
```

**Salt.** Add a random bucket to the big side, replicate the small side once per bucket,
and join on `(key, salt)`. The hot key now spreads over `N` tasks.

```python
from pyspark.sql import functions as F

N = 16  # buckets; start near (hot key rows / rows a task handles comfortably)

facts_salted = facts.withColumn("salt", (F.rand() * N).cast("int"))

dims_salted = dims.withColumn(
    "salt", F.explode(F.array([F.lit(i) for i in range(N)]))
)

joined = facts_salted.join(dims_salted, ["customer_id", "salt"]).drop("salt")
```

**Gotcha.** Salting multiplies the small side by `N`. Salt only the hot keys (and leave
`salt = 0` for the rest) when the dimension is big:

```python
hot = ["c_42", "c_7"]
facts_salted = facts.withColumn(
    "salt",
    F.when(F.col("customer_id").isin(hot), (F.rand() * N).cast("int")).otherwise(0),
)
# explode can't sit inside when(), so pick the array first, then explode it
dims_salted = dims.withColumn(
    "salt",
    F.explode(
        F.when(
            F.col("customer_id").isin(hot),
            F.array([F.lit(i) for i in range(N)]),
        ).otherwise(F.array(F.lit(0)))
    ),
)
```

In the Spark UI the task timeline goes from one long bar to an even block.
