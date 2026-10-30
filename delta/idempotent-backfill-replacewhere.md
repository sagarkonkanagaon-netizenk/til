# A backfill you can run twice with `replaceWhere`

**Problem.** A two-year backfill fails on day 412. Someone restarts it from the top.
If the job *appends*, half the history is now loaded twice.

**Fix.** Make each unit of work one partition, and make the write an **overwrite of
exactly that partition**. Rerunning a day replaces it instead of duplicating it.

```python
from datetime import date, timedelta

def load_day(day: date) -> None:
    d = day.isoformat()
    df = (
        spark.read.table("raw.orders")
        .where(f"order_date = '{d}'")
        # ...transformations...
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"order_date = '{d}'")
        .saveAsTable("silver.orders")
    )

day = date(2024, 1, 1)
while day <= date(2025, 12, 31):
    load_day(day)
    day += timedelta(days=1)
```

**Make it resumable.** Record finished days and skip them, and audit before you mark a
day done (at the end of `load_day`):

```python
src = spark.read.table("raw.orders").where(f"order_date = '{d}'").count()
dst = spark.read.table("silver.orders").where(f"order_date = '{d}'").count()
assert src == dst, f"{d}: {src} source rows vs {dst} target rows"
```

**Why it matters.** Kill the job anywhere, start it again, or run the whole range twice:
the target ends up identical. In Airflow, this is what makes `catchup` and
"clear task" safe.

**Gotcha.** `replaceWhere` fails if the DataFrame contains rows outside the predicate,
which is a feature: it stops a bad filter from silently overwriting the wrong day.
