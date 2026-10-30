# `OPTIMIZE`, `ZORDER` and `VACUUM` without surprises

**`OPTIMIZE`** compacts many small files into fewer large ones (around 1 GB). Reads get
faster, and no data changes.

```sql
OPTIMIZE silver.events WHERE event_date >= '2026-10-01';
```

**`ZORDER BY`** co-locates rows with similar values in the same files, so filters on
those columns skip more files. Pick columns you filter on often that are *not* the
partition column:

```sql
OPTIMIZE silver.events ZORDER BY (customer_id);
```

On newer Delta / Databricks, **liquid clustering** (`CLUSTER BY`) replaces partitioning
plus Z-ordering for most tables.

**`VACUUM`** deletes files no longer referenced by the table. The default retention is
7 days:

```sql
VACUUM silver.events;                  -- keeps 7 days of old files
VACUUM silver.events RETAIN 168 HOURS; -- same, explicit
```

**Gotchas.**
- After `VACUUM`, time travel to versions older than the retention window fails,
  because their files are gone.
- Going below 7 days needs `spark.databricks.delta.retentionDurationCheck.enabled = false`.
  That check exists because a long-running reader or writer can still need those files.
  Don't turn it off casually.
- `OPTIMIZE` rewrites files, so the *next* `VACUUM` is what actually frees the space.
