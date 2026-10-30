# TIL · Today I Learned

Short, practical notes from building data pipelines: Spark, Delta Lake, Airflow, SQL.
Each note is one problem, one fix, and code you can paste. Written by
[Sagar Konkanagaon](https://data-foundry-sand.vercel.app).

<!-- index: newest first -->
## Spark

- [Salting a skewed join](spark/salting-a-skewed-join.md)
- [Deduplicating a stream without unbounded state](spark/dedup-streaming-with-watermark.md)
- [Fixing the small-files problem on write](spark/small-files-problem.md)

## Delta Lake

- [A backfill you can run twice with `replaceWhere`](delta/idempotent-backfill-replacewhere.md)
- [SCD Type 2 with one `MERGE`](delta/scd2-with-one-merge.md)
- [`OPTIMIZE`, `ZORDER` and `VACUUM` without surprises](delta/optimize-zorder-vacuum.md)

## Airflow

- [Sensors that don't hog worker slots](airflow/sensors-reschedule-or-deferrable.md)

## SQL

- [Latest row per key with `ROW_NUMBER`](sql/latest-row-per-key.md)

---

**Why this repo?** I learn something every week that I'd otherwise forget. Writing it
down keeps it, and maybe saves you an hour.
