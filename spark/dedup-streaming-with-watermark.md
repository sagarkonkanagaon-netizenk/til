# Deduplicating a stream without unbounded state

**Problem.** Sources are at-least-once: producers retry, consumers replay old offsets,
phones flush buffered events hours later. Duplicates inflate revenue and session counts.

**Naive fix (don't).** `dropDuplicates(["event_id"])` on a stream keeps *every* id
ever seen in state. It works on day one and runs out of memory in week three.

**Fix.** Bound the state with an event-time watermark.

```python
events = (
    spark.readStream.format("kafka")
    .option("subscribe", "events")
    .load()
    # ...parse value into columns: event_id, event_time, ...
)

deduped = (
    events
    .withWatermark("event_time", "2 hours")
    # Spark 3.5+: ids are kept only until the watermark passes them
    .dropDuplicatesWithinWatermark(["event_id"])
)
```

On Spark before 3.5, include the event-time column in the key so state can expire:

```python
deduped = events.withWatermark("event_time", "2 hours").dropDuplicates(
    ["event_id", "event_time"]
)
```

**The trade-off.** The watermark is the knob between correctness and state size.
A duplicate that arrives later than the watermark gets through. Pick the delay from
real lateness data (p99 of `ingest_time - event_time`), not a guess.

**Don't lose late rows.** Events later than the watermark are dropped from the
streaming aggregate. If they matter, also land the raw stream in Bronze and let a small
batch job merge stragglers in.
