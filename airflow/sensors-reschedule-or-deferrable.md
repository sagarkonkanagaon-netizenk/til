# Sensors that don't hog worker slots

**Problem.** A sensor waiting for a file sits in a worker slot for hours doing nothing
but sleeping. Ten of them and your real tasks queue behind them.

**Fix 1: `mode="reschedule"`.** Between checks the task gives its slot back and is
rescheduled for the next poke.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_file = S3KeySensor(
    task_id="wait_for_orders_file",
    bucket_key="s3://landing/orders/{{ ds }}/_SUCCESS",
    mode="reschedule",      # free the slot between pokes
    poke_interval=300,      # check every 5 min
    timeout=6 * 60 * 60,    # give up after 6 h
)
```

**Fix 2: `deferrable=True`.** If the sensor supports it, the wait moves to the
triggerer process (async), so it costs almost nothing while waiting.

```python
wait_for_file = S3KeySensor(
    task_id="wait_for_orders_file",
    bucket_key="s3://landing/orders/{{ ds }}/_SUCCESS",
    deferrable=True,        # needs a triggerer running
    timeout=6 * 60 * 60,
)
```

**Rule of thumb.**
- Short waits (seconds to a couple of minutes): default `poke` mode is fine.
- Long waits: `deferrable=True` when available, otherwise `mode="reschedule"`.
- Always set `timeout`. A sensor without one can wait forever.
