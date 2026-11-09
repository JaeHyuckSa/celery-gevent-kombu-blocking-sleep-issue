# celery-gevent-kombu-blocking-sleep-issue

Reproduction: a `celery worker -P gevent` freezes every greenlet, running tasks included, while it retries the broker connection, because kombu holds the unpatched `time.sleep`.

## Bug

`celery worker -P gevent` calls `gevent.monkey.patch_all()` only after the `celery` package is imported. While gevent patches `threading`, it runs `isinstance()` on every object in `gc.get_objects()` (`gevent/monkey/_patch_thread_common.py:78`). That calls the `__class__` property of celery's lazy compat proxies (`celery/local.py:95`), which imports kombu before `time` has been patched. `kombu/utils/functional.py:11` then binds the original `time.sleep`, and every wait in `retry_over_time()` blocks the whole hub for 2, 4, 6, ... s (celery 5.7.0, kombu 5.7.0, gevent 26.9.0).

```
kombu.utils.functional.sleep is unpatched time.sleep: True
Reconnect waits: 2.00, 4.00, 6.00, 8.00 s
Tick gaps > 1.5s: 2.0, 4.0, 6.0
Max tick gap: 6.0s (expected ~0.5s)  BLOCKING SLEEP CONFIRMED
```

## Reproduce

Requires Python 3.11+. No broker is needed: the worker points at a closed port.

```bash
uv sync
uv run python main.py
```

With the workaround:

```bash
uv run python main.py --workaround
```

## Expected Result

```
Max tick gap: 6.0s (expected ~0.5s)  BLOCKING SLEEP CONFIRMED
```

## Workaround (no code change)

```bash
python -m gevent.monkey --module celery -A tasks worker -P gevent
```
