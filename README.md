# celery-gevent-kombu-blocking-sleep-issue

Reproduction: a `celery worker -P gevent` freezes every greenlet, running tasks included, while it retries a lost broker connection, because kombu has bound the unpatched `time.sleep`.

## Bug

`celery worker -P gevent` imports the `celery` package first and only then calls `gevent.monkey.patch_all()` (`celery/__main__.py:13` → `celery/__init__.py:116`). Importing `celery` has already created lazy `Proxy(getappattr, ...)` objects for the legacy compat modules (`celery/local.py:520`, `:529`).

`patch_all()` patches `thread` before `time` (`gevent/monkey/__init__.py:710`, `:714`). While patching `thread`, gevent walks `gc.get_objects()` and calls `isinstance(o, rlock_type)` on every object (`gevent/monkey/_patch_thread_common.py:78`, `:93`). `isinstance` falls back to `o.__class__`, and on a celery `Proxy` that is a property which evaluates the proxy (`celery/local.py:95` → `getappattr()`, `:390`). The evaluation imports `celery._state` → `celery.utils` → `kombu.utils` → `kombu/utils/functional.py:11` `from time import sleep, time`, before `time` has been patched. About 200 modules (`kombu.connection` and `amqp.transport` among them) get imported in the middle of `patch_all()` this way, and nothing warns about it.

From then on `retry_over_time()` sleeps with the real `time.sleep` (`kombu/utils/functional.py:349`, `:351`), called from `Connection.ensure_connection()` when the consumer reconnects (`celery/worker/consumer/consumer.py:690`). Every retry wait (2, 4, 6, 8, ... s) blocks the OS thread, which means the whole gevent hub.

Line numbers refer to celery 5.7.0, kombu 5.7.0 and gevent 26.9.0. The same code is on celery `main`, kombu `main` and gevent `master` as of 2026-11-09. The Celery docs present this CLI path as the supported one: "When using the gevent pool via the Celery CLI (`-P gevent`/`--pool=gevent`), Celery applies gevent monkey-patching early during startup." ([docs/userguide/concurrency/gevent.rst](https://github.com/celery/celery/blob/main/docs/userguide/concurrency/gevent.rst))

**Measured by `main.py`** (Python 3.14; the broker is stopped for 16 s while a task loops on `gevent.sleep(0.5)`):

```
Worker ready: time patched=True, kombu.utils.functional.sleep is unpatched time.sleep=True
Worker reconnect attempts during outage: waits 2.00, 4.00, 6.00, 8.00 s
Task tick gaps > 1.5s: 2.2, 4.0, 6.0 (task silent for 3.4s when the outage window closed)
Max task tick gap during broker outage: 6.0s (expected ~0.5s)

With --workaround (same outage, same reconnect attempts):
Worker ready: time patched=True, kombu.utils.functional.sleep is unpatched time.sleep=False
Task tick gaps > 1.5s: none (task silent for 0.4s when the outage window closed)
Max task tick gap during broker outage: 0.5s (expected ~0.5s)
```

The bad binding can be checked without a broker:

```bash
uv run python -c "import sys, celery; sys.argv += ['-P', 'gevent']; celery.maybe_patch_concurrency(); import kombu.utils.functional as f, gevent.monkey as m; print(f.sleep is m.get_original('time', 'sleep'))"
# True
```

## Reproduce

Requires Docker and Python 3.11+.

```bash
uv sync
uv run python main.py
```

Control run with the workaround below:

```bash
uv run python main.py --workaround
```

`RABBITMQ_PORT` (default 5679) changes the host port, and `--outage SECONDS` (default 16) changes how long the broker stays down.

## Expected Result

```
Max task tick gap during broker outage: 6.0s (expected ~0.5s)  BLOCKING SLEEP CONFIRMED
```

## Workaround (no code change)

Let gevent patch everything before celery is imported:

```bash
python -m gevent.monkey --module celery -A tasks worker -P gevent
```

## Results

All rows are `main.py` runs (broker down for 16 s). The fix rows change only the named spot in an otherwise identical environment.

| Setup | Result |
|---|---|
| celery 5.7.0, kombu 5.7.0, gevent 26.9.0, Python 3.14 | 3/3 CONFIRMED (max gap 6.0 s) |
| same, `--workaround` | 0/2 (max gap 0.5 s) |
| celery `main` @1a48583d, kombu `main` @38fbf784, gevent 26.9.0 | 2/2 CONFIRMED |
| celery 5.7.0, kombu 5.7.0, gevent 26.9.0, Python 3.12 | 2/2 CONFIRMED |
| gevent fix: `isinstance(o, T)` → `issubclass(type(o), T)` in `_patch_existing_locks()` | 0/2 |
| celery fix: build compat modules on first access (`LazyModule.__getattr__`), not in `recreate_module()` | 0/2 |
| kombu fix: `import time` and call `time.sleep()` in `retry_over_time()` | 0/2 |

Any one of the three fixes removes the stall. The gevent and celery fixes also stop kombu and amqp from being imported in the middle of `patch_all()`. The kombu fix covers only `sleep`: `kombu.resource.LifoQueue` and `amqp.channel.Queue` still hold the unpatched `queue` classes.
