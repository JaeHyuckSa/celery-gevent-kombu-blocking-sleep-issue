# celery-gevent-kombu-blocking-sleep-issue

Reproduction: a `celery worker -P gevent` freezes every greenlet, running tasks included, while it retries a lost broker connection, because kombu has bound the unpatched `time.sleep`.

## Bug

`celery worker -P gevent` imports the `celery` package first and only then calls `gevent.monkey.patch_all()` (`celery/__main__.py:13` → `celery/__init__.py:116`). Importing `celery` has already created lazy `Proxy(getappattr, ...)` objects for the legacy compat modules (`celery/local.py:520`, `:529`).

`patch_all()` patches `thread` before `time` (`gevent/monkey/__init__.py:710`, `:714`). While patching `thread`, gevent walks `gc.get_objects()` and calls `isinstance(o, rlock_type)` on every object (`gevent/monkey/_patch_thread_common.py:78`, `:93`). `isinstance` falls back to `o.__class__`, and on a celery `Proxy` that is a property which evaluates the proxy (`celery/local.py:95` → `getappattr()`, `:390`). The evaluation imports `celery._state` → `celery.utils` → `kombu.utils` → `kombu/utils/functional.py:11` `from time import sleep, time`, before `time` has been patched. About 200 modules (`kombu.connection` and `amqp.transport` among them) get imported in the middle of `patch_all()` this way, and nothing warns about it.

From then on `retry_over_time()` sleeps with the real `time.sleep` (`kombu/utils/functional.py:349`, `:351`), called from `Connection.ensure_connection()` when the consumer reconnects (`celery/worker/consumer/consumer.py:690`). Every retry wait (2, 4, 6, 8, ... s) blocks the OS thread, which means the whole gevent hub.

Line numbers refer to celery 5.7.0, kombu 5.7.0 and gevent 26.9.0. The same code is on celery `main`, kombu `main` and gevent `master` as of 2026-11-09. The Celery docs present this CLI path as the supported one: "When using the gevent pool via the Celery CLI (`-P gevent`/`--pool=gevent`), Celery applies gevent monkey-patching early during startup." ([docs/userguide/concurrency/gevent.rst](https://github.com/celery/celery/blob/main/docs/userguide/concurrency/gevent.rst))

**Measured by `main.py`** (Python 3.14; a greenlet in the worker loops on `gevent.sleep(0.5)` and records how long each step took):

```
$ uv run python main.py
Worker started: time patched=True, kombu.utils.functional.sleep is unpatched time.sleep=True
Worker reconnect waits: 2.00, 4.00, 6.00, 8.00 s
Hub tick gaps > 1.5s: 2.0, 4.0, 6.0 (hub silent for 3.4s when the window closed)
Max hub tick gap while the broker is unreachable: 6.0s (expected ~0.5s)

$ uv run python main.py --broker-stop      # RabbitMQ stopped under a running task
Task tick gaps > 1.5s: 2.1, 4.0, 6.0 (task silent for 3.3s when the window closed)

$ uv run python main.py --workaround       # same reconnect waits
Worker started: time patched=True, kombu.utils.functional.sleep is unpatched time.sleep=False
Hub tick gaps > 1.5s: none (hub silent for 0.0s when the window closed)
Max hub tick gap while the broker is unreachable: 0.5s (expected ~0.5s)
```

The bad binding can be checked without a broker:

```bash
uv run python -c "import sys, celery; sys.argv += ['-P', 'gevent']; celery.maybe_patch_concurrency(); import kombu.utils.functional as f, gevent.monkey as m; print(f.sleep is m.get_original('time', 'sleep'))"
# True
```

## Reproduce

Requires Python 3.11+. No broker is needed: the worker points at a closed local port and stays in its reconnect loop. Docker is only needed for `--broker-stop`.

```bash
uv sync
uv run python main.py
```

Control run with the workaround below:

```bash
uv run python main.py --workaround
```

Same thing with a real RabbitMQ that is stopped while a task is running (the task's own greenlet freezes too):

```bash
uv run python main.py --broker-stop
```

`--seconds N` (default 16) sets how long the broker stays unreachable, and `RABBITMQ_PORT` (default 5679) sets the host port for `--broker-stop`.

## Expected Result

```
Max hub tick gap while the broker is unreachable: 6.0s (expected ~0.5s)  BLOCKING SLEEP CONFIRMED
```

## Workaround (no code change)

Let gevent patch everything before celery is imported:

```bash
python -m gevent.monkey --module celery -A tasks worker -P gevent
```

## Results

`main.py` runs with a 16 s window, so the 8 s wait that follows the 6 s one is cut off and the largest complete stall is about 6 s. The fix rows change only the named spot in an otherwise identical environment.

| Setup | Result | Max stall |
|---|---|---|
| celery 5.7.0, kombu 5.7.0, gevent 26.9.0, Python 3.14 | 3/3 CONFIRMED | 6.0 s |
| same, `--workaround` | 0/1 | 0.5 s |
| same, `--broker-stop` (running task) | 2/2 CONFIRMED | 6.0 s |
| same, `--broker-stop --workaround` | 0/1 | 0.5 s |
| celery `main` @1a48583d, kombu `main` @38fbf784, gevent 26.9.0, Python 3.14 | 3/3 CONFIRMED | 6.1 s |
| celery 5.7.0, kombu 5.7.0, gevent 26.9.0, Python 3.12 | 3/3 CONFIRMED | 6.0 s |
| gevent fix: `isinstance(o, T)` → `issubclass(type(o), T)` in `_patch_existing_locks()` | 0/3 | 0.5 s |
| celery fix: build compat modules on first access (`LazyModule.__getattr__`), not in `recreate_module()` | 0/3 | 0.5 s |
| kombu fix: `import time` and call `time.sleep()` in `retry_over_time()` | 0/3 | 0.5 s |

## Where to fix

Each of the three fixes above removes the stall, but they differ in scope:

- **celery (primary):** the documented `-P gevent` CLI path promises early patching, yet `import celery` leaves compat-module proxies on the heap, and evaluating them loads about 200 modules in the middle of `patch_all()`. Building the compat modules lazily removes the trigger, so kombu and amqp are imported only after patching.
- **gevent (secondary):** `_patch_existing_locks()` runs the `__class__` property of every object that existed before patching, which can mean arbitrary code. `issubclass(type(o), T)` finds the same real locks and runs no user code, so it also covers proxies from other libraries.
- **kombu (defensive):** `from time import sleep` is a common idiom (7 modules in kombu 5.7.0). Looking up `time.sleep` at call time fixes this one wait, but the early imports remain, and `kombu.resource.LifoQueue` and `amqp.channel.Queue` still hold the unpatched `queue` classes.
