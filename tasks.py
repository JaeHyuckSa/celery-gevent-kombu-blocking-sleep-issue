"""Minimal Celery app for the blocking-sleep reproduction.

Inside the gevent worker a background greenlet (and, with a broker, the ``tick``
task) sleeps ``gevent.sleep(0.5)`` in a loop and records how long each step
really took. Anything that blocks the gevent hub shows up as a gap the length
of the block.
"""

import json
import os
import tempfile
import time
from typing import Any

import celery
import gevent
import kombu.utils.functional
from celery.signals import worker_init
from gevent import monkey

BROKER_URL: str = os.environ.get("BROKER_URL", "amqp://guest:guest@127.0.0.1:5679//")
TICK_FILE: str = os.environ.get("TICK_FILE", os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_ticks.jsonl"))
STATUS_FILE: str = TICK_FILE + ".status"

app: celery.Celery = celery.Celery("tasks", broker=BROKER_URL)
app.conf.broker_connection_retry_on_startup = True


def _ticker(tag: str, seconds: float) -> None:
    """Sleeps cooperatively in 0.5 s steps and appends each step's real length to TICK_FILE.

    Args:
        tag: Label stored with every record ("hub" or "task").
        seconds: How long to keep ticking.
    """
    start: float = time.monotonic()
    last: float = start
    while last - start < seconds:
        gevent.sleep(0.5)
        now: float = time.monotonic()
        with open(TICK_FILE, "a") as f:
            f.write(json.dumps({"tag": tag, "wall": time.time(), "gap": now - last}) + "\n")
        last = now


@app.task
def tick(seconds: float) -> None:
    """Ticks inside a running task.

    Args:
        seconds: How long to keep ticking.
    """
    _ticker("task", seconds)


@worker_init.connect
def _start_hub_ticker(**kwargs: Any) -> None:
    """Records which ``sleep`` kombu holds and starts a ticking greenlet before the broker connection."""
    status: dict[str, bool] = {
        "time_patched": monkey.is_module_patched("time"),
        "kombu_sleep_is_unpatched": getattr(kombu.utils.functional, "sleep", None)
        is monkey.get_original("time", "sleep"),
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)
    gevent.spawn(_ticker, "hub", 600)
