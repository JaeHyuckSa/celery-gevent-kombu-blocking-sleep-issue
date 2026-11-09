"""Minimal Celery app: a greenlet that records how long each gevent.sleep(0.5) really takes."""

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

TICK_FILE: str = os.environ.get("TICK_FILE", os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_ticks.jsonl"))

# Nothing listens on port 1, so the worker stays in its connection retry loop.
app: celery.Celery = celery.Celery("tasks", broker="amqp://guest:guest@127.0.0.1:1//")
app.conf.broker_connection_retry_on_startup = True


def _ticker() -> None:
    """Appends the real length of every 0.5 s gevent.sleep to TICK_FILE."""
    last: float = time.monotonic()
    while True:
        gevent.sleep(0.5)
        now: float = time.monotonic()
        with open(TICK_FILE, "a") as f:
            f.write(json.dumps({"wall": time.time(), "gap": now - last}) + "\n")
        last = now


@worker_init.connect
def _start(**kwargs: Any) -> None:
    """Records which sleep kombu holds, then starts the ticker before the broker connection."""
    unpatched: bool = getattr(kombu.utils.functional, "sleep", None) is monkey.get_original("time", "sleep")
    with open(TICK_FILE, "w") as f:
        f.write(json.dumps({"kombu_sleep_is_unpatched": unpatched}) + "\n")
    gevent.spawn(_ticker)
