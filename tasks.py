"""Minimal Celery app for the blocking-sleep reproduction.

``tick`` runs inside the gevent worker and records how long each 0.5 s
``gevent.sleep`` really took. Anything that blocks the gevent hub shows up
as a gap the length of the block.
"""

import json
import os
import tempfile
import time
from typing import Any

import celery
import gevent
import kombu.utils.functional
from celery.signals import worker_ready
from gevent import monkey

RABBITMQ_PORT: int = int(os.environ.get("RABBITMQ_PORT", "5679"))
TICK_FILE: str = os.environ.get("TICK_FILE", os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_ticks.jsonl"))
STATUS_FILE: str = TICK_FILE + ".status"

app: celery.Celery = celery.Celery("tasks", broker=f"amqp://guest:guest@localhost:{RABBITMQ_PORT}//")
app.conf.broker_connection_retry_on_startup = True


@app.task
def tick(seconds: float) -> None:
    """Sleeps cooperatively in 0.5 s steps and appends each step's real length to TICK_FILE.

    Args:
        seconds: How long to keep ticking.
    """
    start: float = time.monotonic()
    last: float = start
    while last - start < seconds:
        gevent.sleep(0.5)
        now: float = time.monotonic()
        with open(TICK_FILE, "a") as f:
            f.write(json.dumps({"wall": time.time(), "gap": now - last}) + "\n")
        last = now


@worker_ready.connect
def _write_status(**kwargs: Any) -> None:
    """Records whether kombu's retry loop holds the unpatched ``time.sleep``."""
    status: dict[str, bool] = {
        "time_patched": monkey.is_module_patched("time"),
        "kombu_sleep_is_unpatched": getattr(kombu.utils.functional, "sleep", None)
        is monkey.get_original("time", "sleep"),
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)
