"""Celery gevent worker blocks the whole hub while retrying the broker connection.

Usage:
    uv run python main.py [--workaround]
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import time

TICK_FILE: str = os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_ticks.jsonl")
WORKER_LOG: str = os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_worker.log")
WINDOW: float = 16.0


def main() -> None:
    """Starts a gevent worker against an unreachable broker and measures the ticker's gaps."""
    workaround: bool = "--workaround" in sys.argv[1:]
    launcher: list[str] = ["-m", "gevent.monkey", "--module", "celery"] if workaround else ["-m", "celery"]

    import celery
    import gevent
    import kombu

    print(f"Celery {celery.__version__}, kombu {kombu.__version__}, gevent {gevent.__version__}, "
          f"Python {sys.version.split()[0]}")
    print(f"Worker: python {' '.join(launcher)} -A tasks worker -P gevent")

    if os.path.exists(TICK_FILE):
        os.remove(TICK_FILE)
    with open(WORKER_LOG, "w") as log:
        proc: subprocess.Popen[bytes] = subprocess.Popen(
            [sys.executable, *launcher, "-A", "tasks", "worker", "-P", "gevent", "--loglevel=warning"],
            env={**os.environ, "TICK_FILE": TICK_FILE},
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=log,
            stderr=log,
        )
    try:
        while not (os.path.exists(TICK_FILE) and os.path.getsize(TICK_FILE)):
            if proc.poll() is not None:
                raise RuntimeError(f"Worker exited, see {WORKER_LOG}")
            time.sleep(0.2)
        start: float = time.time()
        time.sleep(WINDOW)
        end: float = time.time()
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()

    with open(TICK_FILE) as f:
        status, *ticks = [json.loads(line) for line in f]
    with open(WORKER_LOG) as f:
        retries: list[str] = re.findall(r"Trying again in ([\d.]+) seconds", f.read())
    gaps: list[float] = [t["gap"] for t in ticks if start < t["wall"] <= end]
    silent: float = end - max((t["wall"] for t in ticks if t["wall"] <= end), default=start)
    worst: float = max([*gaps, silent])

    print(f"kombu.utils.functional.sleep is unpatched time.sleep: {status['kombu_sleep_is_unpatched']}")
    print(f"Reconnect waits: {', '.join(retries)} s")
    print(f"Tick gaps > 1.5s: {', '.join(f'{g:.1f}' for g in gaps if g > 1.5) or 'none'}")
    verdict: str = "BLOCKING SLEEP CONFIRMED" if worst > 1.5 else "NOT REPRODUCED"
    print(f"Max tick gap: {worst:.1f}s (expected ~0.5s)  {verdict}")


if __name__ == "__main__":
    main()
