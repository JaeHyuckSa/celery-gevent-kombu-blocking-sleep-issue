"""Celery gevent worker blocks the whole gevent hub while reconnecting to the broker.

``celery worker -P gevent`` imports the celery package and only then calls
``gevent.monkey.patch_all()``. While gevent patches ``threading`` it scans
``gc.get_objects()`` with ``isinstance()``, which evaluates celery's lazy compat
proxies (their ``__class__`` is a property) and imports kombu before ``time`` is
patched. ``kombu.utils.functional`` keeps the original ``time.sleep``, so
``retry_over_time()`` freezes every greenlet, running tasks included, for
2, 4, 6, ... seconds while the broker is unreachable.

Usage:
    uv run python main.py [--workaround] [--outage SECONDS]
"""

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger: logging.Logger = logging.getLogger(__name__)

RABBITMQ_CONTAINER: str = "celery-kombu-sleep-rabbitmq"
RABBITMQ_IMAGE: str = "rabbitmq:3.13.7"
RABBITMQ_PORT: int = int(os.environ.get("RABBITMQ_PORT", "5679"))
TICK_FILE: str = os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_ticks.jsonl")
STATUS_FILE: str = TICK_FILE + ".status"
WORKER_LOG: str = os.path.join(tempfile.gettempdir(), "_celery_kombu_sleep_worker.log")
HERE: str = os.path.dirname(os.path.abspath(__file__))


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    """Runs a command and returns the result.

    Args:
        cmd: Command and arguments.
        check: If True, raises on non-zero exit code.

    Returns:
        subprocess.CompletedProcess[str]: The completed process result.
    """
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def start_rabbitmq() -> None:
    """Starts a small plaintext RabbitMQ container and waits until it accepts connections.

    Raises:
        RuntimeError: If the container fails to start or become ready.
    """
    _run(["docker", "rm", "-fv", RABBITMQ_CONTAINER], check=False)
    # On macOS Docker Desktop the entrypoint's Erlang cookie creation sometimes races
    # into "Error when reading /var/lib/rabbitmq/.erlang.cookie: eacces". Pre-create it.
    init_cmd: str = (
        "printf %s celery-kombu-sleep-cookie > /var/lib/rabbitmq/.erlang.cookie "
        "&& chown rabbitmq:rabbitmq /var/lib/rabbitmq/.erlang.cookie "
        "&& chmod 400 /var/lib/rabbitmq/.erlang.cookie "
        "&& exec docker-entrypoint.sh rabbitmq-server"
    )
    result: subprocess.CompletedProcess[str] = _run(
        ["docker", "run", "-d", "--name", RABBITMQ_CONTAINER, "--memory", "256m",
         "-p", f"127.0.0.1:{RABBITMQ_PORT}:5672", "--entrypoint", "sh", RABBITMQ_IMAGE, "-c", init_cmd],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start RabbitMQ: {result.stderr.strip()}")
    logger.info("Waiting for RabbitMQ...")
    for _ in range(90):
        state: str = _run(["docker", "inspect", "-f", "{{.State.Status}}", RABBITMQ_CONTAINER], check=False).stdout
        if state.strip() != "running":
            logs: str = _run(["docker", "logs", "--tail", "20", RABBITMQ_CONTAINER], check=False).stdout
            raise RuntimeError(f"RabbitMQ container exited. Tail logs:\n{logs}")
        ping: subprocess.CompletedProcess[str] = _run(
            ["docker", "exec", RABBITMQ_CONTAINER, "rabbitmq-diagnostics", "-q", "check_port_connectivity"],
            check=False,
        )
        if ping.returncode == 0:
            logger.info("RabbitMQ ready")
            return
        time.sleep(1)
    raise RuntimeError("RabbitMQ failed to start")


def stop_rabbitmq() -> None:
    """Stops and removes the RabbitMQ container."""
    _run(["docker", "rm", "-fv", RABBITMQ_CONTAINER], check=False)


def start_worker(workaround: bool) -> subprocess.Popen[bytes]:
    """Starts a Celery gevent worker as a subprocess.

    Args:
        workaround: If True, let gevent patch everything before celery is imported.

    Returns:
        subprocess.Popen[bytes]: The worker process handle.
    """
    launcher: list[str] = ["-m", "gevent.monkey", "--module", "celery"] if workaround else ["-m", "celery"]
    env: dict[str, str] = {**os.environ, "RABBITMQ_PORT": str(RABBITMQ_PORT), "TICK_FILE": TICK_FILE}
    with open(WORKER_LOG, "w") as log:
        return subprocess.Popen(
            [sys.executable, *launcher, "-A", "tasks", "worker", "-P", "gevent", "-c", "4", "--loglevel=warning"],
            env=env,
            cwd=HERE,
            stdout=log,
            stderr=log,
        )


def stop_worker(proc: subprocess.Popen[bytes]) -> None:
    """Stops a worker process, falling back to SIGKILL.

    Args:
        proc: The worker subprocess to stop.
    """
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def wait_for_file(path: str, proc: subprocess.Popen[bytes], timeout: float = 60) -> None:
    """Waits until ``path`` exists and is non-empty.

    Args:
        path: File to wait for.
        proc: Worker process; its exit aborts the wait.
        timeout: Seconds before giving up.

    Raises:
        RuntimeError: If the worker exits or the file does not appear in time.
    """
    deadline: float = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Worker exited with {proc.returncode}, see {WORKER_LOG}")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {path}, see {WORKER_LOG}")


def read_ticks() -> list[dict[str, Any]]:
    """Reads the tick records written by the ``tick`` task.

    Returns:
        list[dict[str, Any]]: One ``{"wall", "gap"}`` record per 0.5 s step.
    """
    with open(TICK_FILE) as f:
        return [json.loads(line) for line in f if line.strip()]


def main() -> None:
    """Runs the reproduction: start broker and worker, stop the broker under a running task, measure."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workaround", action="store_true",
                        help="start the worker with `python -m gevent.monkey --module celery`")
    parser.add_argument("--outage", type=float, default=16, help="seconds to keep the broker down (default 16)")
    args: argparse.Namespace = parser.parse_args()

    import celery
    import gevent
    import kombu

    mode: str = "python -m gevent.monkey --module celery" if args.workaround else "python -m celery"
    logger.info("Celery gevent worker: blocking sleep during broker reconnect")
    logger.info(f"Celery {celery.__version__}, kombu {kombu.__version__}, gevent {gevent.__version__}, "
                f"Python {sys.version.split()[0]}")
    logger.info(f"Worker: {mode} -A tasks worker -P gevent")

    for path in (TICK_FILE, STATUS_FILE):
        if os.path.exists(path):
            os.remove(path)

    start_rabbitmq()
    try:
        proc: subprocess.Popen[bytes] = start_worker(args.workaround)
        try:
            wait_for_file(STATUS_FILE, proc)
            with open(STATUS_FILE) as f:
                status: dict[str, bool] = json.load(f)
            logger.info(f"Worker ready: time patched={status['time_patched']}, "
                        f"kombu.utils.functional.sleep is unpatched time.sleep={status['kombu_sleep_is_unpatched']}")

            from tasks import tick

            tick.delay(args.outage + 30)
            wait_for_file(TICK_FILE, proc)
            time.sleep(3)

            logger.info(f"Stopping broker for {args.outage:.0f}s while the task is running...")
            outage_start: float = time.time()
            _run(["docker", "stop", "-t", "5", RABBITMQ_CONTAINER])
            time.sleep(args.outage)
            outage_end: float = time.time()
            if proc.poll() is not None:
                raise RuntimeError(f"Worker exited during the outage, see {WORKER_LOG}")
        finally:
            stop_worker(proc)
    finally:
        stop_rabbitmq()

    ticks: list[dict[str, Any]] = [t for t in read_ticks() if outage_start < t["wall"] <= outage_end]
    gaps: list[float] = [t["gap"] for t in ticks]
    silent_at_end: float = outage_end - max((t["wall"] for t in ticks), default=outage_start)
    with open(WORKER_LOG) as f:
        retries: list[str] = re.findall(r"Trying again in ([\d.]+) seconds", f.read())
    logger.info(f"Worker reconnect attempts during outage: waits {', '.join(retries) or 'none'} s")
    logger.info(f"Task tick gaps > 1.5s: {', '.join(f'{g:.1f}' for g in gaps if g > 1.5) or 'none'} "
                f"(task silent for {silent_at_end:.1f}s when the outage window closed)")

    worst: float = max([*gaps, silent_at_end])
    if worst > 1.5:
        logger.info(f"Max task tick gap during broker outage: {worst:.1f}s (expected ~0.5s)  BLOCKING SLEEP CONFIRMED")
    else:
        logger.info(f"Max task tick gap during broker outage: {worst:.1f}s (expected ~0.5s)  NOT REPRODUCED")


if __name__ == "__main__":
    main()
