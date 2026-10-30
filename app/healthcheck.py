"""Container health probe: ``python -m app.healthcheck api|worker``.

* ``api``    - GET http://127.0.0.1:8000/ready must return 200.
* ``worker`` - the poll loop's heartbeat file must have been touched recently; a
  wedged poll loop stops touching it even though the process is still alive.

Exit code 0 = healthy, 1 = unhealthy. Uses only the standard library so it adds
nothing to startup time.
"""

import os
import sys
import time
import urllib.request

HEARTBEAT_MAX_AGE_SECONDS = 60


def check_api(port: int = 8000) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=3) as response:
            return response.status == 200
    except OSError:
        return False


def check_worker(path: str) -> bool:
    try:
        return time.time() - os.path.getmtime(path) < HEARTBEAT_MAX_AGE_SECONDS
    except OSError:
        return False


def main(argv: list[str]) -> int:
    target = argv[1] if len(argv) > 1 else ""
    if target == "api":
        healthy = check_api(int(os.getenv("API_PORT", "8000")))
    elif target == "worker":
        healthy = check_worker(os.getenv("WORKER_HEARTBEAT_PATH", "/tmp/fraud-worker-heartbeat"))
    else:
        print("usage: python -m app.healthcheck api|worker", file=sys.stderr)
        return 2
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
