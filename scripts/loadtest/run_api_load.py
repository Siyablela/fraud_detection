"""Find the API's sustainable throughput and its latency at each load level.

For each endpoint, runs scripts/loadtest/api.js (k6, in Docker, on the Compose
network) at increasing fixed request rates and stops at the first rate the API
cannot sustain. A step passes when:

* fewer than 1% of requests fail,
* k6 achieved at least 95% of the offered rate (it was not starved of capacity), and
* p99 latency is within --p99-slo-ms.

The highest passing rate is the sustainable throughput; the table shows the
latency percentiles that went with it.

    python scripts/loadtest/run_api_load.py --env-file .env.example
    python scripts/loadtest/run_api_load.py --endpoints get --rates 100 200 400 800 --duration 60s

The stack must be running with the per-caller rate limit raised, or every step
measures 429s:  RATE_LIMIT_REQUESTS=100000000 docker compose up --build --wait
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from smoke_test import load_env  # noqa: E402  (scripts/ is not a package)

DEFAULT_RATES = [50, 100, 200, 400, 800, 1600]


def seconds(duration: str) -> float:
    """k6 duration ("30s", "2m", "1m30s") in seconds."""
    total, number = 0.0, ""
    for char in duration:
        if char.isdigit() or char == ".":
            number += char
        else:
            total += float(number) * {"h": 3600, "m": 60, "s": 1}[char]
            number = ""
    return total


def run_step(args, env: dict[str, str], endpoint: str, rate: int, out_dir: Path) -> dict:
    """Run k6 at one fixed rate for one endpoint and return its key numbers."""
    summary = out_dir / f"{endpoint}-{rate}.json"
    command = [
        "docker", "run", "--rm", "--network", args.network,
        "-v", f"{HERE}:/scripts:ro", "-v", f"{out_dir}:/out",
        "-e", f"CLIENT_SECRET={env['FRAUD_ANALYST_CLIENT_SECRET']}", "-e", f"API_URL={args.api_url}",
        "-e", f"ENDPOINT={endpoint}", "-e", f"RATE={rate}", "-e", f"DURATION={args.duration}",
        args.k6_image, "run", "--quiet", "--no-color", f"--summary-export=/out/{summary.name}", "/scripts/api.js",
    ]  # fmt: skip
    completed = subprocess.run(command, capture_output=True, text=True)
    if not summary.exists():
        sys.exit(f"k6 failed for {endpoint} @ {rate}/s:\n{completed.stdout}\n{completed.stderr}")
    metrics = json.loads(summary.read_text(encoding="utf-8"))["metrics"]
    duration = metrics["http_req_duration{scenario:load}"]
    return {
        # Completed requests over the step's length; setup() requests are excluded.
        "achieved": metrics["http_reqs{scenario:load}"]["count"] / seconds(args.duration),
        "failed": metrics["http_req_failed{scenario:load}"]["value"],
        "dropped": metrics.get("dropped_iterations", {}).get("count", 0),
        "p50": duration["med"],
        "p95": duration["p(95)"],
        "p99": duration["p(99)"],
        "max": duration["max"],
    }


def verdict(result: dict, rate: int, p99_slo_ms: float) -> str:
    """'ok' if the step was sustained within the error and latency limits, else the reason."""
    if result["failed"] > 0.01:
        return f"errors {result['failed']:.1%}"
    if result["achieved"] < 0.95 * rate:
        return "rate not sustained"
    if result["p99"] > p99_slo_ms:
        return f"p99 > {p99_slo_ms:.0f} ms"
    return "ok"


def main() -> int:
    """Step the rate up per endpoint until it fails; print a table and the sustainable rates."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--network", default="fraud-detection_default", help="Compose network (<project>_default)")
    parser.add_argument("--api-url", default="http://api:8000", help="API address as seen from the Compose network")
    parser.add_argument("--endpoints", nargs="+", default=["get", "list", "evaluate", "mixed"])
    parser.add_argument("--rates", nargs="+", type=int, default=DEFAULT_RATES, help="Requests per second to try")
    parser.add_argument("--duration", default="30s", help="Length of each step")
    parser.add_argument("--p99-slo-ms", type=float, default=500.0)
    parser.add_argument("--k6-image", default="grafana/k6:latest")
    args = parser.parse_args()
    env = load_env(args.env_file)

    header = ("endpoint", "offered", "achieved", "p50 ms", "p95 ms", "p99 ms", "max ms")
    print("{:<9} {:>8} {:>9} {:>7} {:>7} {:>7} {:>7}  result".format(*header))
    best: dict[str, tuple[int, dict]] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for endpoint in args.endpoints:
            for rate in args.rates:
                result = run_step(args, env, endpoint, rate, Path(tmp))
                outcome = verdict(result, rate, args.p99_slo_ms)
                print(
                    f"{endpoint:<9} {rate:>8} {result['achieved']:>9.1f} {result['p50']:>7.1f} {result['p95']:>7.1f} "
                    f"{result['p99']:>7.1f} {result['max']:>7.0f}  {outcome}",
                    flush=True,
                )
                if outcome != "ok":
                    break
                best[endpoint] = (rate, result)

    print("\nSustainable throughput (highest passing step):")
    for endpoint in args.endpoints:
        if endpoint in best:
            rate, result = best[endpoint]
            print(f"  {endpoint:<9} {rate:>5} req/s  (p50 {result['p50']:.1f} ms, p99 {result['p99']:.1f} ms)")
        else:
            print(f"  {endpoint:<9} below {args.rates[0]} req/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
