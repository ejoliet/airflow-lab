"""Seed a burst of files into the LocalStack bucket under a scenario prefix.

Records one CSV per burst: results/puts_<scenario>_<burst>.csv (key, put_epoch).
That CSV is the ground truth measure.py joins against.

Run on the host:  python scripts/seed.py --scenario a --n 50
"""

import argparse
import csv
import os
import time
from pathlib import Path

import boto3

BUCKET = os.environ.get("PIPELINE_BUCKET", "pipeline-bucket")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", choices=["a", "b", "c", "d"], required=True)
    p.add_argument("--n", type=int, default=10)
    p.add_argument("--burst", default=None, help="burst id (default: epoch)")
    p.add_argument("--results-dir", default="results")
    args = p.parse_args()

    burst = args.burst or str(int(time.time()))
    s3 = boto3.client(
        "s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    )

    rows = []
    for i in range(args.n):
        key = f"{args.scenario}/{burst}/file_{i:05d}.dat"
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x" * 128)
        rows.append((key, time.time()))

    out = Path(args.results_dir) / f"puts_{args.scenario}_{burst}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["key", "put_epoch"])
        w.writerows(rows)
    print(f"seeded {args.n} objects under {args.scenario}/{burst}/ -> {out}")
    print(burst)


if __name__ == "__main__":
    main()
