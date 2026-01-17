import json
import argparse
from pathlib import Path
from collections import defaultdict
import pandas as pd


def main(args):
    rows = []

    for threshold_dir in sorted(p for p in args.cluster_root.iterdir() if p.is_dir()):
        threshold = threshold_dir.name

        for cluster_file in threshold_dir.glob("cluster_*.jsonl"):
            cluster_id = int(cluster_file.stem.split("_")[1])

            stats = defaultdict(float)
            count = 0

            with open(cluster_file) as f:
                for line in f:
                    obj = json.loads(line)
                    count += 1
                    stats["depth"] += obj["depth"]
                    stats["size"] += obj["size"]
                    stats["max_width"] += obj["max_width"]
                    stats["avg_branching"] += obj["avg_branching"]

            if count == 0:
                continue

            rows.append({
                "threshold": threshold,
                "cluster": cluster_id,
                "count": count,
                "depth_mean": stats["depth"] / count,
                "size_mean": stats["size"] / count,
                "max_width_mean": stats["max_width"] / count,
                "branching_mean": stats["avg_branching"] / count
            })

    df = pd.DataFrame(rows)
    df.to_csv(args.output, index=False)
    print(f"Wrote per-cluster metrics to {args.output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster_root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    main(args)
