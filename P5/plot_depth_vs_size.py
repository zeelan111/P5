import json
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def main(args):
    rows = []
    with open(args.walk_clusters) as f:
        for i, line in enumerate(f):
            if i >= args.max_points:
                break
            obj = json.loads(line)
            rows.append({
                "depth": obj["depth"],
                "size": obj["size"],
                "cluster": obj["cluster"]
            })

    df = pd.DataFrame(rows)

    plt.figure(figsize=(8, 6))

    for cluster_id in sorted(df.cluster.unique()):
        sub = df[df.cluster == cluster_id]
        plt.scatter(
            sub["size"],
            sub["depth"],
            s=32,
            alpha=0.5,
            label=f"Cluster {cluster_id}"
        )

    plt.xlabel("Conversation Size (number of posts)")
    plt.ylabel("Conversation Depth")
    plt.title("Depth vs Size of Conversation Walks by Cluster")
    plt.legend(markerscale=2, frameon=False)
    plt.tight_layout()

    plt.savefig(args.output, dpi=300)
    plt.close()

    print(f"Saved depth–size plot to {args.output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--walk_clusters", required=True, type=Path, help="JSONL file with per-walk features and cluster labels")
    parser.add_argument("--output", required=True, type=Path, help="Output PNG file")
    parser.add_argument("--max_points", type=int, default=200000, help="Optional cap for plotting (for very large files)")
    args = parser.parse_args()
    main(args)
