import json
import argparse
from itertools import combinations
from pathlib import Path

import pandas as pd
from scipy.stats import ks_2samp


def main(args):
    rows = []

    with open(args.walk_clusters) as f:
        for line in f:
            obj = json.loads(line)
            rows.append({
                "cluster": obj["cluster"],
                "depth": obj["depth"],
                "size": obj["size"],
                "max_width": obj["max_width"],
                "avg_branching": obj["avg_branching"],
            })

    df = pd.DataFrame(rows)

    results = []

    clusters = sorted(df["cluster"].unique())

    for c1, c2 in combinations(clusters, 2):
        d1 = df[df.cluster == c1]
        d2 = df[df.cluster == c2]

        for feature in ["depth", "size", "max_width", "avg_branching"]:
            ks = ks_2samp(d1[feature], d2[feature])
            results.append({
                "cluster_1": c1,
                "cluster_2": c2,
                "feature": feature,
                "ks_statistic": ks.statistic,
                "p_value": ks.pvalue,
                "n_1": len(d1),
                "n_2": len(d2),
            })

    out_df = pd.DataFrame(results)
    out_df.to_csv(args.output, index=False)

    print(f"Wrote cluster separability results to {args.output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--walk_clusters", required=True, type=Path, help="JSONL file with per-walk features and cluster labels")
    parser.add_argument("--output", required=True, type=Path, help="CSV file to write separability results")
    args = parser.parse_args()
    main(args)
