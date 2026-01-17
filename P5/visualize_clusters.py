import json
import argparse
from pathlib import Path
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def count_lines(path):
    with open(path, "r") as f:
        return sum(1 for _ in f)


def main(args):
    args.output_dir.mkdir(parents=True, exist_ok=True)

    records = []

    # ---- Collect cluster counts ----
    for threshold_dir in sorted(p for p in args.cluster_root.iterdir() if p.is_dir()):
        threshold = threshold_dir.name
        cluster_counts = {}

        for cluster_file in threshold_dir.glob("cluster_*.jsonl"):
            cluster_id = int(cluster_file.stem.split("_")[1])
            cluster_counts[cluster_id] = count_lines(cluster_file)

        total = sum(cluster_counts.values())

        for cluster_id, count in cluster_counts.items():
            records.append({
                "threshold": threshold,
                "cluster": cluster_id,
                "count": count,
                "proportion": count / total if total > 0 else 0
            })

    df = pd.DataFrame(records)

    # Ensure consistent cluster ordering
    df["cluster"] = df["cluster"].astype(int)
    df = df.sort_values(["threshold", "cluster"])

    # ---- Stacked bar plot (MAIN FIGURE) ----
    pivot_prop = df.pivot(
        index="threshold",
        columns="cluster",
        values="proportion"
    ).fillna(0)

    plt.figure(figsize=(12, 6))
    pivot_prop.plot(
        kind="bar",
        stacked=True,
        width=0.85,
        ax=plt.gca()
    )

    plt.ylabel("Proportion of walks")
    plt.xlabel("Threshold group")
    plt.title("Cluster Composition Across Thresholds")
    plt.xticks(rotation=90)
    plt.legend(title="Cluster", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()

    plt.savefig(args.output_dir / "cluster_composition_stacked.png", dpi=300)
    plt.close()

    # ---- Heatmap (SUPPLEMENTARY FIGURE) ----
    plt.figure(figsize=(12, 6))
    sns.heatmap(
        pivot_prop,
        annot=True,
        fmt=".2f",
        cmap="viridis",
        cbar_kws={"label": "Proportion"}
    )

    plt.ylabel("Threshold group")
    plt.xlabel("Cluster")
    plt.title("Cluster Proportions Heatmap")
    plt.tight_layout()

    plt.savefig(args.output_dir / "cluster_composition_heatmap.png", dpi=300)
    plt.close()

    # ---- Save raw table for reporting ----
    df.to_csv(args.output_dir / "cluster_composition_table.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster_root", required=True, type=Path, help="Root directory containing per-threshold cluster directories")
    parser.add_argument("--output_dir", required=True, type=Path, help="Directory to save plots")
    args = parser.parse_args()
    main(args)
