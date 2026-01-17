import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

epsilon = 1e-300
logp_cap = 300.0

def main(args):
    args.output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.separability)

    for feature in sorted(df.feature.unique()):
        sub = df[df.feature == feature]

        # Pivot KS statistic
        ks_mat = sub.pivot(
            index="cluster_1",
            columns="cluster_2",
            values="ks_statistic"
        )

        # Pivot p-values (log scale)
        p_mat = sub.pivot(
            index="cluster_1",
            columns="cluster_2",
            values="p_value"
        )

        p_mat = p_mat.fillna(1.0)
        p_mat = p_mat.clip(lower=epsilon)
        logp_mat = -np.log10(p_mat)
        logp_mat = logp_mat.clip(upper=logp_cap)

        # --- KS heatmap ---
        plt.figure(figsize=(6, 5))
        sns.heatmap(
            ks_mat,
            annot=True,
            fmt=".2f",
            cmap="viridis",
            cbar_kws={"label": "KS statistic"}
        )
        plt.title(f"KS Separability (Feature: {feature})")
        plt.xlabel("Cluster")
        plt.ylabel("Cluster")
        plt.tight_layout()
        plt.savefig(args.output_dir / f"ks_heatmap_{feature}.png", dpi=300)
        plt.close()

        # --- p-value heatmap ---
        plt.figure(figsize=(6, 5))
        sns.heatmap(
            logp_mat,
            annot=True,
            fmt=".1f",
            cmap="magma",
            cbar_kws={"label": "-log10(p-value)"}
        )
        plt.title(f"Statistical Significance (Feature: {feature})")
        plt.xlabel("Cluster")
        plt.ylabel("Cluster")
        plt.tight_layout()
        plt.savefig(args.output_dir / f"pvalue_heatmap_{feature}.png", dpi=300)
        plt.close()

        # Save pivot tables
        ks_mat.to_csv(args.output_dir / f"ks_table_{feature}.csv")
        logp_mat.to_csv(args.output_dir / f"logp_table_{feature}.csv")

    print(f"Saved KS separability heatmaps and tables to {args.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--separability", required=True, type=Path, help="cluster_separability.csv file")
    parser.add_argument("--output_dir", required=True, type=Path, help="Directory to save plots and tables")
    args = parser.parse_args()
    main(args)
