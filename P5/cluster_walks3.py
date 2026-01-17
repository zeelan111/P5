import json
import argparse
from pathlib import Path
from collections import defaultdict
import numpy as np
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler
import time


def count_lines(path):
    with open(path, "r") as f:
        return sum(1 for _ in f)


def main(args):
    start_time = time.time()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    metric_files = sorted(args.metrics_dir.glob("*_metrics.jsonl"))
    metric_files = metric_files[args.skip_first_n:]

    for metrics_file in metric_files:
        threshold_name = metrics_file.stem.replace("_metrics", "")
        n_samples = count_lines(metrics_file)

        if n_samples < args.min_samples:
            print(
                f"Skipping {threshold_name}: "
                f"{n_samples} samples (< {args.min_samples})"
            )
            continue

        effective_k = min(args.k, n_samples)

        print(
            f"Clustering {threshold_name}: "
            f"samples={n_samples}, k={effective_k}"
        )

        threshold_out = args.output_dir / threshold_name
        threshold_out.mkdir(parents=True, exist_ok=True)

        scaler = StandardScaler()
        kmeans = MiniBatchKMeans(
            n_clusters=effective_k,
            batch_size=min(args.batch_size, n_samples),
            random_state=0
        )

        buffer = []

        # ---- First pass: fit scaler + kmeans ----
        with open(metrics_file) as f:
            for line in f:
                obj = json.loads(line)
                buffer.append([
                    obj["depth"],
                    obj["size"],
                    obj["max_width"],
                    obj["avg_branching"]
                ])

                if len(buffer) >= kmeans.batch_size:
                    X = np.array(buffer)
                    X = scaler.partial_fit(X).transform(X)
                    kmeans.partial_fit(X)
                    buffer.clear()

        if buffer:
            X = np.array(buffer)
            X = scaler.partial_fit(X).transform(X)
            kmeans.partial_fit(X)
            buffer.clear()

        # ---- Prepare output files ----
        cluster_files = {
            i: open(threshold_out / f"cluster_{i}.jsonl", "w")
            for i in range(effective_k)
        }

        # ---- Second pass: assign clusters ----
        with open(metrics_file) as f:
            for line in f:
                obj = json.loads(line)
                X = scaler.transform([[
                    obj["depth"],
                    obj["size"],
                    obj["max_width"],
                    obj["avg_branching"]
                ]])
                label = int(kmeans.predict(X)[0])
                cluster_files[label].write(json.dumps(obj) + "\n")

        for f in cluster_files.values():
            f.close()

    total_time = time.time() - start_time
    print("\n✅ Done.")
    print(f"🕒 Total processing time: {total_time:.2f} seconds\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics_dir", required=True, type=Path, help="Directory containing *_metrics.jsonl files")
    parser.add_argument("--output_dir", required=True, type=Path, help="Root output directory for per-threshold clusters")
    parser.add_argument("--k", type=int, default=5, help="Number of clusters")
    parser.add_argument("--batch_size", type=int, default=10000)
    parser.add_argument("--skip_first_n", type=int, default=0, help="Skip the first N metric files (sorted)")
    parser.add_argument("--min_samples", type=int, default=3, help="Minimum samples required to cluster a threshold")
    args = parser.parse_args()
    main(args)
