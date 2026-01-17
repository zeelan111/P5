import json
import argparse
import pickle
from pathlib import Path
import time
import numpy as np
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler


# ---------- Utilities ----------

def save_checkpoint(path, obj):
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def load_checkpoint(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def extract_features(obj):
    return [
        obj["depth"],
        obj["size"],
        obj["max_width"],
        obj["avg_branching"]
    ]


# ---------- Main Pipeline ----------

def main(args):
    start_time = time.time()
    ckpt_dir = args.checkpoint_dir
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    state_path = ckpt_dir / "state.pkl"
    fit_offset_path = ckpt_dir / "fit_offset.txt"
    label_offset_path = ckpt_dir / "label_offset.txt"

    # ============================================================
    # PASS 1 — Fit scaler + kmeans incrementally
    # ============================================================

    if state_path.exists():
        print("Resuming model from checkpoint")
        scaler, kmeans = load_checkpoint(state_path)
        fit_offset = int(fit_offset_path.read_text())
    else:
        print("Starting fresh model fit")
        scaler = StandardScaler()
        kmeans = MiniBatchKMeans(
            n_clusters=args.k,
            batch_size=args.batch_size,
            random_state=0
        )
        fit_offset = 0

    batch = []

    with open(args.input, "r") as f:
        f.seek(fit_offset)

        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break

            obj = json.loads(line)
            batch.append(extract_features(obj))

            if len(batch) >= args.batch_size:
                X = np.array(batch, dtype=np.float32)
                X = scaler.partial_fit(X).transform(X)
                kmeans.partial_fit(X)

                batch.clear()

                save_checkpoint(state_path, (scaler, kmeans))
                fit_offset_path.write_text(str(f.tell()))

        # Flush remaining
        if batch:
            X = np.array(batch, dtype=np.float32)
            X = scaler.partial_fit(X).transform(X)
            kmeans.partial_fit(X)
            batch.clear()

            save_checkpoint(state_path, (scaler, kmeans))
            fit_offset_path.write_text(str(f.tell()))

    print("PASS 1 complete")

    # ============================================================
    # PASS 2 — Assign labels + write output (streaming)
    # ============================================================

    if label_offset_path.exists():
        label_offset = int(label_offset_path.read_text())
        print(f"Resuming labeling from offset {label_offset}")
        out_mode = "a"
    else:
        label_offset = 0
        out_mode = "w"
        print("Starting labeling from beginning")

    with open(args.input, "r") as fin, open(args.output, out_mode) as fout:
        fin.seek(label_offset)

        while True:
            pos = fin.tell()
            line = fin.readline()
            if not line:
                break

            obj = json.loads(line)
            X = np.array([extract_features(obj)], dtype=np.float32)
            X = scaler.transform(X)

            label = int(kmeans.predict(X)[0])
            obj["cluster"] = label

            fout.write(json.dumps(obj) + "\n")
            label_offset_path.write_text(str(fin.tell()))

    print("PASS 2 complete")
    end_time = time.time() - start_time 
    print(f"Clustering finished successfully in {end_time:.2f} seconds")


# ---------- Entry Point ----------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--checkpoint_dir", required=True, type=Path)
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--batch_size", type=int, default=10000)
    args = parser.parse_args()
    main(args)
