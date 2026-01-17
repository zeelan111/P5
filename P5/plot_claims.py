import pandas as pd
import numpy as np
import json
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--claimsum", required=True, type=Path, help="Summarised_claims.csv file path")
parser.add_argument("--userposts", required=True, type=Path, help="Directory to posts split by user ID")
args = parser.parse_args()

# ----------------------
# LABELING
# ----------------------
df = pd.read_csv(args.claimsum)

conditions = [
    df["p_of_true_claims"] > 0.8,
    df["p_of_true_claims"] < 0.2
]

choices = ["truthful", "misinformer"]

df["label"] = np.select(
    conditions,
    choices,
    default="mixed"
)

df = df[["user_ID", "label"]]

# LUT
user_label_map = dict(zip(df["user_ID"], df["label"]))


posts_dir = Path(args.userposts)
user_data = {}

for user_id, label in user_label_map.items():
    file_path = posts_dir / f"{user_id}.jsonl"

    if not file_path.exists():
        continue

    post_ids = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line)
                post_ids.append(record["post_id"])
            except json.JSONDecodeError as e:
                print(f"Skipping malformed JSON in {file_path}: {e}")
                continue

    user_data[user_id] = {
        "label": label,
        "post_ids": post_ids
    }

with open("user_data.json", "w", encoding="utf-8") as f:
    json.dump(user_data, f)