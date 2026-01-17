import json
from pathlib import Path

rows = []

for summary_file in Path("summaries").glob("*.json"):
    with open(summary_file) as f:
        stats = json.load(f)

    row = {"threshold": summary_file.stem}
    for metric, vals in stats.items():
        row[f"{metric}_mean"] = vals["mean"]
        row[f"{metric}_max"] = vals["max"]

    rows.append(row)

with open("meta_summary.jsonl", "w") as out:
    for r in rows:
        out.write(json.dumps(r) + "\n")
