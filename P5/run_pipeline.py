import subprocess
from pathlib import Path

THRESHOLD_DIR = Path("thresholds")
METRICS_DIR = Path("metrics")
SUMMARY_DIR = Path("summaries")

METRICS_DIR.mkdir(exist_ok=True)
SUMMARY_DIR.mkdir(exist_ok=True)

files = sorted(THRESHOLD_DIR.glob("*.jsonl"))[2:]  # skip first two

for f in files:
    name = f.stem
    metrics_out = METRICS_DIR / f"{name}_metrics.jsonl"
    summary_out = SUMMARY_DIR / f"{name}_summary.json"

    print(f"Processing {name}")

    subprocess.run([
        "python3", "compute_walk_metrics.py",
        "--input", str(f),
        "--output", str(metrics_out),
        "--workers", "8",
        "--chunk_size", "10000"
    ], check=True)

    subprocess.run([
        "python3", "aggregate_metrics.py",
        "--input", str(metrics_out),
        "--output", str(summary_out)
    ], check=True)
