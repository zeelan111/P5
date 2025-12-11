import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict

# ---------------- CONFIG ----------------
INPUT_FILE = "walks.jsonl"
OUTPUT_DIR = Path("filtered_results")
BATCH_SIZE = 100     # lines per worker batch
NUM_WORKERS = 4           # number of CPU cores
PROGRESS_STEP = 1000    # print progress every X lines

THRESHOLDS = [
    {"min_walk_length": 3, "min_walk_depth": 2},
    {"min_walk_length": 4, "min_walk_depth": 3},
]
# ----------------------------------------


def process_batch(batch_lines, thresholds):
    """
    Parse and filter a batch of JSONL lines according to threshold rules.
    Returns a dict of {threshold_key: [records]} and a count summary.
    """
    results = {f"len{t['min_walk_length']}_dep{t['min_walk_depth']}": [] for t in thresholds}
    counts = defaultdict(int)

    for line in batch_lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        wl, wd = obj.get("walk_length", 0), obj.get("walk_depth", 0)
        for t in thresholds:
            if wl >= t["min_walk_length"] and wd >= t["min_walk_depth"]:
                key = f"len{t['min_walk_length']}_dep{t['min_walk_depth']}"
                results[key].append(obj)
                counts[key] += 1

    return results, counts


def write_results(result_dict):
    """Append filtered results to the corresponding JSONL output files."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    for key, records in result_dict.items():
        if not records:
            continue
        out_path = OUTPUT_DIR / f"{key}.jsonl"
        with open(out_path, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")


def count_lines(filename):
    """Get total line count efficiently."""
    with open(filename, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def main():
    start_time = time.time()
    total_lines = count_lines(INPUT_FILE)
    print(f"📊 Total lines to process: {total_lines:,}")
    print(f"⚙️  Using {NUM_WORKERS} workers, batch size {BATCH_SIZE:,}")
    print("=" * 60)

    global_counts = defaultdict(int)
    processed_lines = 0
    batch = []
    futures = []

    with open(INPUT_FILE, "r", encoding="utf-8") as infile, \
         ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:

        for i, line in enumerate(infile, start=1):
            batch.append(line)
            processed_lines += 1

            # Submit batch for processing
            if len(batch) >= BATCH_SIZE:
                futures.append(executor.submit(process_batch, batch, THRESHOLDS))
                batch = []

            # Manual progress report
            if processed_lines % PROGRESS_STEP == 0:
                print(f"➡️  {processed_lines:,} / {total_lines:,} lines processed")

        # Process the remaining batch
        if batch:
            futures.append(executor.submit(process_batch, batch, THRESHOLDS))

        # Collect results
        for idx, future in enumerate(as_completed(futures), start=1):
            result_dict, counts = future.result()
            write_results(result_dict)

            # Merge counts
            for key, val in counts.items():
                global_counts[key] += val

            if idx % 10 == 0:
                print(f"   ... {idx:,} batches completed")

    total_time = time.time() - start_time
    print("\n✅ Done.")
    print(f"🕒 Total processing time: {total_time:.2f} seconds\n")

    # Summary
    print("📈 Summary of filtered results:")
    print("-" * 60)
    for key, count in sorted(global_counts.items()):
        print(f"{key:<20}  {count:>10,} records")
    print("-" * 60)
    print(f"🗂 Output directory: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
