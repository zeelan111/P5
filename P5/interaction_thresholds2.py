import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict
import argparse

# ---------------- CONFIG ----------------
THRESHOLDS = [
    {"min_walk_length": 3, "min_walk_depth": 2},
    {"min_walk_length": 4, "min_walk_depth": 3},
]
# ----------------------------------------

def batch_reader(file_path, batch_size):
    batch = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            batch.append(line)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def process_batch(batch_lines, thresholds):
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


def write_results(result_dict, output):
    output.mkdir(exist_ok=True)
    for key, records in result_dict.items():
        if not records:
            continue
        out_path = output / f"{key}.jsonl"
        with open(out_path, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")


def count_lines(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def main(args):
    start_time = time.time()
    total_lines = count_lines(args.input)
    output_dir = Path(args.output)
    print(f"📊 Total lines to process: {total_lines:,}")
    print(f"⚙️  Using {args.workers} workers, batch size {args.batchsize:,}")
    print("=" * 60)

    global_counts = defaultdict(int)
    processed_lines = 0
    batch = []
    futures = []

    with open(args.input, "r", encoding="utf-8") as infile, \
         ProcessPoolExecutor(max_workers=args.workers) as executor:

        for i, line in enumerate(infile, start=1):
            batch.append(line)
            processed_lines += 1

            # Submit batch for processing
            if len(batch) >= args.batchsize:
                futures.append(executor.submit(process_batch, batch, THRESHOLDS))
                batch = []

            # Manual progress report
            if processed_lines % args.progress == 0:
                print(f"➡️  {processed_lines:,} / {total_lines:,} lines processed")

        # Process the remaining batch
        if batch:
            futures.append(executor.submit(process_batch, batch, THRESHOLDS))

        # Collect results
        for idx, future in enumerate(as_completed(futures), start=1):
            result_dict, counts = future.result()
            write_results(result_dict, output_dir)

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
    print(f"🗂 Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Two-pass reverse hybrid traversal for Bluesky posts.")
    parser.add_argument("--input", type=str, required=True, help="Input JSONL file with posts")
    parser.add_argument("--output", type=str, default="thresholds", help="Output directory for traversal results")
    parser.add_argument("--workers", type=int, default=4, help="Number of threads for parallel traversal")
    parser.add_argument("--batchsize", type=int, default=1_000, help="Number of lines per batch")
    parser.add_argument("--progress", type=int, default=10_000, help="How many processed lines between each progress update")
    args = parser.parse_args()
    main(args)
