import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
from pathlib import Path
from collections import defaultdict
import argparse


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


def build_threshold_key(t):
    return (
        f"len{t.get('min_walk_length','-')}-{t.get('max_walk_length','-')}_"
        f"dep{t.get('min_walk_depth','-')}-{t.get('max_walk_depth','-')}"
    )


def process_batch(batch_lines, thresholds):
    results = {}
    counts = defaultdict(int)

    # Pre-build keys for each threshold combination
    for t in thresholds:
        key = build_threshold_key(t)
        results[key] = []

    for line in batch_lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        wl, wd = obj.get("walk_length", 0), obj.get("walk_depth", 0)

        for t in thresholds:
            min_len = t.get("min_walk_length", float("-inf"))
            max_len = t.get("max_walk_length", float("inf"))
            min_dep = t.get("min_walk_depth", float("-inf"))
            max_dep = t.get("max_walk_depth", float("inf"))

            if (min_len <= wl <= max_len) and (min_dep <= wd <= max_dep):
                key = build_threshold_key(t)
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

    # --- Load thresholds dynamically ---
    if args.thresholds:
        if args.thresholds.endswith(".json"):
            with open(args.thresholds, "r", encoding="utf-8") as tf:
                thresholds = json.load(tf)
        else:
            thresholds = json.loads(args.thresholds)
    else:
        thresholds = [{"min_walk_length": 3, "min_walk_depth": 2}]
    print("🔧 Thresholds loaded:")
    for th in thresholds:
        print(f"{th}")
    print("=" * 60)

    global_counts = defaultdict(int)
    processed_lines = 0
    futures = set()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for batch in batch_reader(args.input, args.batchsize):
            processed_lines += len(batch)

            # Throttle number of queued futures to prevent memory growth
            if len(futures) >= args.workers * 2:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for f in done:
                    result_dict, counts = f.result()
                    write_results(result_dict, output_dir)
                    for k, v in counts.items():
                        global_counts[k] += v

            futures.add(executor.submit(process_batch, batch, thresholds))

            if processed_lines % args.progress == 0:
                print(f"➡️  {processed_lines:,} / {total_lines:,} lines processed")

        # Drain any remaining futures
        for f in as_completed(futures):
            result_dict, counts = f.result()
            write_results(result_dict, output_dir)
            for k, v in counts.items():
                global_counts[k] += v
        print(f"➡️  {total_lines:,} / {total_lines:,} lines processed")

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
    parser.add_argument("--thresholds", type=str, help="JSON string or .json file defining thresholds")
    parser.add_argument("--workers", type=int, default=4, help="Number of threads for parallel traversal")
    parser.add_argument("--batchsize", type=int, default=1_000, help="Number of lines per batch")
    parser.add_argument("--progress", type=int, default=10_000, help="How many processed lines between each progress update")
    args = parser.parse_args()
    main(args)
