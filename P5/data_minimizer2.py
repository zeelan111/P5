import json
import os
import time
import argparse
from itertools import islice
from concurrent.futures import as_completed, ThreadPoolExecutor

# --- helper: iterate over files ---
def iter_files(directory):
    for user in os.scandir(directory):
        if user.is_file() and user.name.endswith(".jsonl"):
            yield user.path

# --- helper: batch generator ---
def chunker(iterable, chunksize):
    filenames = iter(iterable)
    while True:
        batch = list(islice(filenames, chunksize))
        if not batch:
            break
        yield batch

# --- worker ---
# --- function that processes a batch of files ---
def batch_process(batch, threadless_set):
    useful_lines = []   # raw JSON lines
    for filepath in batch:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    obj = json.loads(line)

                    # Skip threadless posts
                    if obj["post_id"] in threadless_set:
                        continue

                    # Keep raw JSON line
                    useful_lines.append(line)

        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    return useful_lines


def main(args):
    start = time.time()
    file_iterator = iter_files(args.inputpath)

    # load lookup table (strings only)
    with open(args.lookup, "r") as f:
        threadless_set = set(line.strip() for line in f)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(batch_process, batch, threadless_set)
            for batch in chunker(file_iterator, args.batchsize)
        ]

        with open(args.output, "w", encoding="utf-8") as out:
            for fut in as_completed(futures):
                lines = fut.result()
                for line in lines:
                    if not line.endswith("\n"):
                        line += "\n"
                    out.write(line)

    dt = time.time() - start
    print("Done in", dt, "seconds")
    print("Output saved to:", args.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputpath', type=str, required=True, help='Path to input directory')
    parser.add_argument('--output', type=str, required=True, help='Output filepath')
    parser.add_argument('--lookup', type=str, required=True, help='Path to lookup table')
    parser.add_argument('--workers', type=int, default=32, help='Number of parallel workers')
    parser.add_argument('--batchsize', type=int, default=1000, help="The size of the batches")
    args = parser.parse_args()
    main(args)
