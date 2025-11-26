import argparse
import os
from itertools import islice
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
import time

# --- helper: iterate over files ---
def iter_files(directory):
    for user in os.scandir(directory):
        if user.is_file() and user.name.endswith(".jsonl"):
            yield user.path

# --- helper: chunking generator ---
def chunker(iterable, chunksize):
    filenames = iter(iterable)
    while True:
        batch = list(islice(filenames, chunksize))
        if not batch:
            break
        yield batch

# --- function that processes a batch of files ---
def batch_process(batch):
    total = 0
    for filepath in batch:
        try:
            with open(filepath, "r", errors="ignore") as json_file:
                for line in json_file:
                    total += 1
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
    return total

# --- main script ---
def main(args):
    start = time.time()

    file_iterator = iter_files(args.inputpath)

    total_users = 0
    total_posts = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for batch in chunker(file_iterator, args.batchsize):
            total_users += len(batch)
            futures.append(executor.submit(batch_process, batch))

        for fut in as_completed(futures):
            total_posts += fut.result()
    
    elapsed = time.time() - start

    with open(f"{args.output}.txt", "w") as f:
        f.write("Total users (files):")
        f.write(str(total_users))
        f.write("\n")
        f.write("Total posts (lines):")
        f.write(str(total_posts))
        f.write("\n")
        f.write("Time spent (seconds):")
        f.write(str(elapsed))
        f.write("\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputpath', type=str, required=True, help='Path to input directory')
    parser.add_argument('--output', type=str, required=True, help='Output filepath')
    parser.add_argument('--workers', type=int, default=32, help='Number of parallel workers')
    parser.add_argument('--batchsize', type=int, default=1000, help="The size of the batches")
    args = parser.parse_args()
    main(args)