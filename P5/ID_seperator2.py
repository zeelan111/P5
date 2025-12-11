import argparse
import json
import os
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import threading
import time

# Thread-local storage for per-thread file caches
local = threading.local()


def get_handle(user_id, output_dir, max_open):
    """
    Returns a writable file handle for a given user_id.
    Uses an LRU cache per thread.
    """
    if not hasattr(local, "open_files"):
        local.open_files = OrderedDict()

    open_files = local.open_files

    # If present, move to end (LRU)
    if user_id in open_files:
        open_files.move_to_end(user_id)
        return open_files[user_id]

    # Evict if needed
    if len(open_files) >= max_open:
        old_id, old_fh = open_files.popitem(last=False)
        old_fh.close()

    path = os.path.join(output_dir, f"{user_id}.jsonl")
    fh = open(path, "a", encoding="utf-8")

    open_files[user_id] = fh
    return fh


def process_chunk(chunk, output_dir, max_open):
    """
    Worker function that processes a chunk of JSON lines.
    """
    for line in chunk:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            user_id = obj["user_id"]
        except Exception:
            continue  # ignore malformed lines

        fh = get_handle(user_id, output_dir, max_open)
        fh.write(line)


def main(args):
    start_time = time.time()
    os.makedirs(args.output_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        chunk = []

        with open(args.input, "r", encoding="utf-8") as f:
            for line in f:
                chunk.append(line)
                if len(chunk) >= args.chunk_size:
                    futures.append(
                        executor.submit(process_chunk, chunk, args.output_dir, args.max_open)
                    )
                    chunk = []

        if chunk:
            futures.append(
                executor.submit(process_chunk, chunk, args.output_dir, args.max_open)
            )

        # Wait for all threads to finish
        for fut in futures:
            fut.result()

    if hasattr(local, "open_files"):
        for fh in local.open_files.values():
            fh.close()

    print("Completed splitting file by user_id (threaded).")
    duration = time.time() - start_time
    print(f"[INFO] Finished in {duration:.2f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a JSONL file into per-user JSONL files using ThreadPoolExecutor.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--chunk_size", type=int, default=1000)
    parser.add_argument("--max_open", type=int, default=100)
    args = parser.parse_args()
    main(args)
