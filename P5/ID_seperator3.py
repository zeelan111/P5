import argparse
import json
import os
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import threading
import time

# Global shared file handle cache
file_cache_lock = threading.Lock()
file_cache = OrderedDict()  # user_id -> open file handle


def get_handle(user_id, output_dir, max_open):
    """
    Thread-safe global LRU file handle cache.
    Ensures at most max_open files exist across ALL threads.
    """
    with file_cache_lock:
        # Already open? Move to end (LRU)
        if user_id in file_cache:
            file_cache.move_to_end(user_id)
            return file_cache[user_id]

        # Evict if needed
        if len(file_cache) >= max_open:
            old_uid, old_fh = file_cache.popitem(last=False)
            old_fh.close()

        path = os.path.join(output_dir, f"{user_id}.jsonl")
        fh = open(path, "a", encoding="utf-8")
        file_cache[user_id] = fh
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

        for fut in futures:
            fut.result()

    # Clean up global cache
    with file_cache_lock:
        for fh in file_cache.values():
            fh.close()

    print("Completed splitting file by user_id (threaded).")
    print(f"[INFO] Finished in {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split users into JSONL files with threads.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--chunk_size", type=int, default=1000)
    parser.add_argument("--max_open", type=int, default=100)
    args = parser.parse_args()
    main(args)
