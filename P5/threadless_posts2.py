import json
import os
import time
import argparse
from itertools import islice
from concurrent.futures import as_completed, ProcessPoolExecutor

# --- helper: iterate over files ---
def iter_files(directory):
    for entry in os.scandir(directory):
        if entry.is_file() and entry.name.endswith(".jsonl"):
            yield entry.path

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
    all_posts = set()
    sources = set()
    targets = set()
    invalid = []  # to store lines with missing/malformed post_id
    total_lines = 0

    for filepath in batch:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    total_lines += 1
                    try:
                        post = json.loads(line)
                    except json.JSONDecodeError:
                        invalid.append({"file": filepath, "reason": "decode_error", "raw": line.strip()})
                        continue

                    pid = post.get("post_id")
                    if pid is None:
                        invalid.append({"file": filepath, "reason": "missing_post_id", "raw": line.strip()})
                        continue

                    # Optional: enforce integer IDs only
                    if not isinstance(pid, int):
                        invalid.append({"file": filepath, "reason": f"invalid_type_{type(pid).__name__}", "raw": line.strip()})
                        continue

                    all_posts.add(pid)

                    # outgoing edges
                    has_interaction = False
                    for key in ("reply_to", "quotes", "repost_from"):
                        target = post.get(key)
                        if target is not None:
                            has_interaction = True
                            targets.add(target)

                    if has_interaction:
                        sources.add(pid)

        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    return all_posts, sources, targets, invalid, total_lines


def main(args):
    st = time.time()
    file_iterator = iter_files(args.inputpath)

    global_all_posts = set()
    global_sources = set()
    global_targets = set()
    global_invalid = []
    global_total_lines = 0

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(batch_process, batch)
            for batch in chunker(file_iterator, args.batchsize)
        ]

        for fut in as_completed(futures):
            all_posts, sources, targets, invalid, total_lines = fut.result()
            global_all_posts.update(all_posts)
            global_sources.update(sources)
            global_targets.update(targets)
            global_invalid.extend(invalid)
            global_total_lines += total_lines

    # compute connected and isolated posts
    connected_posts = global_sources.union(global_targets)
    isolated_posts = global_all_posts.difference(connected_posts)

    print(f"Total JSON lines parsed: {global_total_lines:,}")
    print(f"Total posts: {len(global_all_posts):,}")
    print(f"Posts interacting (sources): {len(global_sources):,}")
    print(f"Posts interacted with (targets): {len(global_targets):,}")
    print(f"Connected (any interaction): {len(connected_posts):,}")
    print(f"Isolated posts (no in/out edges): {len(isolated_posts):,}")
    print(f"Invalid or missing post_id entries: {len(global_invalid):,}")

    # save isolated post IDs
    with open(args.output, "w") as f:
        for pid in isolated_posts:
            f.write(f"{pid}\n")

    # save invalid log
    if global_invalid:
        log_path = os.path.join(os.path.dirname(args.output), "invalid_posts.log")
        with open(log_path, "w", encoding="utf-8") as log:
            for item in global_invalid:
                log.write(json.dumps(item, ensure_ascii=False) + "\n")
        print(f"⚠️ Logged {len(global_invalid)} invalid/malformed entries to: {log_path}")

    print(f"\n✅ Saved isolated post lookup table to: {args.output}")
    print(f"Total isolated posts: {len(isolated_posts):,}")
    print(f"Δt = {time.time() - st:.2f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputpath", type=str, required=True, help="Path to input directory")
    parser.add_argument("--output", type=str, required=True, help="Output filepath")
    parser.add_argument("--workers", type=int, default=32, help="Number of parallel workers")
    parser.add_argument("--batchsize", type=int, default=1000, help="Number of files per batch")
    args = parser.parse_args()
    main(args)
