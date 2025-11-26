import json
import os
import time
import argparse
from itertools import islice
from concurrent.futures import as_completed, ProcessPoolExecutor, ThreadPoolExecutor

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
    interactions = {}
    threadlessposts = []

    for filepath in batch:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    result = json.loads(line)

                    if result["reply_to"]:
                        interactions.setdefault(result["thread_root"], []).append(result["post_id"])
                    elif result["repost_from"]:
                        interactions.setdefault(result["repost_from"], []).append(result["post_id"])
                    elif result["quotes"]:
                        interactions.setdefault(result["quotes"], []).append(result["post_id"])
                    else:
                        threadlessposts.append(result["post_id"])

        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    return interactions, threadlessposts

# --- main script ---
def main(args):
    st = time.time()
    file_iterator = iter_files(args.inputpath)
    global_interactions = {}
    global_threadless = []

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(batch_process, batch)
                for batch in chunker(file_iterator, args.batchsize)]

        for fut in as_completed(futures):
            postdict, threadlessposts = fut.result()

            # merge dictionaries
            for root, posts in postdict.items():
                global_interactions.setdefault(root, []).extend(posts)

            # merge threadless list
            global_threadless.extend(threadlessposts)

    total_interaction_posts = sum(len(posts) for posts in global_interactions.values())

    print("Unique roots with interactions:", len(global_interactions))
    print("Number of posts that interacted:", total_interaction_posts)
    print("Threadless posts:", len(global_threadless))

    DT = time.time() - st
    print("Delta:", DT, "\n")

    # save threadless list to file
    output_path = args.output
    with open(output_path, "w") as f:
        for post_id in global_threadless:
            f.write(str(post_id))
            f.write("\n")

    print("Saved threadless lookup table to:", output_path)
    print("Total threadless posts:", len(global_threadless))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputpath', type=str, required=True, help='Path to input directory')
    parser.add_argument('--output', type=str, required=True, help='Output filepath')
    parser.add_argument('--workers', type=int, default=32, help='Number of parallel workers')
    parser.add_argument('--batchsize', type=int, default=1000, help="The size of the batches")
    args = parser.parse_args()
    main(args)