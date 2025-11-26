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
def batch_process(batch, sets):
    usefull_posts = []
    for filepath in batch:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    result = json.loads(line)
                    if result["post_id"] in sets:
                        continue
                    else:
                        usefull_posts.append(result)

        except Exception as e:
            print(f"Error reading {filepath}: {e}")
    
    return usefull_posts

# --- main script ---
def main(args):
    st = time.time()
    file_iterator = iter_files(args.inputpath)
    global_interactions = []

    output_path = args.output
    lookup = args.lookup
    with open(lookup, "r") as f:
        threadless_set = set(int(line.strip()) for line in f)

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(batch_process, batch, threadless_set)
                for batch in chunker(file_iterator, args.batchsize)]
        
        with open(output_path, "a", encoding="utf-8") as f:
            for fut in as_completed(futures):
                for post in fut.result():
                    f.write(json.dumps(post, ensure_ascii=False))
                    f.write("\n")
                

    DT = time.time() - st
    print("Delta:", DT, "\n")
    
    # save all posts with interactions to file
    """
    with open(output_path, "w", encoding="utf-8") as f:
        for post in global_interactions:
            f.write(json.dumps(post, ensure_ascii=False))
            f.write("\n")
    """
    print("Saved all posts to:", output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputpath', type=str, required=True, help='Path to input directory')
    parser.add_argument('--output', type=str, required=True, help='Output filepath')
    parser.add_argument('--lookup', type=str, required=True, help='Path to lookup table')
    parser.add_argument('--workers', type=int, default=32, help='Number of parallel workers')
    parser.add_argument('--batchsize', type=int, default=1000, help="The size of the batches")
    args = parser.parse_args()
    main(args)