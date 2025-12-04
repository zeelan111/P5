import json
import os
import time
import argparse
from itertools import islice
from concurrent.futures import as_completed, ThreadPoolExecutor
from collections import deque, defaultdict


# ------------------------------
# Graph Traversal Functions
# ------------------------------

def get_neighbors(post, lookup_func):
    """
    Given a post object and a lookup function (post_id -> post dict),
    yield all post_ids that the current post points to.
    """
    for field in ("reply_to", "quotes", "repost_from"):
        target = post.get(field)
        if target is not None:
            yield target


def hybrid_traversal(start_post, lookup_func, get_neighbors, max_depth=None):
    """
    Hybrid BFS/DFS-style traversal that:
      - Walks layer by layer (BFS)
      - Builds path layers grouped by depth
      - Tracks total path length, depth, and visited nodes
    Returns:
      dict in the format:
      {
        "start_node": 3,
        "walk_length": 3,
        "walk_depth": 2,
        "walk_path": {"0": [3], "1": [2], "2": [1]}
      }
    """
    start_id = start_post["post_id"]
    visited = set([start_id])
    walk_path = defaultdict(list)

    queue = deque([(start_id, 0)])
    max_found_depth = 0

    while queue:
        node_id, depth = queue.popleft()
        walk_path[depth].append(node_id)
        max_found_depth = max(max_found_depth, depth)

        if max_depth is not None and depth >= max_depth:
            continue

        post = lookup_func(node_id)
        if not post:
            continue

        for nbr in get_neighbors(post, lookup_func):
            if nbr not in visited:
                visited.add(nbr)
                queue.append((nbr, depth + 1))

    return {
        "start_node": start_id,
        "walk_length": len(visited),
        "walk_depth": max_found_depth,
        "walk_path": {str(k): v for k, v in walk_path.items()},
    }


# ------------------------------
# Mock Dataset and Lookup Example
# ------------------------------

# In production, you'll replace this with a database-backed lookup.
MOCK_POSTS = {
    1: {"post_id": 1, "reply_to": None, "quotes": None, "repost_from": None},
    2: {"post_id": 2, "reply_to": 1, "quotes": None, "repost_from": None},
    3: {"post_id": 3, "reply_to": 2, "quotes": None, "repost_from": None},
    4: {"post_id": 4, "reply_to": 3, "quotes": None, "repost_from": None},
    5: {"post_id": 5, "reply_to": 2, "quotes": None, "repost_from": None},
    6: {"post_id": 6, "reply_to": 2, "quotes": None, "repost_from": None}
}

def lookup_func(post_id):
    """Fetch post by ID from an in-memory dataset."""
    return MOCK_POSTS.get(post_id)


# ------------------------------
# Batch Processing with Threads
# ------------------------------

def process_many(start_posts, lookup_func, get_neighbors, out_dir, max_depth=None, max_workers=4):
    """
    Process multiple start posts concurrently and save each traversal
    as a JSON file in the output directory.
    """
    os.makedirs(out_dir, exist_ok=True)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(hybrid_traversal, post, lookup_func, get_neighbors, max_depth): post["post_id"]
            for post in start_posts
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)

                out_path = os.path.join(out_dir, f"{result['start_node']}.json")
                with open(out_path, "w") as f:
                    json.dump(result, f)
            except Exception as e:
                print(f"[ERROR] Failed processing post {futures[future]}: {e}")

    return results

# ------------------------------
# Main Entrypoint
# ------------------------------

def main(args):
    start_time = time.time()

    # In practice, you’d stream these from disk or DB
    start_posts = list(islice(MOCK_POSTS.values(), args.sample))

    print(f"[INFO] Starting traversal of {len(start_posts)} posts with {args.workers} workers...")

    results = process_many(
        start_posts,
        lookup_func=lookup_func,
        get_neighbors=get_neighbors,
        out_dir=args.output,
        max_depth=args.max_depth,
        max_workers=args.workers,
    )

    duration = time.time() - start_time
    print(f"[INFO] Completed {len(results)} traversals in {duration:.2f}s")

    # Print a sample result
    if results:
        print(json.dumps(results[0], indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hybrid BFS/DFS traversal for Bluesky posts.")
    parser.add_argument("--output", type=str, default="output", help="Directory to store traversal results")
    parser.add_argument("--max-depth", type=int, default=None, help="Optional max depth limit for traversal")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads for parallel traversal")
    parser.add_argument("--sample", type=int, default=None, help="Number of sample start nodes to process (demo mode)")
    args = parser.parse_args()
    main(args)
