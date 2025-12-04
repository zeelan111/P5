#!/usr/bin/env python3
import json
import os
import time
import argparse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed


# =========================================================
# 1️⃣  PASS ONE — EDGE EXTRACTION
# =========================================================
def extract_edges(posts_path, edges_path):
    """
    Read a JSONL dataset of posts and write an edge list to a .jsonl file.
    Each edge: {"src": post_id, "dst": target_post_id}
    """
    print(f"[INFO] Extracting edges from {posts_path} → {edges_path}")
    count = 0

    with open(posts_path, "r", encoding="utf-8") as infile, open(edges_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            try:
                post = json.loads(line)
                src = post.get("post_id")
                for field in ("reply_to", "quotes", "repost_from"):
                    dst = post.get(field)
                    if dst:
                        outfile.write(json.dumps({"src": src, "dst": dst}) + "\n")
                        count += 1
            except Exception as e:
                print(f"[WARN] Skipped line due to error: {e}")
                continue

    print(f"[INFO] Wrote {count:,} edges to {edges_path}")


# =========================================================
# 2️⃣  BUILD REVERSE INDEX FROM EDGE LIST
# =========================================================
def build_reverse_index(edges_path):
    """
    Build a reverse index (target → list of sources) from edge list JSONL file.
    Returns a dict: {target_id: [source_ids, ...]}
    Suitable for moderate-scale datasets. For very large ones, stream with SQLite.
    """
    print(f"[INFO] Building reverse index from {edges_path}")
    reverse_index = defaultdict(list)

    with open(edges_path, "r", encoding="utf-8") as f:
        for line in f:
            edge = json.loads(line)
            src, dst = edge["src"], edge["dst"]
            reverse_index[dst].append(src)

    print(f"[INFO] Reverse index built with {len(reverse_index):,} target nodes")
    return reverse_index


# =========================================================
# 3️⃣  HYBRID REVERSE TRAVERSAL
# =========================================================
def reverse_hybrid_traversal(root_id, reverse_index, max_depth=None):
    """
    BFS-by-layer traversal from a root node through the reverse edges.
    Returns:
    {
      "start_node": root_id,
      "walk_length": <# of visited nodes>,
      "walk_depth": <max layer>,
      "walk_path": {"0": [root_id], "1": [...], ...}
    }
    """
    visited = set([root_id])
    walk_path = defaultdict(list)
    queue = deque([(root_id, 0)])
    max_found_depth = 0

    while queue:
        node_id, depth = queue.popleft()
        walk_path[depth].append(node_id)
        max_found_depth = max(max_found_depth, depth)

        if max_depth is not None and depth >= max_depth:
            continue

        for nbr in reverse_index.get(node_id, []):
            if nbr not in visited:
                visited.add(nbr)
                queue.append((nbr, depth + 1))

    return {
        "start_node": root_id,
        "walk_length": len(visited),
        "walk_depth": max_found_depth,
        "walk_path": {str(k): v for k, v in walk_path.items()},
    }


# =========================================================
# 4️⃣  PARALLEL EXECUTION FOR MULTIPLE ROOTS
# =========================================================
def process_many_roots(root_ids, reverse_index, out_dir, max_depth=None, max_workers=4):
    os.makedirs(out_dir, exist_ok=True)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(reverse_hybrid_traversal, root_id, reverse_index, max_depth): root_id
            for root_id in root_ids
        }

        for future in as_completed(futures):
            root_id = futures[future]
            try:
                result = future.result()
                results.append(result)
                out_path = os.path.join(out_dir, f"{root_id}.json")
                with open(out_path, "w") as f:
                    json.dump(result, f)
            except Exception as e:
                print(f"[ERROR] Traversal failed for root {root_id}: {e}")

    return results


# =========================================================
# 5️⃣  CLI + MAIN LOGIC
# =========================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Two-pass reverse hybrid traversal for Bluesky posts.")
    parser.add_argument("--posts", type=str, required=True, help="Input JSONL file with posts")
    parser.add_argument("--edges", type=str, default="edges.jsonl", help="Temporary edge list output file")
    parser.add_argument("--output", type=str, default="reverse_walks", help="Output directory for traversal results")
    parser.add_argument("--roots", type=int, nargs="+", required=True, help="List of root post IDs to start from")
    parser.add_argument("--max-depth", type=int, default=None, help="Optional traversal depth limit")
    parser.add_argument("--workers", type=int, default=4, help="Number of threads for parallel traversal")
    return parser.parse_args()


def main():
    args = parse_args()
    start_time = time.time()

    # --- Pass 1: Build edge list ---
    if not os.path.exists(args.edges):
        extract_edges(args.posts, args.edges)
    else:
        print(f"[INFO] Using existing edge file: {args.edges}")

    # --- Pass 2: Build reverse index ---
    reverse_index = build_reverse_index(args.edges)

    # --- Reverse traversal(s) ---
    print(f"[INFO] Starting reverse traversals from {len(args.roots)} roots...")
    results = process_many_roots(args.roots, reverse_index, args.output,
                                 max_depth=args.max_depth, max_workers=args.workers)

    print(f"[INFO] Completed {len(results)} traversals in {time.time() - start_time:.2f}s")
    if results:
        print(json.dumps(results[0], indent=2))


if __name__ == "__main__":
    main()
