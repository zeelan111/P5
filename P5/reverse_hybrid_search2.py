import json
import os
import time
import argparse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

# =====================================================
# EDGE EXTRACTION + ROOT DETECTION
# =====================================================
def extract_edges(posts_path, edges_path, roots_path):
    print(f"[INFO] Extracting edges & roots from {posts_path}")
    count_edges = 0
    all_sources = set()
    seen_targets = set()
    all_posts = set()

    with open(posts_path, "r", encoding="utf-8") as infile, \
         open(edges_path, "w", encoding="utf-8") as edge_out:

        for line in infile:
            try:
                post = json.loads(line)
                pid = post.get("post_id")
                if pid is None:
                    continue
                all_posts.add(pid)

                has_edge = False
                for field in ("reply_to", "quotes", "repost_from"):
                    dst = post.get(field)
                    if dst:
                        edge_out.write(json.dumps({"src": pid, "dst": dst}) + "\n")
                        count_edges += 1
                        seen_targets.add(dst)
                        has_edge = True
                if has_edge:
                    all_sources.add(pid)

            except Exception as e:
                print(f"[WARN] Skipped malformed line: {e}")
                continue

    roots = seen_targets - all_sources
    isolated = all_posts - seen_targets - all_sources
    roots |= isolated

    with open(roots_path, "w", encoding="utf-8") as f:
        for r in sorted(roots):
            f.write(json.dumps(r) + "\n")

    print(f"[INFO] Wrote {count_edges:,} edges and {len(roots):,} roots to disk")




# =====================================================
# BUILD OR LOAD REVERSE INDEX
# =====================================================
def build_reverse_index(edges_path, reverse_edges_path):
    print(f"[INFO] Building reverse index from {edges_path}")
    reverse_index = defaultdict(list)

    with open(edges_path, "r", encoding="utf-8") as f:
        for line in f:
            edge = json.loads(line)
            src, dst = edge["src"], edge["dst"]
            reverse_index[dst].append(src)

    print(f"[INFO] Reverse index built with {len(reverse_index):,} target nodes")

    # --- Save immediately for fault tolerance ---
    tmp_path = reverse_edges_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as outfile:
        for target, sources in reverse_index.items():
            outfile.write(json.dumps({"target": target, "sources": sources}) + "\n")

    os.replace(tmp_path, reverse_edges_path)
    print(f"[INFO] Saved reverse edges to {reverse_edges_path}")

    return reverse_index


def load_reverse_index(reverse_edges_path):
    reverse_index = defaultdict(list)
    with open(reverse_edges_path, "r", encoding="utf-8") as infile:
        for line in infile:
            record = json.loads(line)
            reverse_index[record["target"]] = record["sources"]
    print(f"[INFO] Loaded reverse index ({len(reverse_index):,} targets)")
    return reverse_index


# =====================================================
# REVERSE HYBRID TRAVERSAL (BFS by layer)
# =====================================================
def reverse_hybrid_traversal(root_id, reverse_index, max_depth=None):
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


# =====================================================
# MAIN LOGIC
# =====================================================
def main(args):
    start_time = time.time()

    # --- Step 1: Extract edges and roots if not present ---
    if not (os.path.exists(args.edges) and os.path.exists(args.roots_file)):
        extract_edges(args.input, args.edges, args.roots_file)
    else:
        print(f"[INFO] Using existing edges & roots files.")

    # --- Step 2: Build or load reverse index ---
    if not os.path.exists(args.reverse_edges):
        reverse_index = build_reverse_index(args.edges, args.reverse_edges)
    else:
        reverse_index = load_reverse_index(args.reverse_edges)

    # --- Step 3: Prepare outputs ---
    os.makedirs(args.output, exist_ok=True)
    if args.walks_file:
        os.makedirs(os.path.dirname(args.walks_file) or ".", exist_ok=True)

    # --- Step 4: Load roots (streaming) ---
    def load_roots(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)

    # --- Step 5: Resume safety — skip already processed roots ---
    processed_roots = set()
    if os.path.exists(args.walks_file):
        with open(args.walks_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    processed_roots.add(record["start_node"])
                except Exception:
                    continue
        print(f"[INFO] Found {len(processed_roots):,} already processed roots — will skip them.")

    # --- Step 6: Traverse all roots ---
    total_roots = 0
    completed = 0

    with open(args.walks_file, "a", encoding="utf-8") as walks_out:
        for root_id in load_roots(args.roots_file):
            total_roots += 1
            if root_id in processed_roots:
                continue
            try:
                result = reverse_hybrid_traversal(root_id, reverse_index, args.max_depth)

                # Save individual traversal JSON
                out_path = os.path.join(args.output, f"{root_id}.json")
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(result, f)

                # Append to global JSONL
                walks_out.write(json.dumps(result) + "\n")
                completed += 1

                if completed % 1000 == 0:
                    print(f"[PROGRESS] {completed:,} traversals completed...")

            except Exception as e:
                print(f"[ERROR] Traversal failed for root {root_id}: {e}")

    duration = time.time() - start_time
    print(f"[INFO] Finished {completed:,}/{total_roots:,} traversals in {duration:.2f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Two-pass reverse hybrid traversal for Bluesky posts.")
    parser.add_argument("--input", type=str, required=True, help="Input JSONL file with posts")
    parser.add_argument("--edges", type=str, default="edges.jsonl", help="Temporary edge list output file")
    parser.add_argument("--reverse_edges", type=str, default="reverse_edges.jsonl", help="Temporary edge list output file")
    parser.add_argument("--roots_file", type=str, default="roots.jsonl", help="File to store automatically detected roots")
    parser.add_argument("--walks_file", type=str, default="walks.jsonl", help="Aggregated JSONL file for all traversals")
    parser.add_argument("--output", type=str, default="reverse_walks", help="Output directory for traversal results")
    parser.add_argument("--max-depth", type=int, default=None, help="Optional traversal depth limit")
    #parser.add_argument("--workers", type=int, default=4, help="Number of threads for parallel traversal")
    args = parser.parse_args()
    main(args)
