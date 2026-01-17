import json
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict

# -------- Core logic -------- #

def compute_metrics(walk):
    walk_path = walk["walk_path"]

    # normalize layers
    num_layers = len(walk_path)
    layers = [walk_path[str(i)] for i in range(num_layers)]
    widths = [len(layer) for layer in layers]

    depth = num_layers - 1
    size = sum(widths)
    max_width = max(widths)

    branching = []
    for i in range(1, len(widths)):
        if widths[i - 1] > 0:
            branching.append(widths[i] / widths[i - 1])

    avg_branching = sum(branching) / len(branching) if branching else 0

    return {
        "start_node": walk["start_node"],
        "walk_length": walk["walk_length"],
        "depth": depth,
        "size": size,
        "max_width": max_width,
        "avg_branching": avg_branching,
        "widths": widths
    }


def process_chunk(lines):
    results = []
    for line in lines:
        walk = json.loads(line)
        results.append(compute_metrics(walk))
    return results


# -------- Entry point -------- #

def main(args):
    with open(args.input, "r") as infile, open(args.output, "w") as outfile:
        if args.workers == 1:
            for line in infile:
                result = compute_metrics(json.loads(line))
                outfile.write(json.dumps(result) + "\n")
        else:
            executor = ProcessPoolExecutor(max_workers=args.workers)
            futures = []

            chunk = []
            for line in infile:
                chunk.append(line)
                if len(chunk) >= args.chunk_size:
                    futures.append(executor.submit(process_chunk, chunk))
                    chunk = []

            if chunk:
                futures.append(executor.submit(process_chunk, chunk))

            for future in as_completed(futures):
                for result in future.result():
                    outfile.write(json.dumps(result) + "\n")

            executor.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--chunk_size", type=int, default=10_000)
    args = parser.parse_args()
    main(args)
