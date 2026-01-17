import json
import argparse
from collections import defaultdict
from pathlib import Path

def main(args):
    counts = defaultdict(int)
    sums = defaultdict(float)
    maxes = defaultdict(float)

    with open(args.input) as f:
        for line in f:
            obj = json.loads(line)
            for k, v in obj.items():
                if isinstance(v, (int, float)):
                    counts[k] += 1
                    sums[k] += v
                    maxes[k] = max(maxes[k], v)

    summary = {
        k: {
            "mean": sums[k] / counts[k],
            "max": maxes[k],
            "count": counts[k]
        }
        for k in counts
    }

    with open(args.output, "w") as out:
        json.dump(summary, out, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    main(args)
