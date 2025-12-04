import json, os, time, argparse
from itertools import islice
from concurrent.futures import as_completed, ThreadPoolExecutor
from tempfile import NamedTemporaryFile, mkdtemp
from pathlib import Path
from tempfile import mkdtemp

def iter_files(directory):
    for user in os.scandir(directory):
        if user.is_file() and user.name.endswith(".jsonl"):
            yield user.path

def chunker(iterable, chunksize):
    filenames = iter(iterable)
    while True:
        batch = list(islice(filenames, chunksize))
        if not batch:
            break
        yield batch

def batch_process(batch, threadless_set, worker_id, tmpdir):
    removed = kept = 0
    tmp_out = NamedTemporaryFile("w", delete=False, dir=tmpdir, prefix=f"worker_{worker_id}_", suffix=".jsonl")

    for filepath in batch:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    pid = str(obj.get("post_id"))
                    if pid in threadless_set:
                        removed += 1
                        continue
                    kept += 1
                    tmp_out.write(line if line.endswith("\n") else line + "\n")

        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    tmp_out.close()
    print(f"Worker {worker_id}: Removed {removed:,}, Kept {kept:,}")
    if worker_id % 10 == 0:
        print(f"Processed {worker_id * args.batchsize:,} files so far...")
    return tmp_out.name

def main(args):
    start = time.time()
    file_iterator = iter_files(args.inputpath)
    tmpdir = args.tempdir or mkdtemp(prefix="minimizer_")
    os.makedirs(tmpdir, exist_ok=True)
    print(f"🗂 Using temporary dir: {tmpdir}")

    with open(args.lookup, "r") as f:
        threadless_set = set(line.strip() for line in f)

    tmp_files = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        worker_id = 1
        for batch in chunker(file_iterator, args.batchsize):
            futures.append(executor.submit(batch_process, batch, threadless_set, worker_id, tmpdir))
            worker_id += 1

        for fut in as_completed(futures):
            tmp_files.append(fut.result())

    # Combine worker outputs
    with open(args.output, "w", encoding="utf-8") as out:
        for tmp in tmp_files:
            with open(tmp, "r", encoding="utf-8") as f:
                for line in f:
                    out.write(line)
            os.remove(tmp)

    dt = time.time() - start
    print(f"✅ Done in {dt:.2f} seconds")
    print(f"📁 Output saved to: {args.output}")
    print(f"🧹 Cleaned up temporary files from: {tmpdir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputpath", type=str, required=True, help="Path to input directory")
    parser.add_argument("--output", type=str, required=True, help="Output filepath")
    parser.add_argument("--lookup", type=str, required=True, help="Path to lookup table (LUT)")
    parser.add_argument("--tempdir", type=str, required=True, help="Path to temporary directory")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--batchsize", type=int, default=1000)
    args = parser.parse_args()
    main(args)
