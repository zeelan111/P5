import json
import os
from collections import OrderedDict

input_path = "min_data.jsonl"
output_dir = "sep_files"
os.makedirs(output_dir, exist_ok=True)

MAX_OPEN = 1000  # Adjust depending on OS limit

open_files = OrderedDict()  # user_id -> file handle

def get_handle(uid):
    # If already open, move to end (most recently used)
    if uid in open_files:
        open_files.move_to_end(uid)
        return open_files[uid]
    
    # Need to open new file
    if len(open_files) >= MAX_OPEN:
        old_uid, old_fh = open_files.popitem(last=False)
        old_fh.close()
    
    path = os.path.join(output_dir, f"{uid}.jsonl")
    fh = open(path, "a", encoding="utf-8")
    open_files[uid] = fh
    return fh

with open(input_path, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        
        obj = json.loads(line)
        user_id = obj["user_id"]
        
        fh = get_handle(user_id)
        fh.write(line)

# Close any remaining
for fh in open_files.values():
    fh.close()
