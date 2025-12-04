from pathlib import Path

def count_jsonl_files_and_objects(folder_path: str):
    folder = Path(folder_path)
    jsonl_files = list(folder.glob("*.jsonl"))  # Get all .jsonl files
    
    file_count = len(jsonl_files)
    object_count = 0

    for file_path in jsonl_files:
        # Count lines (each line = one JSON object)
        with file_path.open("r", encoding="utf-8") as f:
            for _ in f:
                object_count += 1

    print(f"📄 Total JSONL files: {file_count}")
    print(f"🧩 Total JSON objects: {object_count}")

# Example usage
if __name__ == "__main__":
    count_jsonl_files_and_objects("./posts")
