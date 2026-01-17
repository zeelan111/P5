import json

with open("user_data.json", "r", encoding="utf-8") as f:
    user_data = json.load(f)

# convert string keys back to int if needed
user_data = {int(uid): v for uid, v in user_data.items()}

post_metrics = {}

with open("walks_metrics.jsonl", "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        post_id = record.get("start_node")
        if post_id is None:
            continue

        post_metrics[post_id] = {
            "walk_length": record["walk_length"],
            "depth": record["depth"]
        }

plot_records = []

for user_id, data in user_data.items():
    label = data["label"]

    for post_id in data["post_ids"]:
        metrics = post_metrics.get(post_id)
        if metrics is None:
            continue  # post has no walk metrics

        plot_records.append({
            "user_ID": user_id,
            "post_id": post_id,
            "walk_length": metrics["walk_length"],
            "depth": metrics["depth"],
            "label": label
        })

with open("user_data2.json", "w", encoding="utf-8") as f:
    json.dump(plot_records, f)