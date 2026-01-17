import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

rows = []
with open("meta_summary.jsonl") as f:
    for line in f:
        rows.append(json.loads(line))

df = pd.DataFrame(rows)

plt.figure(figsize=(10, 6))
sns.lineplot(data=df, x="threshold", y="depth_mean", marker="o")
plt.xticks(rotation=90)
plt.title("Mean Conversation Depth Across Thresholds")
plt.tight_layout()
plt.savefig("mean_depth_by_threshold.png")

plt.figure(figsize=(10, 6))
sns.lineplot(data=df, x="threshold", y="size_mean", marker="o")
plt.xticks(rotation=90)
plt.title("Mean Conversation Size Across Thresholds")
plt.tight_layout()
plt.savefig("mean_size_by_threshold.png")
