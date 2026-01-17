import json
import pandas as pd
import matplotlib.pyplot as plt

# Load combined data
with open("user_data3.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)

user_agg = df.groupby("user_ID").agg(
    avg_walk_length=("walk_length", "mean"),
    avg_depth=("depth", "mean"),
    label=("label", "first")  # each user has a single label
).reset_index()

import matplotlib.pyplot as plt

color_map = {
    "truthful": "green",
    "mixed": "orange",
    "misinformer": "red"
}

for label, group in user_agg.groupby("label"):
    plt.scatter(
        group["avg_walk_length"],
        group["avg_depth"],
        label=label,
        color=color_map.get(label, "blue"),  # default color just in case
        s=50,  # optional: marker size
        alpha=0.7
    )

plt.xlabel("Average Walk Length")
plt.ylabel("Average Depth")
plt.title("Average Walk Metrics per User")
plt.xlim(-1,300)
plt.ylim(1,9)
plt.legend()
plt.tight_layout()
plt.show()
#plt.savefig("./server_plots/claim_plot_by_user2.png", dpi=300)
plt.close()
