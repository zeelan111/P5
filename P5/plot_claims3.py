import json
import pandas as pd
import matplotlib.pyplot as plt

# Load combined data
with open("user_data3.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)

# Basic sanity check
required_cols = {"walk_length", "depth", "label"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# Plot: walk_length vs depth, colored by label
for label, group in df.groupby("label"):
    plt.scatter(
        group["walk_length"],
        group["depth"],
        label=label,
        alpha=0.6
    )

plt.xlabel("Walk Length")
plt.ylabel("Depth")
plt.title("Walk Length vs Depth by User Label")
plt.xlim(-1,1000)
plt.ylim(-1,40)
plt.legend()
plt.tight_layout()
plt.show()