import csv
import random

# Parameters
num_nodes = 1000
num_edges = 5000  # Tune density as desired

edges = set()

# Generate directed edges
while len(edges) < num_edges:
    a, b = random.sample(range(num_nodes), 2)
    edge = (a, b)  # keep direction
    edges.add(edge)

# Save to CSV
with open("directed_graph.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["src", "dst"])
    writer.writerows(edges)

print(f"✅ Generated directed_graph.csv with {num_nodes} nodes and {len(edges)} directed edges.")
