from collections import Counter

degree = Counter()

with open("followers.csv") as f:
    for line in f:
        x, y = map(int, line.strip().split(","))
        degree[x] += 1
        degree[y] += 1

top_nodes = set([node for node, _ in degree.most_common(10000)])

with open("followers.csv") as f, open("followers_10k.csv", "w") as out:
    for line in f:
        x, y = map(int, line.strip().split(","))
        if x in top_nodes and y in top_nodes:
            out.write(f"{x},{y}\n")
