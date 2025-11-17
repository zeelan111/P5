from collections import Counter
import heapq
import time

st = time.time()

degree = Counter()
k = 10000
heap = []

with open("followers.csv") as f:
    for line in f:
        x, y = map(int, line.strip().split(","))
        degree[x] += 1
        degree[y] += 1

with open("followers.csv") as f:
    for line in f:
        x, y = map(int, line.strip().split(","))
        score = degree[x] + degree[y]  # edge importance

        # Keep largest k edges
        if len(heap) < k:
            heapq.heappush(heap, (score, line))
        else:
            heapq.heappushpop(heap, (score, line))

with open("followers_10k_edges.csv", "w") as out:
    for _, edge in sorted(heap, reverse=True):
        out.write(edge)

print(time.time() - st)