import csv
from collections import deque

def load_graph(path = "directed_graph.csv") -> dict:
    graph = {}

    with open(path) as f:
        for line in f:
            a, b = map(int, line.strip().split(","))
            graph.setdefault(a, []).append(b)
    
    return graph

def bfs(graph, start):
    visited = set([start])
    queue = deque([start])
    order = []

    while queue:
        node = queue.popleft()
        order.append(node)

        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)

    return order

graph = load_graph()
bfs_path = bfs(graph, 1)

def diff(walk_path, graph, node_amount):
    t1 = []

    for x in range(node_amount):
        if x in graph:
            t1.append(x)
        else:
            continue

    return set(t1) - set(walk_path)

print(diff(bfs_path, graph, 1000))
#print(load_graph())