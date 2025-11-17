import csv

dfs_path = [1]

def startup(graph_path = "directed_graph.csv"):
    edges = []

    with open(f"{graph_path}") as f:
        for lines in f:
            edge = lines.strip().split(",")
            edge[0], edge[1] = int(edge[0]), int(edge[1])
            edges.append(edge)
    
    return edges

