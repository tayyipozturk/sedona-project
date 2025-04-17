import osmnx as ox

location = "Ankara, Turkey"
graph = ox.graph_from_place(location, network_type="drive")

nodes, edges = ox.graph_to_gdfs(graph, nodes=True, edges=True)

nodes_filepath = "data/nodes_ankara.csv"
edges_filepath = "data/edges_ankara.csv"
nodes.to_csv(nodes_filepath, index=False)
edges.to_csv(edges_filepath, index=False)

print(f"Nodes saved to {nodes_filepath}")
print(f"Edges saved to {edges_filepath}")

