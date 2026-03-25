import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.graph_builder import fetch_graph_data

print("Fetching graph data...")
nodes, edges = fetch_graph_data(highlight_ids={"123"})
print(f"Got {len(nodes)} nodes and {len(edges)} edges.")
