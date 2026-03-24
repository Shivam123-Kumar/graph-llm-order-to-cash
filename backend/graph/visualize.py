from pyvis.network import Network

def build_graph_html(nodes, edges):
    net = Network(
        height="600px",
        width="100%",
        bgcolor="#111",
        font_color="white"
    )

    # -----------------------------
    # Add nodes (UPDATED)
    # -----------------------------
    for node in nodes:
        color = "red" if node.get("highlight") else "#97C2FC"

        net.add_node(
            node["id"],
            label=node["label"],
            color=color
        )

    # -----------------------------
    # Add edges (UPDATED)
    # -----------------------------
    for edge in edges:
        net.add_edge(
            edge["from"],
            edge["to"],
            label=edge["label"]
        )

    # Layout
    net.repulsion()

    # Save graph
    net.save_graph("graph.html")

    return "graph.html"