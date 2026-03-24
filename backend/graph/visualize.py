from pyvis.network import Network

def build_graph_html(nodes, edges):

    net = Network(
        height="600px",
        width="100%",
        bgcolor="#111",
        font_color="white"
    )

    # -----------------------------
    # ADD NODES (FIXED)
    # -----------------------------
    for node in nodes:

        color = "red" if node.get("highlight") else "#97C2FC"

        net.add_node(
            node["id"],
            label=node["label"],
            color=color,   # 🔥 THIS IS KEY
            title=node["label"]
        )

    # -----------------------------
    # ADD EDGES
    # -----------------------------
    for edge in edges:
        net.add_edge(
            edge["from"],
            edge["to"],
            label=edge["label"]
        )

    net.repulsion()

    net.save_graph("graph.html")
    return "graph.html"