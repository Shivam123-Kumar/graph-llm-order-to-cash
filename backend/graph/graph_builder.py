from backend.db.neo4j_driver import get_driver

def fetch_graph_data(highlight_ids=None):
    driver = get_driver()

    if highlight_ids is None:
        highlight_ids = set()

    query = """
    MATCH (n)-[r]->(m)
    RETURN n, r, m LIMIT 100
    """

    with driver.session() as session:
        result = session.run(query)

        nodes = {}
        edges = []

        for record in result:
            n = record["n"]
            m = record["m"]
            r = record["r"]

            # Extract IDs safely
            n_id = n.get("id")
            m_id = m.get("id")

            n_label = list(n.labels)[0]
            m_label = list(m.labels)[0]

            # Add nodes (avoid duplicates)
            if n_id not in nodes:
                nodes[n_id] = {
                    "id": n_id,
                    "label": f"{n_label}\n{n_id}",
                    "group": n_label,
                    "highlight": n_id in highlight_ids
                }

            if m_id not in nodes:
                nodes[m_id] = {
                    "id": m_id,
                    "label": f"{m_label}\n{m_id}",
                    "group": m_label,
                    "highlight": m_id in highlight_ids
                }

            # Add edge
            edges.append({
                "from": n_id,
                "to": m_id,
                "label": r.type
            })

        return list(nodes.values()), edges