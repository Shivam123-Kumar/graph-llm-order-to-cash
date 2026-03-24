from backend.db.neo4j_driver import get_session

def fetch_graph_data(highlight_ids=None):

    if highlight_ids is None:
        highlight_ids = set()

    query = """
    MATCH (n)-[r]->(m)
    RETURN n, r, m, id(n) as n_id, id(m) as m_id
    LIMIT 100
    """

    try:
        with get_session() as session:
            result = session.run(query)

            nodes = {}
            edges = []

            for record in result:
                n = record["n"]
                m = record["m"]
                r = record["r"]

                # ✅ FIXED IDS (Neo4j internal)
                n_id = str(record["n_id"])
                m_id = str(record["m_id"])

                if n_id is None or m_id is None:
                    continue

                n_label = list(n.labels)[0]
                m_label = list(m.labels)[0]

                # Add node n
                if n_id not in nodes:
                    nodes[n_id] = {
                        "id": n_id,
                        "label": f"{n_label}\n{n_id}",
                        "group": n_label,
                        "highlight": n_id in highlight_ids
                    }

                # Add node m
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

    except Exception as e:
        print("❌ Graph Fetch Error:", e)
        return [], []