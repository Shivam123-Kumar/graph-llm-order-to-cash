import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query

queries = {
    "node_counts": "MATCH (n) RETURN labels(n)[0] AS Node, count(n) AS Count",
    "edge_counts": "MATCH (n)-[r]->(m) RETURN type(r) AS Edge, count(r) AS Count",
    "isolated_nodes": "MATCH (n) WHERE NOT (n)--() RETURN labels(n)[0] AS Node, count(n) AS Count",
    "broken_invoices": "MATCH (i:Invoice) WHERE NOT (i)<-[:PAYS]-(:Payment) RETURN count(i) AS UnpaidInvoices",
    "delivery_nodes": "MATCH (n:Delivery) RETURN count(n) AS Count",
    "order_nodes": "MATCH (n:Order) RETURN count(n) AS Count",
    "schema_sample": "MATCH (n)-[r]->(m) RETURN labels(n)[0] AS source, type(r) AS edge, labels(m)[0] AS target LIMIT 10"
}

for name, q in queries.items():
    print(f"\n--- {name} ---")
    try:
        res = run_query(q)
        for r in res:
            print(r)
    except Exception as e:
        print("Error:", e)
