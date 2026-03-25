import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query

queries = {
    "Check Complete Path": "MATCH (o:SalesOrder)-[:HAS_DELIVERY]->(d:Delivery)-[:BILLED_IN]->(i:Invoice) RETURN count(*)",
    "Check SalesOrder -> Delivery": "MATCH (o:SalesOrder)-[:HAS_DELIVERY]->(d:Delivery) RETURN count(*)",
    "Check Delivery -> Invoice": "MATCH (d:Delivery)-[:BILLED_IN]->(i:Invoice) RETURN count(*)",
    "Sample Delivery -> Invoice": "MATCH (d:Delivery)-[:BILLED_IN]->(i:Invoice) RETURN d.id, i.id LIMIT 5"
}

for name, q in queries.items():
    print(f"\n--- {name} ---")
    try:
        res = run_query(q)
        for r in res:
            print(r)
    except Exception as e:
        print("Error:", e)
