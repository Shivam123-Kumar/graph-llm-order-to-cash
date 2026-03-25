import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query

with open("verify_out.txt", "w") as f:
    f.write("--- Complete Path ---\n")
    res = run_query("MATCH (o:SalesOrder)-[:HAS_DELIVERY]->(d:Delivery)-[:BILLED_IN]->(i:Invoice) RETURN count(*) as c")
    f.write(str(res) + "\n")
    
    f.write("--- Deliveries from Order ---\n")
    res = run_query("MATCH (o:SalesOrder)-[:HAS_DELIVERY]->(d:Delivery) RETURN d.id LIMIT 5")
    f.write(str(res) + "\n")
    
    f.write("--- Deliveries to Invoice ---\n")
    res = run_query("MATCH (d:Delivery)-[:BILLED_IN]->(i:Invoice) RETURN d.id LIMIT 5")
    f.write(str(res) + "\n")
