import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query
from scripts.load_to_graph import driver, DATABASE, create_deliveries, link_order_delivery, link_delivery_invoice, link_payment_invoice_via_journal

print("Rebuilding nodes...")
with driver.session(database=DATABASE) as session:
    create_deliveries(session)

print("Rebuilding precise path mappings...")
with driver.session(database=DATABASE) as session:
    link_order_delivery(session)
    link_delivery_invoice(session)
    link_payment_invoice_via_journal(session)

print("Validating constraints...")
q1 = "MATCH (o:SalesOrder)-[r1:DELIVERED_TO]->(d:Delivery)-[r2:GENERATES]->(i:Invoice) RETURN o.id as order, d.id as delivery, i.id as invoice LIMIT 10"
q2 = "MATCH (o:SalesOrder)-[:DELIVERED_TO]->(d:Delivery)-[:GENERATES]->(i:Invoice)<-[:PAYS]-(p:Payment) RETURN o.id as order, d.id as delivery, i.id as invoice, p.id as payment LIMIT 10"

res1 = run_query(q1)
res2 = run_query(q2)

with open("fix_out.txt", "w") as f:
    f.write("--- Valid Flow 1 (Order -> Delivery -> Invoice) ---\n")
    for r in res1: f.write(str(r) + "\n")
    f.write("\n--- Valid Flow 2 (Order -> Delivery -> Invoice <- Payment) ---\n")
    for r in res2: f.write(str(r) + "\n")
