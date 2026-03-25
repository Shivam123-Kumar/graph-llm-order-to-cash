import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query

def safe_print(rows):
    for r in rows: print(str(r).encode('ascii', errors='ignore').decode())

q = "MATCH (n) RETURN labels(n)[0] AS Label, count(n) AS Count"
safe_print(run_query(q))
