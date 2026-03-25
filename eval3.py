import sys, os, json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query

q = "MATCH (n) RETURN labels(n)[0] AS Label, count(n) AS Count"
with open("db_eval.json", "w", encoding="utf-8") as f:
    json.dump(run_query(q), f)
