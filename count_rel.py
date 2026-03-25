import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from backend.graph.query_executor import run_query

print("DELIVERED_TO:", run_query("MATCH ()-[r:DELIVERED_TO]->() RETURN count(r)")[0]["count(r)"])
print("GENERATES:", run_query("MATCH ()-[r:GENERATES]->() RETURN count(r)")[0]["count(r)"])
print("PAYS:", run_query("MATCH ()-[r:PAYS]->() RETURN count(r)")[0]["count(r)"])
