from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

print("URI:", URI)
print("USER:", USER)

try:
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    driver.verify_connectivity()
    print("✅ CONNECTION SUCCESSFUL")
except Exception as e:
    print("❌ CONNECTION FAILED")
    print(e)