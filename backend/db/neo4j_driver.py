from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE")

_driver = None  # Global driver


def get_driver():
    global _driver
    if _driver is None:
        try:
            _driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
            # 🔥 Test connection once
            _driver.verify_connectivity()
            print("✅ Neo4j Connected Successfully")
        except Exception as e:
            print("❌ Neo4j Connection Failed:", e)
            raise e
    return _driver


def get_session():
    driver = get_driver()
    return driver.session(database=DATABASE)


def close_driver():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        print("🔒 Neo4j connection closed")