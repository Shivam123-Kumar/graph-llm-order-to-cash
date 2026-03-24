
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
URI = "bolt://127.0.0.1:7687"
USER = "neo4j"
PASSWORD = "password"
_driver = None  # global driver


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    return _driver


def close_driver():
    global _driver
    if _driver:
        _driver.close()
        _driver = None