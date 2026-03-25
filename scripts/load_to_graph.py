import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# -----------------------------
# LOAD DATA
# -----------------------------
sales_orders = pd.read_csv("processed/sales_orders.csv")
sales_order_items = pd.read_csv("processed/sales_order_items.csv")
deliveries = pd.read_csv("processed/deliveries.csv")
delivery_items = pd.read_csv("processed/delivery_items.csv")
invoices = pd.read_csv("processed/invoices.csv")
invoice_items = pd.read_csv("processed/invoice_items.csv")
payments = pd.read_csv("processed/payments.csv")
journal_entries = pd.read_csv("processed/journal_entries.csv")
customers = pd.read_csv("processed/customers.csv")
products = pd.read_csv("processed/products.csv")
addresses = pd.read_csv("processed/addresses.csv")

# -----------------------------
# CHUNK FUNCTION
# -----------------------------
def chunk_df(df, size=20):
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]

# -----------------------------
# CREATE NODES
# -----------------------------
def create_sales_orders(session):
    for i, chunk in enumerate(chunk_df(sales_orders)):
        print(f"SalesOrder batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (o:SalesOrder {id: row.salesorder})
            SET o.type = row.salesordertype,
                o.amount = row.totalnetamount
        """, rows=chunk.to_dict("records"))

def create_customers(session):
    for i, chunk in enumerate(chunk_df(customers)):
        print(f"Customer batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (c:Customer {id: row.businesspartner})
        """, rows=chunk.to_dict("records"))

def create_invoices(session):
    df = invoices.dropna(subset=["billingdocument"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Invoice batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (i:Invoice {id: row.billingdocument})
            SET i.amount = row.totalnetamount
        """, rows=chunk.to_dict("records"))

def create_payments(session):
    for i, chunk in enumerate(chunk_df(payments)):
        print(f"Payment batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (p:Payment {id: row.accountingdocument})
        """, rows=chunk.to_dict("records"))

def create_journal_entries(session):
    for i, chunk in enumerate(chunk_df(journal_entries)):
        print(f"Journal batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (j:JournalEntry {id: row.accountingdocument})
        """, rows=chunk.to_dict("records"))

def create_deliveries(session):
    df = deliveries.dropna(subset=["deliverydocument"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Delivery batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (d:Delivery {id: row.deliverydocument})
            SET d.shippingPoint = row.shippingpoint
        """, rows=chunk.to_dict("records"))

def create_products(session):
    df = products.dropna(subset=["product"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Product batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (p:Product {id: row.product})
            SET p.group = row.productgroup
        """, rows=chunk.to_dict("records"))

def create_addresses(session):
    df = addresses.dropna(subset=["addressuuid"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Address batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MERGE (a:Address {id: row.addressuuid})
            SET a.city = row.cityname, a.country = row.country
        """, rows=chunk.to_dict("records"))

# -----------------------------
# CREATE RELATIONSHIPS
# -----------------------------
def link_order_customer(session):
    df = sales_orders.dropna(subset=["soldtoparty"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Order-Customer batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (o:SalesOrder {id: row.salesorder})
            MATCH (c:Customer {id: row.soldtoparty})
            MERGE (o)-[:PLACED_BY]->(c)
        """, rows=chunk.to_dict("records"))

def link_invoice_customer(session):
    df = invoices.dropna(subset=["soldtoparty"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Invoice-Customer batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (i:Invoice {id: row.billingdocument})
            MATCH (c:Customer {id: row.soldtoparty})
            MERGE (i)-[:BILLED_TO]->(c)
        """, rows=chunk.to_dict("records"))

def link_invoice_journal(session):
    df = invoices.dropna(subset=["billingdocument", "accountingdocument"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Invoice-Journal batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (i:Invoice {id: row.billingdocument})
            MATCH (j:JournalEntry {id: row.accountingdocument})
            MERGE (i)-[:RECORDED_AS]->(j)
        """, rows=chunk.to_dict("records"))

def link_payment_journal(session):
    df = payments.dropna(subset=["accountingdocument"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Payment-Journal batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (p:Payment {id: row.accountingdocument})
            MATCH (j:JournalEntry {id: row.accountingdocument})
            MERGE (p)-[:RECORDED_AS]->(j)
        """, rows=chunk.to_dict("records"))

def link_payment_invoice_via_journal(session):
    print("🔗 Linking Payment → Invoice via Journal")
    session.run("""
        MATCH (p:Payment)-[:RECORDED_AS]->(j:JournalEntry)
        MATCH (i:Invoice)-[:RECORDED_AS]->(j)
        MERGE (p)-[:PAYS]->(i)
    """)

def link_customer_address(session):
    df = addresses.dropna(subset=["businesspartner", "addressuuid"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Customer-Address batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (c:Customer {id: row.businesspartner})
            MATCH (a:Address {id: row.addressuuid})
            MERGE (c)-[:HAS_ADDRESS]->(a)
        """, rows=chunk.to_dict("records"))

def link_order_product(session):
    df = sales_order_items.dropna(subset=["salesorder", "material"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Order-Product batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (o:SalesOrder {id: row.salesorder})
            MATCH (p:Product {id: row.material})
            MERGE (o)-[:CONTAINS]->(p)
        """, rows=chunk.to_dict("records"))

def link_order_delivery(session):
    df = delivery_items.dropna(subset=["referencesddocument", "deliverydocument"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Order-Delivery batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (o:SalesOrder {id: row.referencesddocument})
            MATCH (d:Delivery {id: row.deliverydocument})
            MERGE (o)-[:DELIVERED_TO]->(d)
        """, rows=chunk.to_dict("records"))

def link_delivery_invoice(session):
    df = invoice_items.dropna(subset=["referencesddocument", "billingdocument"])
    for i, chunk in enumerate(chunk_df(df)):
        print(f"Delivery-Invoice batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (d:Delivery {id: row.referencesddocument})
            MATCH (i:Invoice {id: row.billingdocument})
            MERGE (d)-[:GENERATES]->(i)
        """, rows=chunk.to_dict("records"))

# -----------------------------
# MAIN
# -----------------------------
def main():
    # -------- NODES --------
    with driver.session(database=DATABASE) as session:
        print("🚀 Creating nodes...")
        create_sales_orders(session)
        create_customers(session)
        create_invoices(session)
        create_payments(session)
        create_journal_entries(session)
        create_deliveries(session)
        create_products(session)
        create_addresses(session)

    print("✅ Nodes Done!")

    # -------- RELATIONSHIPS --------
    with driver.session(database=DATABASE) as session:
        print("🔗 Creating relationships...")
        link_order_customer(session)
        link_invoice_customer(session)
        link_invoice_journal(session)
        link_payment_journal(session)
        link_payment_invoice_via_journal(session)
        link_customer_address(session)
        link_order_product(session)
        link_order_delivery(session)
        link_delivery_invoice(session)
    print("🎉 Graph Fully Loaded!")

if __name__ == "__main__":
    main()