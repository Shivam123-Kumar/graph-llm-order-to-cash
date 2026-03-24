from neo4j import GraphDatabase
import pandas as pd
import os
# -----------------------------
# CONNECT TO NEO4J
# -----------------------------
import os
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
deliveries = pd.read_csv("processed/deliveries.csv")
invoices = pd.read_csv("processed/invoices.csv")
payments = pd.read_csv("processed/payments.csv")
journal_entries = pd.read_csv("processed/journal_entries.csv")
customers = pd.read_csv("processed/customers.csv")


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
    for i, chunk in enumerate(chunk_df(invoices)):
        print(f"Invoice-Journal batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (i:Invoice {id: row.billingdocument})
            MATCH (j:JournalEntry {id: row.accountingdocument})
            MERGE (i)-[:RECORDED_AS]->(j)
        """, rows=chunk.to_dict("records"))


# def link_payment_invoice(session):
#     df = payments.dropna(subset=["invoicereference"])

#     for i, chunk in enumerate(chunk_df(df)):
#         print(f"Payment-Invoice batch {i+1}")
#         session.run("""
#             UNWIND $rows AS row
#             MATCH (p:Payment {id: row.accountingdocument})
#             MATCH (i:Invoice {id: row.invoicereference})
#             MERGE (p)-[:PAYS]->(i)
#         """, rows=chunk.to_dict("records"))

def link_payment_invoice_via_journal(session):
    print("🔗 Linking Payment → Invoice via Journal")

    session.run("""
        MATCH (p:Payment)-[:RECORDED_AS]->(j:JournalEntry)
        MATCH (i:Invoice)-[:RECORDED_AS]->(j)
        MERGE (p)-[:PAYS]->(i)
    """)


def link_payment_journal(session):
    for i, chunk in enumerate(chunk_df(payments)):
        print(f"Payment-Journal batch {i+1}")
        session.run("""
            UNWIND $rows AS row
            MATCH (p:Payment {id: row.accountingdocument})
            MATCH (j:JournalEntry {id: row.accountingdocument})
            MERGE (p)-[:RECORDED_AS]->(j)
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

    print("✅ Nodes Done!")

    # -------- RELATIONSHIPS --------
    with driver.session(database=DATABASE) as session:
        print("🔗 Creating relationships...")
        link_order_customer(session)
        link_invoice_customer(session)
        link_invoice_journal(session)
        link_payment_journal(session)
        link_payment_invoice_via_journal(session)
    print("🎉 Graph Fully Loaded!")

# def main():
#     with driver.session() as session:
#         print("🔗 Fixing missing relationships...")

#         link_payment_invoice_via_journal(session)

#     print("✅ Fixed!")


if __name__ == "__main__":
    main()