import pandas as pd
import glob
import os

# -----------------------------
# 1. Helper Functions FIRST
# -----------------------------

def load_jsonl_folder(path):
    files = glob.glob(os.path.join(path, "*.jsonl"))
    df_list = []

    for file in files:
        print(f"📂 Loading: {file}")
        df = pd.read_json(file, lines=True)
        df_list.append(df)

    if df_list:
        return pd.concat(df_list, ignore_index=True)
    return pd.DataFrame()


def inspect(df, name):
    print(f"\n📊 {name}")
    print("=" * 50)
    if df.empty:
        print("Empty DataFrame")
        return
    print("Columns:", df.columns.tolist())
    print(df.head(2))


def clean_dataframe(df):
    if df.empty:
        return df
    
    df.columns = [col.lower().strip() for col in df.columns]

    for col in df.columns:
        if "id" in col:
            df[col] = df[col].astype(str)

    return df


def save_dataframe(df, name):
    if df.empty:
        return
    os.makedirs("processed", exist_ok=True)
    df.to_csv(f"processed/{name}.csv", index=False)


# -----------------------------
# 2. MAIN FUNCTION AFTER
# -----------------------------

def main():
    print("\n🚀 Starting preprocessing...\n")

    base_path = "data"

    sales_orders = load_jsonl_folder(f"{base_path}/sales_order_headers")
    sales_order_items = load_jsonl_folder(f"{base_path}/sales_order_items")
    deliveries = load_jsonl_folder(f"{base_path}/outbound_delivery_headers")
    delivery_items = load_jsonl_folder(f"{base_path}/outbound_delivery_items")
    invoices = load_jsonl_folder(f"{base_path}/billing_document_headers")
    invoice_items = load_jsonl_folder(f"{base_path}/billing_document_items")
    payments = load_jsonl_folder(f"{base_path}/payments_accounts_receivable")
    journal_entries = load_jsonl_folder(f"{base_path}/journal_entry_items_accounts_receivable")

    customers = load_jsonl_folder(f"{base_path}/business_partners")
    addresses = load_jsonl_folder(f"{base_path}/business_partner_addresses")
    products = load_jsonl_folder(f"{base_path}/products")

    # Clean
    sales_orders = clean_dataframe(sales_orders)
    sales_order_items = clean_dataframe(sales_order_items)
    deliveries = clean_dataframe(deliveries)
    delivery_items = clean_dataframe(delivery_items)
    invoices = clean_dataframe(invoices)
    invoice_items = clean_dataframe(invoice_items)
    payments = clean_dataframe(payments)
    journal_entries = clean_dataframe(journal_entries)
    customers = clean_dataframe(customers)
    addresses = clean_dataframe(addresses)
    products = clean_dataframe(products)

    # Save
    save_dataframe(sales_orders, "sales_orders")
    save_dataframe(sales_order_items, "sales_order_items")
    save_dataframe(deliveries, "deliveries")
    save_dataframe(delivery_items, "delivery_items")
    save_dataframe(invoices, "invoices")
    save_dataframe(invoice_items, "invoice_items")
    save_dataframe(payments, "payments")
    save_dataframe(journal_entries, "journal_entries")
    save_dataframe(customers, "customers")
    save_dataframe(addresses, "addresses")
    save_dataframe(products, "products")

    print("\n✅ Done!")

# -----------------------------
# 3. ENTRY POINT LAST
# -----------------------------

if __name__ == "__main__":
    main()



