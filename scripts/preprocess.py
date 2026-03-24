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
    deliveries = load_jsonl_folder(f"{base_path}/outbound_delivery_headers")
    invoices = load_jsonl_folder(f"{base_path}/billing_document_headers")
    payments = load_jsonl_folder(f"{base_path}/payments_accounts_receivable")
    journal_entries = load_jsonl_folder(f"{base_path}/journal_entry_items_accounts_receivable")

    customers = load_jsonl_folder(f"{base_path}/business_partners")
    addresses = load_jsonl_folder(f"{base_path}/business_partner_addresses")
    products = load_jsonl_folder(f"{base_path}/products")

    # Inspect
    inspect(sales_orders, "Sales Orders")
    inspect(deliveries, "Deliveries")
    inspect(invoices, "Invoices")
    inspect(payments, "Payments")
    inspect(journal_entries, "Journal Entries")

    # Clean
    # Clean
    sales_orders = clean_dataframe(sales_orders)
    deliveries = clean_dataframe(deliveries)
    invoices = clean_dataframe(invoices)
    payments = clean_dataframe(payments)
    journal_entries = clean_dataframe(journal_entries)
    customers = clean_dataframe(customers)

    # Save
    save_dataframe(sales_orders, "sales_orders")
    save_dataframe(deliveries, "deliveries")
    save_dataframe(invoices, "invoices")
    save_dataframe(payments, "payments")
    save_dataframe(journal_entries, "journal_entries")
    save_dataframe(customers, "customers")

    print("\n✅ Done!")


# -----------------------------
# 3. ENTRY POINT LAST
# -----------------------------

if __name__ == "__main__":
    main()



