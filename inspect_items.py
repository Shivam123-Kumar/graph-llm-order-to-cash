import pandas as pd
import glob, os

def inspect_jsonl(path, name, f):
    files = glob.glob(os.path.join(path, "*.jsonl"))
    if not files: return
    df = pd.read_json(files[0], lines=True)
    f.write(f"\n--- {name} ---\n")
    f.write(str(df.columns.tolist()) + "\n")

base = "data"
with open("inspect_out.txt", "w", encoding="utf-8") as f:
    inspect_jsonl(f"{base}/outbound_delivery_items", "Delivery Items", f)
    inspect_jsonl(f"{base}/billing_document_items", "Billing Items", f)
    inspect_jsonl(f"{base}/sales_order_items", "Sales Order Items", f)
    inspect_jsonl(f"{base}/products", "Products", f)
    inspect_jsonl(f"{base}/business_partner_addresses", "Addresses", f)
