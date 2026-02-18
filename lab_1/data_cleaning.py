import sys

import pandas as pd

# Load data
df = pd.read_csv(sys.argv[1] if len(sys.argv) > 1 else "data.csv")

print(df.head())
print(df.info())

# Rename columns
df.rename(
    columns={
        "cust_id": "customer_id",
        "bi_st": "billing_status",
        "ref_num": "reference_number",
        "sku": "product_variant_sku",
    },
    inplace=True,
)

# Convert column types
df = df.astype(
    {
        "qty_ordered": "int64",
        "customer_id": "int64",
        "item_id": "int64",
    }
)

df.to_csv("data_cleaned.csv", index=False)
