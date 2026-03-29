from string import printable

import pandas as pd

df = pd.read_csv("data_cleaned.csv")
print(df.info())

# 1) Which month had the highest average order total?
df["order_date"] = pd.to_datetime(df["order_date"])
orders = (
    df.groupby("order_id").agg({"total": "sum", "order_date": "first"}).reset_index()
)
monthly_avg = pd.Series(
    orders.groupby(orders["order_date"].dt.to_period("M"))["total"].mean()
)
print("Monthly average order total (top 5):", monthly_avg.nlargest(5))
print(
    "Month with highest average order total:", monthly_avg.idxmax(), monthly_avg.max()
)

# 2) Which day of week has the highest average order total?
dow_names = df["order_date"].dt.day_name()
dow_avg = pd.Series(df.groupby(dow_names)["total"].mean()).sort_values(ascending=False)
print("Average total by day of week:", dow_avg)
print("Top day:", dow_avg.idxmax(), dow_avg.max())

# 3) Top 10 SKUs by total revenue
sku_revenue = (
    pd.Series(df.groupby("product_variant_sku")["total"].sum())
    .sort_values(ascending=False)
    .head(10)
)
print("Top 10 SKUs by revenue:", sku_revenue)

# 4) Which Region had the highest average discount percent?
region_disc = pd.Series(df.groupby("Region")["Discount_Percent"].mean()).sort_values(
    ascending=False
)
print("Average discount percent by Region:", region_disc)
print("Top region:", region_disc.idxmax(), region_disc.max())

# 5) What percentage of orders (rows) were canceled?
pct_canceled = (df["status"] == "canceled").mean() * 100
print(f"Canceled rows: {pct_canceled:.2f}%")
print(df["status"].value_counts().head())

# 6) Top 10 customers by total spend
top_customers = (
    pd.Series(df.groupby("customer_id")["total"].sum())
    .sort_values(ascending=False)
    .head(10)
)
print("Top customers by total spend:", top_customers)

# 7) Average order value (AOV) by billing type
aov_by_billing = (
    pd.Series(df.groupby("billing_status")["total"].mean())
    .round(2)
    .sort_values(ascending=False)
)
print("AOV by billing type:", aov_by_billing)

# 8) How many unique customers are there?
print("Unique customers:", df["customer_id"].nunique())

# 9) What is the most common order status?
vc = df["status"].value_counts()
print("Top statuses:", vc.head())
print("Most common:", vc.idxmax(), vc.max())

# 10) Average items per order
qty_per_order = pd.Series(df.groupby("order_id")["qty_ordered"].sum())
print("Average items per order:", qty_per_order.mean().round(2))
print("Median items per order:", qty_per_order.median())

# 11) What fraction of rows had a discount applied?
frac = (df["discount_amount"] > 0).mean() * 100
print(f"Rows with discount: {frac:.2f}%")

# 12) Correlation between discount amount and order total
corr_matrix = pd.DataFrame(df[["discount_amount", "total"]]).corr()
corr = corr_matrix.iloc[0, 1]
print("Correlation (discount_amount vs total):", round(corr, 4))
print(
    "Interpretation: near 0 => weak linear relation; positive/negative show direction"
)

# percentage of unique customers who over 25 years old and have made at least 1 order with a discount
customers = df[(df["age"] > 25) & (df["discount_amount"] > 0)]
print(
    "Percentage of unique customers over 25 with at least 1 discount order: "
    f"{len(customers['customer_id'].unique()) / len(df['customer_id'].unique()) * 100:.2f}%"
)
