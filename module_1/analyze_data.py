import pandas as pd
from scipy import stats

t2 = pd.read_csv("task/Task_2.csv")
t3 = pd.read_csv("task/Task_3.csv")

print(f"Task_2 duplicates: {t2.duplicated().sum()}")
print(f"Task_3 duplicates: {t3.duplicated().sum()}")

# Check if rows in T2 exist in T3
t2_unique = t2.drop_duplicates()
t3_unique = t3.drop_duplicates()
common_unique = pd.merge(t2_unique, t3_unique, how="inner")
print(f"Unique rows in T2: {len(t2_unique)}")
print(f"Unique rows in T3: {len(t3_unique)}")
print(f"Unique rows common to both: {len(common_unique)}")

t2_unique.to_csv("task/t2_unique.csv")
t3_unique.to_csv("task/t3_unique.csv")
common_unique.to_csv("task/common_unique.csv")

# Statistical similarity (KS test)
print(
    "\nKolmogorov-Smirnov test for distributions (p-value > 0.05 means same distribution):"
)
for col in ["V1", "V6", "V7", "V15"]:
    ks_stat, p_val = stats.ks_2samp(t2[col], t3[col])
    print(f"{col}: p-value = {p_val:.4f}")

# Correlation comparison
print("\nCorrelation with Vclass (T2):")
print(t2.corr()["Vclass"].sort_values(ascending=False).head(3))
print("\nCorrelation with Vclass (T3):")
print(t3.corr()["Vclass"].sort_values(ascending=False).head(3))
