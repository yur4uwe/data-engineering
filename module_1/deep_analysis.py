import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# Load data
t2 = pd.read_csv('task/Task_2.csv')
t3 = pd.read_csv('task/Task_3.csv')

# Focus on unique patterns since both files are built from them
unique_rows = pd.concat([t2, t3]).drop_duplicates().reset_index(drop=True)
X = unique_rows.drop('Vclass', axis=1)
y = unique_rows['Vclass']

print(f"Total unique rows: {len(unique_rows)}")
print("Class breakdown in unique rows:")
print(y.value_counts())

# 1. Constant features
variances = X.var()
constant_features = variances[variances == 0].index.tolist()
print(f"\nConstant features (zero variance): {constant_features}")

# 2. High correlation features
corr_matrix = X.corr().abs()
# Filter out constant columns to avoid NaNs in correlation
non_const_X = X.drop(columns=constant_features)
corr_matrix = non_const_X.corr().abs()

upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
print(f"\nHighly redundant features (>0.95 correlation): {to_drop}")

# 3. PCA Analysis
scaler = StandardScaler()
X_scaled = scaler.fit_transform(non_const_X)
pca = PCA()
pca.fit(X_scaled)
evr = pca.explained_variance_ratio_
cumsum_evr = np.cumsum(evr)
n_90 = np.argmax(cumsum_evr >= 0.90) + 1

print(f"\nPCA Analysis:")
print(f"Number of components to explain 90% variance: {n_90} (out of {non_const_X.shape[1]})")
print(f"First 3 components explain: {cumsum_evr[2]:.2%}")

# 4. Feature importance (correlation with class)
feat_importance = unique_rows.drop(columns=constant_features).corr()['Vclass'].abs().sort_values(ascending=False)
print("\nTop features by correlation with Vclass:")
print(feat_importance.head(6)[1:])

# 5. Sparsity (Zero values)
sparsity = (X == 0).mean().mean()
print(f"\nOverall data sparsity (percentage of zeros): {sparsity:.2%}")
