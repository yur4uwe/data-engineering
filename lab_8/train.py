import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import joblib

# 1. Завантаження даних (Breast Cancer Wisconsin)
url = "https://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/wdbc.data"
df = pd.read_csv(url, header=None)

# Відділяємо ID (колонка 0), клас (колонка 1) та характеристики (колонки 2-31)
X = df.iloc[:, 2:].values
y = df.iloc[:, 1].values  # 'M' - malignant (злоякісна), 'B' - benign (доброякісна)

# 2. Розбиття та стандартизація
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

best_accuracy = 0
best_pca_n = 1

print("--- Оцінка точності для різної кількості компонент PCA ---")
for n in range(1, 11):
    # Використання PCA
    pca = PCA(n_components=n)
    X_train_pca = pca.fit_transform(X_train_scaled)
    X_test_pca = pca.transform(X_test_scaled)

    # Класифікація Decision Tree
    clf = DecisionTreeClassifier(random_state=42)
    clf.fit(X_train_pca, y_train)
    y_pred = clf.predict(X_test_pca)

    acc = accuracy_score(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred)

    print(f"Компонент: {n} | Точність: {acc:.4f}")
    if acc > best_accuracy:
        best_accuracy = acc
        best_pca_n = n

print(f"\nНайкраща кількість компонент: {best_pca_n} (Точність: {best_accuracy:.4f})")

# Зберігаємо модель з найкращими параметрами (використовуємо 10 компонент для прикладу)
pca_final = PCA(n_components=10)
X_train_final = pca_final.fit_transform(X_train_scaled)
clf_final = DecisionTreeClassifier(random_state=42).fit(X_train_final, y_train)

joblib.dump(scaler, "scaler.pkl")
joblib.dump(pca_final, "pca.pkl")
joblib.dump(clf_final, "model.pkl")
print("Моделі збережено у файли .pkl")
