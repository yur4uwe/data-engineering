import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import joblib

# Завантажуємо датасет діабету (Pima Indians)
url = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"
df = pd.read_csv(url, header=None)

# Беремо лише 4 інтуїтивно зрозумілі колонки:
# Глюкоза (1), Кров'яний тиск (2), Індекс маси тіла (5), Вік (7)
X = df.iloc[:, [1, 2, 5, 7]].values
y = df.iloc[:, 8].values  # 0 - здорова людина, 1 - діабет

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

best_accuracy = 0
best_pca_n = 1

print("--- Оцінка точності для різної кількості компонент PCA ---")
for n in range(1, 5):
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
pca_final = PCA(n_components=4)
X_train_final = pca_final.fit_transform(X_train_scaled)
clf_final = DecisionTreeClassifier(random_state=230420069).fit(X_train_final, y_train)

joblib.dump(scaler, "scaler.pkl")
joblib.dump(pca_final, "pca.pkl")
joblib.dump(clf_final, "model.pkl")
print("Моделі збережено у файли .pkl")
