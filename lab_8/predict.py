import joblib
import numpy as np

# Завантажуємо збережені артефакти
try:
    scaler = joblib.load("scaler.pkl")
    pca = joblib.load("pca.pkl")
    model = joblib.load("model.pkl")
except FileNotFoundError:
    print("Помилка: Файли моделі не знайдено. Спочатку запустіть train.py.")
    exit()

print("=== Модель класифікації пухлин (Breast Cancer) ===")
print("Очікується введення 30 числових характеристик через пробіл.")
print("Приклад тестового рядка (можете скопіювати):")
print(
    "17.99 10.38 122.8 1001.0 0.1184 0.2776 0.3001 0.1471 0.2419 0.07871 1.095 0.9053 8.589 153.4 0.006399 0.04904 0.05373 0.01587 0.03003 0.006193 25.38 17.33 184.6 2019.0 0.1622 0.6656 0.7119 0.2654 0.4601 0.1189"
)

user_input = input("\nВведіть 30 характеристик (через пробіл): ")

try:
    # Перетворюємо введений рядок у масив чисел
    features = [float(x) for x in user_input.split()]
    if len(features) != 30:
        print(f"Помилка: введено {len(features)} значень замість 30.")
        exit()

    features_array = np.array(features).reshape(1, -1)

    # Послідовна обробка: масштабування -> PCA -> прогнозування
    scaled_data = scaler.transform(features_array)
    pca_data = pca.transform(scaled_data)
    prediction = model.predict(pca_data)

    result = "Злоякісна (Malignant)" if prediction[0] == "M" else "Доброякісна (Benign)"
    print(f"\n-> Модель класифікує об'єкт як: {result}")

except ValueError:
    print("Помилка: Будь ласка, вводьте лише числа через пробіл.")
