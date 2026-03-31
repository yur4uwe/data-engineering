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

print("=== Медична діагностика: Прогнозування діабету ===")
print("Введіть 4 показники пацієнта через пробіл:")
print("1. Рівень глюкози (наприклад: 120.0)")
print("2. Кров'яний тиск (наприклад: 80.0)")
print("3. Індекс маси тіла, BMI (наприклад: 25.5)")
print("4. Вік (наприклад: 45.0)")

user_input = input("\nВведіть значення (наприклад: 120 80 25.5 45): ")

try:
    # Перетворюємо введений рядок у масив чисел
    features = [float(x) for x in user_input.split()]
    if len(features) != 4:
        print(f"Помилка: введено {len(features)} значень замість 30.")
        exit()

    if features[0] < 0 or features[1] < 0 or features[2] < 0 or features[3] < 0:
        print("Помилка: введені значення мають бути більше нуля.")
        exit()

    if features[0] > 400:
        print("Помилка: значення рівня глюкози критичне зразу йдіть у лікарню")
        exit()

    if features[1] > 250:
        print("Помилка: значення кров'яного тиск критичне зразу йдіть у лікарню")
        exit()

    if features[2] > 80:
        print("Помилка: індекс маси тіла критичний зразу йдіть у лікарню")
        exit()

    if features[3] > 100:
        print("Помилка: як жилось між динозаврами?")
        exit()

    features_array = np.array(features).reshape(1, -1)

    # Послідовна обробка: масштабування -> PCA -> прогнозування
    scaled_data = scaler.transform(features_array)
    pca_data = pca.transform(scaled_data)
    prediction = model.predict(pca_data)
    result = (
        "Виявлено ознаки діабету" if prediction[0] == 1 else "Здорова людина (Норма)"
    )
    print(f"\n-> Модель класифікує об'єкт як: {result}")
except ValueError:
    print("Помилка: Будь ласка, вводьте лише числа через пробіл.")
