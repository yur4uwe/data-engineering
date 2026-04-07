from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import json

# Базові налаштування нашого графа завдань
default_args = {
    "owner": "yura",
    "start_date": days_ago(1),
}


# 1. Етап Extract
def _extract_data(ti):
    # Імітуємо отримання даних з двох джерел: CSV та API
    # (Використовуємо імітацію, щоб граф гарантовано працював без зовнішніх файлів)
    csv_data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
    api_data = [{"id": 3, "value": 30}, {"id": 4, "value": 40}, {"id": 5, "value": 50}]

    # Airflow вимагає використовувати XCom для передачі даних між завданнями
    ti.xcom_push(key="csv_data", value=csv_data)
    ti.xcom_push(key="api_data", value=api_data)
    print("Дані успішно 'завантажені' та передані далі.")


# 2. Етап Validate
def _validate_data(ti):
    # Читаємо дані з попереднього кроку
    csv_data = ti.xcom_pull(key="csv_data", task_ids="extract_data")
    api_data = ti.xcom_pull(key="api_data", task_ids="extract_data")

    # Перевіряємо, чи обидва масиви існують і не порожні
    if not csv_data or not api_data:
        raise ValueError("Помилка валідації: Одне з джерел не передало дані!")
    print("Дані з обох джерел валідовано успішно.")


# 3. Умовне розгалуження (Branching)
def _check_size(ti):
    csv_data = ti.xcom_pull(key="csv_data", task_ids="extract_data")
    api_data = ti.xcom_pull(key="api_data", task_ids="extract_data")

    total_records = len(csv_data) + len(api_data)
    print(f"Загальна кількість записів: {total_records}")

    # Якщо записів більше 4 - йдемо складною гілкою, інакше - простою
    if total_records > 4:
        return "process_large"
    return "process_small"


# 4а. Обробка малого обсягу
def _process_small(ti):
    csv_data = ti.xcom_pull(key="csv_data", task_ids="extract_data")
    api_data = ti.xcom_pull(key="api_data", task_ids="extract_data")

    combined_data = csv_data + api_data
    for item in combined_data:
        item["status"] = "processed_by_small_branch"

    ti.xcom_push(key="processed_data", value=combined_data)
    print("Виконана проста обробка.")


# 4б. Обробка великого обсягу (Агрегація)
def _process_large(ti):
    csv_data = ti.xcom_pull(key="csv_data", task_ids="extract_data")
    api_data = ti.xcom_pull(key="api_data", task_ids="extract_data")

    combined_data = csv_data + api_data
    # Рахуємо суму всіх 'value'
    total_sum = sum(item["value"] for item in combined_data)

    aggregated_result = [
        {"aggregated_sum": total_sum, "status": "processed_by_large_branch"}
    ]
    ti.xcom_push(key="processed_data", value=aggregated_result)
    print("Виконана складна обробка (агрегація).")


# 5. Етап Load
def _load_data(ti):
    # Намагаємося витягнути дані з обох гілок (одна з них буде None)
    data_small = ti.xcom_pull(key="processed_data", task_ids="process_small")
    data_large = ti.xcom_pull(key="processed_data", task_ids="process_large")

    # Беремо ті дані, які існують
    final_data = data_small if data_small else data_large

    # Зберігаємо результат у файл в системній папці /tmp
    output_path = "/tmp/lab9_etl_output.json"
    with open(output_path, "w") as f:
        json.dump(final_data, f)
    print(f"Фінальний звіт збережено у {output_path}: {final_data}")


# --- ЗБІРКА ГРАФА (DAG) ---
with DAG(
    "lab9_etl_branching",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL процес для 9-ї лабораторної",
) as dag:
    # Створюємо завдання (Tasks)
    start = EmptyOperator(task_id="start")

    extract = PythonOperator(task_id="extract_data", python_callable=_extract_data)

    validate = PythonOperator(task_id="validate_data", python_callable=_validate_data)

    # Оператор розгалуження
    branch = BranchPythonOperator(task_id="check_size", python_callable=_check_size)

    process_small_task = PythonOperator(
        task_id="process_small", python_callable=_process_small
    )

    process_large_task = PythonOperator(
        task_id="process_large", python_callable=_process_large
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=_load_data,
        # ВАЖЛИВО: дозволяє запустити Load, навіть якщо іншу гілку було пропущено (skipped)
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(task_id="end")

    # Будуємо послідовність виконання за схемою з методички
    start >> extract >> validate >> branch
    branch >> [process_small_task, process_large_task]
    [process_small_task, process_large_task] >> load >> end
