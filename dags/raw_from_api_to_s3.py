import logging
import duckdb
import pendulum
import requests
from minio import Minio
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Константы
owner = "a.st"
dag_id = "raw_from_api_to_s3"
layer = "raw"
source = "earthquake"

# Переменные из Airflow (создайте их в UI: Admin → Variables)
ACCESS_KEY = Variable.get("access_key", default_var="minioadmin")
SECRET_KEY = Variable.get("secret_key", default_var="minioadmin")
BUCKET_NAME = Variable.get("minio_bucket", default_var="earthquake-data")


# Функция для загрузки из API
def fetch_earthquake_api():
    """Загружает данные из USGS API"""
    logger.info("Начинаем загрузку из USGS API")

    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Загружено {len(data['features'])} землетрясений")

    # Сохраняем в XCom для следующей задачи
    return data


# Функция для загрузки в MinIO
def upload_to_minio(**context):
    """Загружает данные в MinIO (S3)"""
    from io import BytesIO
    import json

    # Получаем данные из предыдущей задачи
    data = context["task_instance"].xcom_pull(task_ids="fetch_earthquake_data")

    if not data:
        logger.error("Нет данных для загрузки")
        return

    # Подключение к MinIO
    client = Minio(
        "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    # Создаем bucket, если не существует
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Создан bucket: {BUCKET_NAME}")

    # Формируем имя файла с датой
    execution_date = context["execution_date"]
    file_name = f"{layer}/{source}/{execution_date.strftime('%Y/%m/%d/%H')}/data.json"

    # Загружаем данные
    json_data = json.dumps(data, indent=2)
    data_bytes = BytesIO(json_data.encode("utf-8"))

    client.put_object(BUCKET_NAME, file_name, data_bytes, length=len(json_data))

    logger.info(f"Данные загружены в MinIO: {file_name}")


# Определение DAG
with DAG(
    dag_id=dag_id,
    description="Загружает данные о землетрясениях из USGS API в MinIO",
    default_args={
        "owner": owner,
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    },
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=[layer, source, "api", "s3"],
) as dag:

    # Операторы
    start = EmptyOperator(task_id="start")

    fetch_task = PythonOperator(
        task_id="fetch_earthquake_data",
        python_callable=fetch_earthquake_api,
    )

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    # Порядок выполнения
    start >> fetch_task >> upload_task >> end
