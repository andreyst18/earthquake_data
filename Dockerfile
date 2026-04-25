FROM apache/airflow:2.7.1

# Переключаемся на root для установки системных пакетов (если нужны)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    g++ \
    make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Возвращаемся к airflow для pip install
USER airflow

# Устанавливаем duckdb из бинарного пакета (без компиляции)
RUN pip install --no-cache-dir --only-binary=duckdb duckdb