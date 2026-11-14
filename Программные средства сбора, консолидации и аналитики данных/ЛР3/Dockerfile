# Образ Airflow с зависимостями конвейера (собирается командой `docker compose up -d`).
# Базовый образ фиксирован заданием: Airflow 2.5.0, Python 3.8.
FROM apache/airflow:2.5.0-python3.8

COPY requirements.txt /requirements.txt

# apache-airflow==2.5.0 указан явно, чтобы pip не обновил сам Airflow, подтягивая зависимости.
RUN pip install --no-cache-dir "apache-airflow==2.5.0" -r /requirements.txt
