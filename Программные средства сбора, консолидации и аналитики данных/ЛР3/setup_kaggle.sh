#!/usr/bin/env bash
# Настройка доступа к Kaggle для контейнеров Airflow.
# Ожидает kaggle.json в корне проекта (kaggle.com → Settings → API → Create New Token).
# Содержимое токена скрипт не читает и не выводит — только копирует файл.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f kaggle.json ]; then
    echo "Ошибка: kaggle.json не найден в $(pwd). Положите токен в корень проекта и повторите." >&2
    exit 1
fi

mkdir -p dags/.kaggle dags/data
cp kaggle.json dags/.kaggle/kaggle.json
chmod 700 dags/.kaggle
chmod 600 dags/.kaggle/kaggle.json
chmod 777 dags/data            # сюда задача extract_from_kaggle кладёт CSV

# Контейнеры Airflow работают под UID пользователя хоста, иначе не прочитают kaggle.json (chmod 600).
if [ "$(uname -s)" = "Linux" ]; then
    uid="$(id -u)"
else
    uid=50000                  # Docker Desktop (Windows/macOS) не проверяет владельца bind-mount
fi
touch .env
grep -q '^AIRFLOW_UID=' .env || echo "AIRFLOW_UID=${uid}" >> .env

# Проверка, что секреты не попадут в Git
if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git check-ignore -q kaggle.json && git check-ignore -q dags/.kaggle/kaggle.json \
        || { echo "Внимание: kaggle.json или dags/.kaggle/ не в .gitignore!" >&2; exit 1; }
fi

echo "Готово: dags/.kaggle/kaggle.json (chmod 600), AIRFLOW_UID=$(grep '^AIRFLOW_UID=' .env | cut -d= -f2)."
echo "Дальше: docker compose up -d"
