#!/usr/bin/env bash
# Полная очистка окружения после сдачи: контейнеры, тома (все данные БД), локально собранный образ,
# скачанный CSV и копия токена Kaggle. kaggle.json в корне проекта не трогается.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down --volumes --rmi local --remove-orphans
find dags/data -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
rm -rf dags/.kaggle

echo "Окружение очищено."
