# Практическая работа 2–1  
Так как файл описания задания содержит список из 30 вариантов индивидуальных заданий, а мой вариант по списку группы - 31, я решил взять для решения вариант 30.

## 📁 Файлы проекта

[`dbt run.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/dbt%20run.png) — Скриншот выполнения команды `dbt run` в терминале (загрузка или обновление моделей).

[`dbt test.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/dbt%20test.png) — Скриншот выполнения команды `dbt test` (проверка качества данных и валидации моделей).

[`lineage архитектура dwh.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/lineage%20%D0%B0%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0%20dwh.png) — Визуализация линейки зависимостей (data lineage между моделями в хранилище данных).

[`mart_pareto_customer_analysis.sql`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/mart_pareto_customer_analysis.sql) — SQL-скрипт для анализа Парето: определение доли выручки, приносимой 20% самых прибыльных клиентов.

[`sales_fact.sql`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/sales_fact.sql) — SQL-модель для создания или наполнения таблицы `sales_fact` в рамках dbt-проекта.

[`schema yml.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/schema%20yml.png) — Скриншот файла `schema.yml`.

[`stg_orders.sql`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/stg_orders.sql) — SQL-модель `stg_orders`.

[`Архитектура dwh.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%202-1/%D0%90%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0%20dwh.png) — Визуальная схема архитектуры хранилища данных.
