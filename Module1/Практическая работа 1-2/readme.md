Практическая работа 1–2  
Так как файл описания задания содержит список из 30 вариантов индивидуальных заданий, а мой вариант по списку группы - 31, я решил взять для решения вариант 30.   

## 📁 Файлы проекта

[`create_tables.sql`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/create_tables.sql) — SQL-скрипт для создания таблиц в схемах `public`, `stg` и `dw`.

[`data_quality.sql`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/data_quality.sql) — SQL-запросы для проверки качества данных (пустые значения, дубликаты, диапазоны).

[`variant_30_tasks.sql`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/variant_30_tasks.sql) — SQL-скрипт с решениями трёх заданий варианта 30:  
 • Выручка по подкатегориям  
 • Создание таблицы возвратов по регионам  
 • Средний чек по менеджерам

[`Концептуальная модель.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/%D0%9A%D0%BE%D0%BD%D1%86%D0%B5%D0%BF%D1%82%D1%83%D0%B0%D0%BB%D1%8C%D0%BD%D0%B0%D1%8F%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C.png) — Концептуальная модель данных (ER-диаграмма) на уровне бизнес-объектов.

[`Логическая модель.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/%D0%9B%D0%BE%D0%B3%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C.png) — Логическая модель данных (реляционная схема) с таблицами и связями.

[`Физическая модель dw.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/%D0%A4%D0%B8%D0%B7%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C%20dw.png) — Физическая модель данных для схемы хранилища `dw` (звёздная схема).

[`Физическая модель public.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/%D0%A4%D0%B8%D0%B7%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C%20public.png) — Физическая модель исходных таблиц в схеме `public`.

[`Физическая модель stg.png`](https://github.com/d10ne/DEP-MGPU/blob/main/Module1/%D0%9F%D1%80%D0%B0%D0%BA%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%B0%201-2/%D0%A4%D0%B8%D0%B7%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C%20stg.png) — Физическая модель стейджинг-слоя `stg`.
