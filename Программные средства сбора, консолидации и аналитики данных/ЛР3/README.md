# Анализ рынка видеоигр: Kaggle → Airflow → PostgreSQL → Superset

Лабораторная работа №3 · вариант 2 · «Программные средства аналитики данных»
Студент: Циприков Денис Михайлович, группа БД-251м
---

## 1. Цель работы

**Бизнес-контекст.** Команда планирования портфеля издателя видеоигр выбирает, какие игры, на каких платформах и в каких жанрах выпускать в следующем цикле. Для этого нужно понять, где исторически концентрируются продажи и за счёт чего лидируют крупнейшие издатели.

**Цель.** На открытых данных о мировых продажах видеоигр (Kaggle `sidtwr/videogames-sales-dataset`, срез на 22.12.2016) построить воспроизводимый конвейер и интерактивный дашборд, который отвечает на четыре вопроса:

1. Каков суммарный объём рынка в выборке и как продажи менялись по годам выпуска?
2. Какие жанры дают основную часть продаж?
3. Как продажи распределены между игровыми платформами?
4. Какие издатели лидируют по продажам и за счёт чего: большого числа релизов или высокой отдачи на один релиз?

**Решение, которое принимается по результатам:** какие жанры и платформы приоритизировать в релизном портфеле и с какими издателями-лидерами сравнивать собственную стратегию (широкий каталог или ставка на немногие сильные релизы).

---

## 2. Исследуемые метрики бизнес-процесса

Единица измерения продаж — **млн проданных копий** (поле `Global_Sales` датасета), а не выручка в деньгах.

| № | Метрика | Как считается | Поля источника | Какой управленческий вопрос закрывает |
|---|---|---|---|---|
| 1 | Суммарные мировые продажи | `SUM(global_sales)` по витрине | `Global_Sales` | Каков размер рынка в выборке и «масштаб» для сравнений? |
| 2 | Динамика продаж по годам | `SUM(global_sales)` в разрезе `year_of_release`; строки без года на график не попадают | `Global_Sales`, `Year_of_Release` | Когда рынок рос, где пик и спад, какой период считать репрезентативным? |
| 3 | Топ-5 жанров по продажам | `SUM(global_sales)` по `genre`, сортировка по убыванию, 5 первых | `Global_Sales`, `Genre` | В какие жанры вкладывать ресурсы? |
| 4 | Доли продаж по платформам | `SUM(global_sales)` по `platform` / общая сумма × 100 % | `Global_Sales`, `Platform` | На какие платформы выходить в первую очередь? |
| 5 | Продажи и число релизов по издателям | столбцы — `SUM(global_sales)`, линия — `COUNT(*)` по `publisher` | `Global_Sales`, `Publisher` (+ количество записей) | Лидеры держатся на широком каталоге или на высокой отдаче одного релиза? |

**Как получено «количество игр» (метрика 5).** Отдельного поля «количество игр» в датасете нет, поэтому оно **выводится из имеющихся данных**: одна строка очищенной таблицы STG — это одна пара «игра × платформа × год» после удаления дублей, а `COUNT(*)` по витрине — **число релизов**. Игра, вышедшая на трёх платформах, считается трижды. Это сознательное решение: поля витрины зафиксированы вариантом (пять полей), а число релизов — естественная единица этого датасета. Если нужны уникальные названия, в витрину добавляют `name` и считают `COUNT(DISTINCT name)`.

Все пять полей варианта (`year_of_release`, `genre`, `platform`, `publisher`, `global_sales`) в источнике есть, замены метрик не потребовалось.

### Исходные данные и формулы

**Источник.** Kaggle `sidtwr/videogames-sales-dataset`, файл `Video_Games_Sales_as_at_22_Dec_2016.csv` (16 полей): `Name, Platform, Year_of_Release, Genre, Publisher, NA_Sales, EU_Sales, JP_Sales, Other_Sales, Global_Sales, Critic_Score, Critic_Count, User_Score, User_Count, Developer, Rating`. В RAW загружаются все 16, в STG и витрину идут поля варианта (плюс `name` и `release_decade` в STG для проверки дублей и обогащения).

**Формулы метрик** (все по VIEW `videogames_datamart`, единица — млн копий):

```text
total_sales        = SUM(global_sales)
sales_by_year      = SUM(global_sales)  GROUP BY year_of_release   -- только year_of_release IS NOT NULL
top5_genres        = SUM(global_sales)  GROUP BY genre  ORDER BY 1 DESC  LIMIT 5
platform_share_pct = SUM(global_sales) по platform / SUM(global_sales) по всей витрине * 100
releases           = COUNT(*)                                       -- релиз = игра × платформа × год
sales_per_release  = SUM(global_sales) / COUNT(*)                   -- отдача на релиз, по издателю
```

**Ожидаемый результат.** Дашборд из пяти чартов (Big Number, Line, Bar, Pie, Mixed) с фильтрами по жанру, платформе, издателю и годам, где сумма по любому разрезу сходится с итогом (раздел 7.7).

---

## 3. Архитектура решения

**Среда выполнения.** Виртуальная машина Ubuntu 20.04 LTS; все сервисы запущены в Docker-контейнерах одним `docker-compose.yml` в общей Docker-сети и обращаются друг к другу по именам сервисов. Отдельная установка Airflow, PostgreSQL и Superset на ВМ не требуется.

```mermaid
flowchart LR
    K["Kaggle<br/>sidtwr/videogames-sales-dataset"] -->|kagglehub| AF

    subgraph VM["ВМ Ubuntu 20.04 LTS · Docker Compose"]
        AF["Airflow<br/>webserver :8080 + scheduler"]
        META[("postgres :5432<br/>метаданные Airflow")]
        subgraph AN["analytics_postgres :5433 · БД analytics"]
            RAW["raw_videogames<br/>RAW: всё текстом"]
            STG["stg_videogames<br/>STG: типы, очистка"]
            LOG["etl_rejected_rows<br/>etl_audit"]
            DM["videogames_datamart<br/>VIEW"]
        end
        SS["Superset :8088"]
        RD[("redis :6379")]
        PA["pgAdmin :5050"]
    end

    AF --- META
    AF -->|1. загрузка| RAW
    RAW -->|2. очистка| STG
    STG -->|журнал потерь| LOG
    STG -->|3. CREATE VIEW| DM
    DM -->|dataset, 5 чартов, фильтры| SS
    SS --- RD
    PA -.-> AN
```

### Контейнеры

| Сервис | Образ | Порт на ВМ | Роль |
|---|---|---|---|
| webserver | Apache Airflow 2.5.0 | 8080 | веб-интерфейс оркестратора |
| scheduler | Apache Airflow 2.5.0 | — | планировщик и выполнение задач DAG |
| postgres | PostgreSQL 12 | 5432 | служебная БД метаданных Airflow |
| analytics_postgres | PostgreSQL 12 | 5433 | аналитическая БД: RAW, STG, журналы, витрина |
| superset | Apache Superset 3.1.1 | 8088 | чарты, дашборд, фильтры |
| pgadmin | pgAdmin 4 (8.14) | 5050 | проверка объектов БД, ERD, ручные запросы |
| redis | Redis 7 | 6379 | кэш и очереди Superset |

### Слои данных

| Слой | Объект в БД | Что содержит |
|---|---|---|
| RAW | `raw_videogames` | точная копия CSV, все 16 полей текстом, плюс `raw_id` и `loaded_at`; ничего не чистим и не удаляем |
| STG | `stg_videogames` | типы приведены, категории нормализованы, пропуски обработаны, дубли убраны |
| журналы | `etl_rejected_rows`, `etl_audit` | каждая удалённая строка с причиной; число строк и сумма продаж на каждом шаге |
| DATAMART | `videogames_datamart` (VIEW) | пять полей варианта; **именно к ней подключён Superset** |

Конвейер Airflow (`videogames_market_analysis`), зависимости заданы явно:

```mermaid
flowchart LR
    A[extract_from_kaggle] --> B[load_raw_to_postgres] --> C[transform_to_staging] --> D[create_datamart] --> E[validate_pipeline]
```

---

## 4. Технологический стек

| Уровень | Технология | Версия |
|---|---|---|
| Хост | Ubuntu LTS | 20.04 |
| Контейнеризация | Docker Engine / Docker Compose | 29.9.1 / Compose v5.6.0 (версии стенда, на котором выполнен прогон) |
| Оркестрация | Apache Airflow (образ `apache/airflow:2.5.0-python3.8` + `Dockerfile`) | 2.5.0, Python 3.8 |
| Хранение | PostgreSQL (образ `postgres:12-alpine`) | 12 |
| Визуализация | Apache Superset (образ `apache/superset:3.1.1`) | 3.1.1 |
| Кэш Superset | Redis (образ `redis:7-alpine`) | 7 |
| Администрирование БД | pgAdmin 4 (образ `dpage/pgadmin4:8.14`) | 8.14 — закреплена: в 9.x построение ERD на PostgreSQL 12 падает (раздел 10) |
| Источник | Kaggle API через `kagglehub` (+ `kaggle`) | kagglehub 0.2.9, kaggle 1.7.4.5 |
| Обработка | Python: pandas, psycopg2-binary, apache-airflow-providers-postgres, SQLAlchemy | pandas 1.5.2, psycopg2-binary 2.9.10, providers-postgres 5.3.1, SQLAlchemy 1.4.44 |

Зависимости ставятся в образ Airflow при сборке (`Dockerfile` → `pip install "apache-airflow==2.5.0" -r requirements.txt`); pip подбирает версии под Python 3.8 и уже установленные в базовом образе пакеты. Версии библиотек внутри контейнера:

---

## 5. ER-диаграмма базы данных

База `analytics` (контейнер `analytics_postgres`). `raw_id` логически связывает слои (без объявленных `FOREIGN KEY`): любая строка RAW либо попала в STG, либо записана в журнал удалённых строк.

```mermaid
erDiagram
    raw_videogames ||--o| stg_videogames : "raw_id: прошла очистку"
    raw_videogames ||--o{ etl_rejected_rows : "raw_id: удалена с причиной"
    stg_videogames ||--|| videogames_datamart : "VIEW без WHERE"

    raw_videogames {
        bigint raw_id PK
        text name
        text platform
        text year_of_release
        text genre
        text publisher
        text na_sales
        text eu_sales
        text jp_sales
        text other_sales
        text global_sales
        text critic_score
        text critic_count
        text user_score
        text user_count
        text developer
        text rating
        timestamp loaded_at
    }
    stg_videogames {
        bigint raw_id PK
        text name
        text platform
        integer year_of_release
        integer release_decade
        text genre
        text publisher
        numeric global_sales
    }
    videogames_datamart {
        integer year_of_release
        text genre
        text platform
        text publisher
        numeric global_sales
    }
    etl_rejected_rows {
        bigint id PK
        text run_id
        bigint raw_id FK
        text reason
        text name
        text platform
        text year_of_release
        text genre
        text publisher
        text global_sales
        numeric global_sales_num
        timestamp logged_at
    }
    etl_audit {
        bigint id PK
        text run_id
        text step
        text metric
        bigint rows_cnt
        numeric sales_sum
        timestamp logged_at
    }
```

### Описание полей

`raw_videogames` (RAW): `raw_id BIGSERIAL` — номер строки источника, 16 полей CSV в нижнем регистре с типом `TEXT` (значения как в файле, включая `N/A`), `loaded_at` — время загрузки.

`stg_videogames` (STG):

| Поле | Тип | Описание |
|---|---|---|
| `raw_id` | BIGINT, PK | ссылка на строку `raw_videogames` (происхождение записи) |
| `name` | TEXT | название игры после нормализации пробелов; нужно для поиска дублей |
| `platform` | TEXT NOT NULL | платформа (`PS2`, `X360`, `Wii` …) |
| `year_of_release` | INTEGER | год выпуска 1970–2016; `NULL`, если год пуст или недостоверен |
| `release_decade` | INTEGER | обогащение: десятилетие (`2000`, `2010` …), `NULL` при неизвестном годе |
| `genre` | TEXT NOT NULL | жанр; пропуск → `Unknown` |
| `publisher` | TEXT NOT NULL | издатель; пропуск → `Unknown` |
| `global_sales` | NUMERIC(10,2) NOT NULL, ≥ 0 | мировые продажи, млн копий |

`videogames_datamart` (VIEW) — пять полей варианта из `stg_videogames` без фильтрации: `year_of_release INTEGER, genre TEXT, platform TEXT, publisher TEXT, global_sales NUMERIC(10,2)`.

`etl_rejected_rows` — удалённые при очистке строки: исходные значения, `reason`, `global_sales_num` (продажи числом для сверки сумм), `run_id` запуска DAG. `etl_audit` — счётчики каждого шага: `step`, `metric`, `rows_cnt`, `sales_sum`, `run_id`.

ERD из pgAdmin (Servers → analytics_postgres → Databases → analytics → правый клик → ERD For Database):

![ERD](screenshots/erd.png)

pgAdmin строит ERD только по таблицам: представление `videogames_datamart` в него не попадает, а линий связей нет, потому что связь по `raw_id` логическая, без объявленных `FOREIGN KEY` (RAW пересоздаётся при каждом запуске). Полная схема со связями и витриной — на диаграмме Mermaid выше. Типы журналов на скриншоте точнее, чем в Mermaid: `etl_audit.sales_sum` — `numeric(18,4)`, `etl_rejected_rows.global_sales_num` — `numeric(14,4)`.

---

## 6. Ссылки на исходный код

- DAG: [`dags/videogames_dag.py`](dags/videogames_dag.py)
- SQL витрины: [`dags/datamart_variant_02.sql`](dags/datamart_variant_02.sql)
- Экспорт дашборда Superset:
  - [`superset/dashboard_export.json`](superset/dashboard_export.json) — JSON, как требует задание (формат v0, выгружен через `Dashboard.export_dashboards` внутри контейнера Superset): дашборд, 5 чартов, 4 фильтра, раскладка, ссылка на датасет `public.videogames_datamart`. Метрик датасета и вычисляемой колонки `release_date` в этом формате нет, поэтому JSON импортируется после шагов 1–2 раздела 7.6;
  - [`superset/dashboard_export.zip`](superset/dashboard_export.zip) — родной экспорт Superset 3.1.1 (ZIP с YAML): дашборд, чарты, датасет с метриками и `release_date`, подключение (пароль замаскирован). Восстанавливает всё одним импортом.

  Оба файла проверены импортом на тестовой ВМ: ZIP — в пустой Superset, JSON — после удаления дашборда и чартов при настроенном датасете.
- Окружение: [`docker-compose.yml`](docker-compose.yml), [`Dockerfile`](Dockerfile), [`setup_kaggle.sh`](setup_kaggle.sh), [`cleanup.sh`](cleanup.sh), [`superset_config.py`](superset_config.py) (кэш Superset в Redis), [`pgadmin/servers.json`](pgadmin/servers.json) (сервер `analytics_postgres` в pgAdmin регистрируется автоматически)

---

Граф успешно выполненного DAG (все задачи `success`):

![Граф DAG](screenshots/dag_graph.png)

### 7.4. Качество данных

**Правила очистки слоя STG.**

| Правило | Что происходит со строкой |
|---|---|
| `global_sales` пусто, не число или отрицательно | **удаляется**, причина `invalid_global_sales` |
| `platform` пусто / `N/A` | **удаляется**, причина `missing_platform` |
| повтор пары (игра, платформа, год) | остаётся запись с максимальными продажами, остальные **удаляются**, причина `duplicate_game_platform_year` |
| `year_of_release` пусто / `N/A` | остаётся, год = `NULL` (**не 0**) |
| год вне диапазона 1970–2016 | остаётся, год = `NULL`: датасет на 22.12.2016, годы 2017 и 2020 в нём невозможны |
| `genre`, `publisher` пусто / `N/A` | остаётся, значение `Unknown` |
| `platform`, `genre`, `publisher`, `name` | убираются двойные и краевые пробелы, неразрывные пробелы, сноски `[1]`, «*» на конце |
| варианты, различающиеся только регистром (`nintendo` / `Nintendo`) | сводятся к самому частому написанию |
| `global_sales` | `NUMERIC(10,2)` без округления: в источнике не больше двух знаков |

Каждая удалённая строка записана в `etl_rejected_rows` (с исходными значениями и причиной), счётчики по шагам — в `etl_audit`. Просмотр результатов последнего запуска:

```sql
SELECT metric, rows_cnt, sales_sum
FROM etl_audit
WHERE run_id = (SELECT run_id FROM etl_audit ORDER BY id DESC LIMIT 1)
ORDER BY id;

SELECT reason, COUNT(*) AS rows_cnt, SUM(global_sales_num) AS sales_sum
FROM etl_rejected_rows
WHERE run_id = (SELECT run_id FROM etl_audit ORDER BY id DESC LIMIT 1)
GROUP BY reason;
```

**Потери записей в моём прогоне** (заполняется из запросов выше):

| Шаг | Строк | Сумма `global_sales`, млн | Комментарий |
|---|---|---|---|
| Источник (CSV) = RAW | 16 719 | 8 920,30 | `rows_in_raw` |
| Удалено: `invalid_global_sales` | 0 | 0 | у всех строк источника продажи — валидное число |
| Удалено: `missing_platform` | 0 | 0 | |
| Удалено: `duplicate_game_platform_year` | 1 | 0,01 | `Madden NFL 13`, PS3, 2012 (`raw_id` 16234, 0,01 млн); оставлена запись `raw_id` 605 с 2,56 млн |
| **STG = витрина** | **16 718** | **8 920,29** | `rows_loaded_to_stg` |
| Остались с `year_of_release = NULL` | 273 | 99,31 | `rows_year_unknown_in_datamart` (шаг `validate`); не видны на линейном графике |
| из них год пуст / `N/A` в источнике | 269 | 98,96 | `year_missing_in_source` |
| из них год вне диапазона → NULL | 4 | 0,35 | `year_out_of_range_set_null`: три строки с 2017 и одна с 2020 |
| Издатель `Unknown` | 255 | 49,21 | `publisher_unknown_total`: 54 строки (14,80 млн) с `N/A` в источнике + 201 строка (34,41 млн), где в источнике уже стоит `Unknown` |
| Жанр `Unknown` | 2 | 2,42 | `genre_unknown_total` (у этих же двух строк нет и названия игры) |

Баланс: `RAW = STG + удалённые` по строкам и по сумме продаж — это проверяет задача `validate_pipeline`. Вывод задачи в моём прогоне:

```text
[OK ] RAW = STG + журнал (строки): 16719 = 16718 + 1
[OK ] RAW = STG + журнал (сумма продаж): 8920.30 = 8920.29 + 0.0100
[OK ] витрина = STG (строки): 16718 vs 16718
[OK ] витрина = STG (сумма продаж): 8920.29 vs 8920.29
[OK ] сумма по годам + год неизвестен = итог: 8820.98 + 99.31 vs 8920.29
[OK ] нет NULL в genre/platform/publisher/global_sales: 0 строк
[OK ] нет лишних пробелов и сносок в категориях: 0 строк
[OK ] нет категорий, различающихся только регистром: 0 групп
[OK ] пропуски года не заменены нулём: 0 строк с годом 0
```

Нормализация текста сработала на 35 названиях игр и 2 названиях издателей (лишние пробелы); у платформ и жанров артефактов источника не нашлось, вариантов, различающихся только регистром, тоже.

### 7.5. SQL витрины данных

Файл [`dags/datamart_variant_02.sql`](dags/datamart_variant_02.sql). Очистка выполнена на слое STG, поэтому у витрины нет `WHERE`: число строк и сумма продаж совпадают с `stg_videogames`.

```sql
DROP VIEW IF EXISTS videogames_datamart;

CREATE VIEW videogames_datamart AS
SELECT
    year_of_release,
    genre,
    platform,
    publisher,
    global_sales
FROM stg_videogames;
```

### 7.6. Дашборд в Apache Superset

Названия кнопок и полей ниже — по интерфейсу Superset 3.1.1 (английская локализация).

#### Шаг 1. Подключение к аналитической БД

1. Superset `http://localhost:8088` → Settings (справа вверху) → Database Connections → **+ DATABASE**.
2. Выбрать **PostgreSQL** и заполнить форму:

| Поле | Значение | Почему так |
|---|---|---|
| HOST | `analytics_postgres` | имя сервиса в `docker-compose.yml`; `localhost` внутри контейнера Superset указывал бы на сам Superset |
| PORT | `5432` | порт внутри Docker-сети (не 5433) |
| DATABASE NAME | `analytics` | `POSTGRES_DB` |
| USERNAME / PASSWORD | `analytics` / `analytics` | `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| DISPLAY NAME | `analytics` | имя подключения в списках Superset |

То же одной строкой (вкладка «Connect this database with a SQLAlchemy URI string instead»): `postgresql+psycopg2://analytics:analytics@analytics_postgres:5432/analytics`.

3. **CONNECT** → **FINISH**. Подключаемся к аналитической БД, а не к служебной `postgres` с метаданными Airflow.

#### Шаг 2. Датасет на витрине

1. Datasets → **+ DATASET**: DATABASE `analytics`, SCHEMA `public`, TABLE **`videogames_datamart`** (это VIEW; таблицу `stg_videogames` не выбираем) → **CREATE DATASET AND CREATE CHART**.
2. Datasets → карандаш у `videogames_datamart` (Edit dataset):
   - вкладка **Metrics** → + ADD ITEM: Metric Key `sum_global_sales`, Label «Продажи, млн копий», SQL expression `SUM(global_sales)`; второй элемент — `releases_cnt`, «Релизов, шт.», `COUNT(*)`;
   - вкладка **Calculated columns** → + ADD ITEM: Column `release_date`, SQL expression `make_date(year_of_release, 1, 1)`, Data type `DATE`, отметить **Is temporal**. Колонка нужна, чтобы линейный график строился по оси времени и годы шли по порядку без пропусков в подписях;
   - вкладка **Columns**: подписи (Label) — «Год выпуска», «Жанр», «Платформа», «Издатель», «Продажи, млн копий».
3. **SAVE**.

#### Шаг 3. Чарты

Каждый чарт: Charts → **+ CHART** → датасет `videogames_datamart` → тип → **CREATE NEW CHART** → настройки → **UPDATE CHART** → **SAVE** (в окне сохранения в поле ADD TO DASHBOARD можно сразу выбрать или создать дашборд «Анализ рынка видеоигр»).

| № | Тип | Название | На какой вопрос отвечает |
|---|---|---|---|
| 1 | Big Number | Суммарные мировые продажи, млн копий | объём рынка в выборке |
| 2 | Line Chart | Динамика мировых продаж по годам выпуска | рост, пик и спад рынка |
| 3 | Bar Chart | Топ-5 жанров по мировым продажам | куда вкладывать по жанрам |
| 4 | Pie Chart | Доли мировых продаж по платформам | на каких платформах выходить |
| 5 | Mixed Chart | Продажи (столбцы) и число релизов (линия) по топ-10 издателям | каталог или отдача на релиз |

**Чарт 1. Big Number.**
- METRIC → вкладка SAVED → `sum_global_sales`.
- SUBHEADER: «млн проданных копий»; NUMBER FORMAT: `,.1f`.

**Чарт 2. Line Chart.**
- X-AXIS: `release_date`; TIME GRAIN: Year.
- METRICS: `sum_global_sales`.
- FILTERS: `release_date` — No filter (по умолчанию); добавить `year_of_release` — оператор `IS NOT NULL` (строки без года на оси времени не изображаются, их сумма показана в разделе 7.7).
- Customize: X AXIS TITLE «Год выпуска»; Y AXIS TITLE «Продажи, млн копий», Y AXIS TITLE MARGIN 50; Marker — включить; Show legend — выключить (ряд один).

**Чарт 3. Bar Chart (топ-5 жанров).**
- X-AXIS: `genre`; METRICS: `sum_global_sales`.
- ROW LIMIT: **5**; SORT BY: `sum_global_sales`, SORT DESCENDING — включить (так в запрос попадает `ORDER BY ... DESC LIMIT 5`, то есть именно пять крупнейших жанров).
- X-AXIS SORT BY: `sum_global_sales`; X-AXIS SORT ASCENDING — выключить (столбцы по убыванию).
- Customize: Show Value — включить; X AXIS TITLE «Жанр»; Y AXIS TITLE «Продажи, млн копий».

**Чарт 4. Pie Chart.**
- DIMENSIONS: `platform`; METRIC: `sum_global_sales`.
- ROW LIMIT: 50 (все 31 платформа: доли считаются от полной суммы, а не от «топа»); Sort by metric — включить.
- Customize: Donut — включить; Label Type: Category and Percentage; Percentage threshold: 3 (подписи только у секторов от 3 %); Put labels outside — включить; Show legend — включить, Orientation: Bottom; Number format `,.1f`.

**Чарт 5. Mixed Chart.**
- X-AXIS: `publisher`.
- **Query A:** METRICS `sum_global_sales`; FILTERS `publisher` IN (топ-10 из таблицы ниже); ROW LIMIT 10.
- **Query B:** METRICS `releases_cnt`; FILTERS — тот же `publisher` IN (...); ROW LIMIT 10.
- X-AXIS SORT BY не задавать: в Mixed Chart сортировка применяется к обоим запросам, а метрики `sum_global_sales` в запросе B нет, и чарт падает с ошибкой «Column referenced by aggregate is undefined».
- Customize → Query A: Series Style **Bar**, Y Axis **Primary**; Query B: Series Style **Line**, Marker — включить, Y Axis **Secondary**.
- Customize → X AXIS TITLE «Издатель», Rotate x axis label 45°; Y AXIS TITLE «Продажи, млн копий»; Secondary y-axis title «Релизов, шт.».

Список «топ-10» для чарта 5 (издатель `Unknown` — не издатель, исключён). Один раз получить в pgAdmin:

```sql
SELECT publisher, ROUND(SUM(global_sales), 2) AS sales_mln, COUNT(*) AS releases
FROM videogames_datamart
WHERE publisher <> 'Unknown'
GROUP BY publisher
ORDER BY sales_mln DESC
LIMIT 10;
```

Результат в моём прогоне:

| Издатель | Продажи, млн | Релизов | Млн на релиз |
|---|---|---|---|
| Nintendo | 1 788,81 | 706 | 2,53 |
| Electronic Arts | 1 116,95 | 1 355 | 0,82 |
| Activision | 731,16 | 985 | 0,74 |
| Sony Computer Entertainment | 606,48 | 687 | 0,88 |
| Ubisoft | 471,61 | 933 | 0,51 |
| Take-Two Interactive | 403,82 | 422 | 0,96 |
| THQ | 338,44 | 715 | 0,47 |
| Konami Digital Entertainment | 282,39 | 834 | 0,34 |
| Sega | 270,35 | 638 | 0,42 |
| Namco Bandai Games | 254,62 | 939 | 0,27 |

Список подставляется в оба запроса чарта как одинаковый фильтр `publisher IN (...)`, чтобы столбцы и линия строились по одним и тем же издателям: запросы A и B в Mixed Chart выполняются независимо, и без общего фильтра каждый выбирал бы «свои» десять издателей по своей метрике. Подзапрос в Custom SQL Superset по умолчанию не пропускает (`ALLOW_ADHOC_SUBQUERY` выключен), поэтому список статический; при смене данных его нужно пересчитать тем же запросом.

#### Шаг 4. Сборка дашборда

1. Dashboards → **+ DASHBOARD**, название «Анализ рынка видеоигр» (если дашборд создан при сохранении чартов — открыть его и нажать **EDIT DASHBOARD**).
2. Перетащить чарты из правой панели (вкладка CHARTS) на поле:
   - ряд 1 — Big Number (узкий, слева) и Line Chart;
   - ряд 2 — Bar Chart и Pie Chart поровну;
   - ряд 3 — Mixed Chart на всю ширину (десяти издателям с повёрнутыми подписями нужно место).
3. **SAVE**.

#### Шаг 5. Фильтры

1. На дашборде открыть левую панель фильтров → **+ ADD/EDIT FILTERS**.
2. Для каждого фильтра из таблицы: FILTER TYPE, FILTER NAME (без имени Superset не даёт сохранить), DATASET `videogames_datamart`, COLUMN. Для фильтров типа Value включить Can select multiple values и Sort filter values.
3. Вкладка Scoping: **Apply to all panels** — фильтр действует на все пять чартов.
4. **SAVE**, затем **SAVE** на самом дашборде.

| Фильтр | FILTER TYPE | COLUMN |
|---|---|---|
| Жанр | Value | `genre` |
| Платформа | Value | `platform` |
| Издатель | Value | `publisher` |
| Годы выпуска | Numerical range | `year_of_release` |

Проверка: выбрать значение (например, Платформа = PS4) → **APPLY FILTERS** → пересчитываются все пять виджетов (результат на втором скриншоте ниже).

**Восстановление дашборда на чистой ВМ.** Настройки Superset хранятся в его томе, а не в репозитории, поэтому после `docker compose up -d` и успешного DAG дашборд импортируется из экспорта (Dashboards → кнопка импорта справа вверху → Import dashboards):

- `superset/dashboard_export.zip` — на запрос пароля к базе `analytics` ввести `analytics`. Вместе с дашбордом создаются подключение, датасет на VIEW (с метриками и `release_date`) и все пять чартов с фильтрами;
- `superset/dashboard_export.json` — сначала выполнить шаги 1–2 (подключение и датасет с метриками `sum_global_sales`, `releases_cnt` и колонкой `release_date`), затем импортировать файл. Создаются дашборд, пять чартов и фильтры; адрес дашборда — по номеру (`/superset/dashboard/<id>/`), потому что формат v0 не сохраняет короткое имя `videogames-market`.

Итоговый дашборд (виден каждый из пяти типов чартов и панель фильтров):

![Дашборд](screenshots/dashboard.png)

Тот же дашборд с фильтром «Платформа» = PS4: пересчитываются все пять виджетов (Big Number — 314,2 млн, совпадает с `SELECT SUM(global_sales) FROM videogames_datamart WHERE platform = 'PS4'` = 314,23; линия — 2013–2016; топ-5 жанров PS4 — Action, Shooter, Sports, Role-Playing, Racing):

![Дашборд с фильтром](screenshots/dashboard_filtered.png)

Мелкие особенности Superset 3.1.1, которые видны на чартах: в Mixed Chart издатели на оси X идут по алфавиту (сортировка оси X в этом типе чарта применяется к обоим запросам, а метрики продаж в запросе B нет, поэтому она не включена); на Pie подписи долей показаны только для секторов от 3 %, остальные платформы — в легенде.

### 7.7. Сверка цифр дашборда с данными

Сумма по любому разрезу должна совпадать с итогом; года без значения (`NULL`) показываются отдельной строкой.

```sql
SELECT 'итого (Big Number)' AS cut, SUM(global_sales) AS sales_mln FROM videogames_datamart
UNION ALL SELECT 'по годам: год известен', SUM(global_sales) FROM videogames_datamart WHERE year_of_release IS NOT NULL
UNION ALL SELECT 'по годам: год неизвестен', COALESCE(SUM(global_sales), 0) FROM videogames_datamart WHERE year_of_release IS NULL
UNION ALL SELECT 'по жанрам', SUM(s) FROM (SELECT SUM(global_sales) AS s FROM videogames_datamart GROUP BY genre) t
UNION ALL SELECT 'по платформам', SUM(s) FROM (SELECT SUM(global_sales) AS s FROM videogames_datamart GROUP BY platform) t
UNION ALL SELECT 'по издателям', SUM(s) FROM (SELECT SUM(global_sales) AS s FROM videogames_datamart GROUP BY publisher) t;
```

Результат в моём прогоне:

| Разрез | Продажи, млн |
|---|---|
| итого (Big Number) | 8 920,29 |
| по годам: год известен | 8 820,98 |
| по годам: год неизвестен | 99,31 |
| по жанрам | 8 920,29 |
| по платформам | 8 920,29 |
| по издателям | 8 920,29 |

Итого 8 920,29 = 8 820,98 + 99,31 = сумма по жанрам = по платформам = по издателям. Big Number на дашборде показывает 8 920,3 (формат `,.1f`).

---

## 8. Выводы по анализу

Формат вывода: наблюдение на данных → объяснение → предлагаемое действие.

**1. Динамика рынка.**
- *Наблюдение:* пик продаж приходится на 2008 год (671,79 млн копий, 1 427 релизов; 2009 год почти на том же уровне — 658,88 млн). После 2011 года продажи падают: в 2012 году — 355,83 млн, к 2016 году — 130,10 млн (502 релиза).
- *Объяснение:* падают и продажи, и число релизов в базе (1 427 в 2008 году против 502 в 2016-м). На дашборде видна смена поколений: при фильтре «Годы выпуска» = 2008 лидируют Wii, DS, X360 и PS3, а в 2016 году 95,56 из 130,10 млн (73 %) дают PS4 и XOne. Новое поколение ещё не набрало установленную базу. Ещё две причины — гипотезы, которые эти данные не проверяют: (а) датасет считает розничные копии, а цифровые продажи не видит; (б) срез сделан 22.12.2016, и игры последних лет ещё не успели накопить продажи.
- *Действие:* не читать снижение последних лет как «рынок умер». Для планирования брать окно 2005–2011 годов и сравнивать платформы одного поколения, а данные 2016 года считать неполными.

**2. Жанры и платформы.**
- *Наблюдение:* на топ-5 жанров (Action, Sports, Shooter, Role-Playing, Platform) приходится 66,06 % продаж (5 893 из 8 920 млн), лидер — Action (1 745,27 млн, 19,57 %). Платформа-лидер — PS2 (1 255,64 млн, 14,08 %); три крупнейшие платформы (PS2, X360, PS3) дают 35,50 %. Всего платформ 31.
- *Объяснение:* массовые жанры держат продажи на всём периоде: при фильтре 2010–2016 в топ-5 снова Action, Shooter, Sports и Role-Playing, только Platform сменяется Misc. Лидеры по платформам — консоли, которые продавались много лет, и их доли накоплены за весь срок жизни. Ни одна платформа текущего на 2016 год поколения не входит в топ-6 (PS4 — 3,52 %).
- *Действие:* приоритизировать жанры Action, Shooter, Sports и Role-Playing; платформу выбирать по продажам текущего поколения (фильтр по годам), а не по накопленной доле. Нишевые жанры планировать как диверсификацию с малым бюджетом.

**3. Издатели.**
- *Наблюдение:* по продажам лидирует Nintendo: 1 788,81 млн при 706 релизах, ≈ 2,53 млн на релиз. У Electronic Arts релизов почти вдвое больше (1 355), а продаж меньше (1 116,95 млн, ≈ 0,82 млн на релиз). Крайний случай широкого каталога — Namco Bandai Games: 939 релизов, 254,62 млн, ≈ 0,27 млн на релиз.
- *Объяснение:* лидерство Nintendo строится на высокой отдаче каждого релиза, а не на объёме каталога. У остальных издателей топ-10 отдача от 0,27 до 0,96 млн на релиз (самая высокая — у Take-Two, 0,96). Предположительно, это эффект собственных франшиз и собственных платформ Nintendo; на этих данных гипотеза не проверяется.
- *Действие:* для собственной стратегии ориентироваться на отдачу на релиз (`SUM / COUNT`), а не на число релизов. Ориентиры — Nintendo и Take-Two (немного сильных релизов), а не наращивание каталога.

### Ограничения данных

- **Что измеряет датасет.** Это сведения о продажах в млн копий, собранные с сайта VGChartz (по описанию датасета на Kaggle), срез на 22.12.2016. Выручки, цифровых продаж и мобильных игр в нём нет, сравнение платформ и лет идёт без поправки на цену.
- **Отброшенные записи:** 1 строка из 16 719 (0,01 млн копий, ≈ 0,0001 % от итога): невалидные продажи — 0, нет платформы — 0, повторы (игра, платформа, год) — 1 (`Madden NFL 13`, PS3, 2012). Она записана в таблицу `etl_rejected_rows`.
- **Неизвестный год.** 273 строки (99,31 млн копий, 1,11 % продаж) имеют `year_of_release = NULL` и не видны на линейном графике, хотя входят в остальные чарты; при фильтре «Годы выпуска» они тоже выпадают. Среди них 4 строки (0,35 млн) с годом вне диапазона: три с 2017 и одна с 2020 при срезе на конец 2016 года. Год заменён на `NULL`, продажи сохранены.
- **Неизвестный издатель/жанр.** Пропуски заменены меткой `Unknown`, а не удалены: у издателя — 255 строк (49,21 млн, 0,55 % продаж; 201 из них уже помечена `Unknown` в самом источнике), у жанра — 2 строки (2,42 млн). На чарте издателей `Unknown` исключён, чтобы не выглядеть «крупнейшим издателем».
- **Издатели не объединены.** `Activision` (731,16 млн), `Activision Value` (6,95 млн) и `Activision Blizzard` (0,47 млн), а также переименованные компании считаются разными издателями. Платформы одного производителя (PS2, PS3, PS4) не группируются.
- **«Количество игр» — число релизов** (игра × платформа), а не уникальных названий.
- **Дубли** определяются по паре (название, платформа, год): возможны ошибки на играх с одинаковым названием, вышедших в один год на одной платформе, но это единичные случаи.

---

## 9. Структура проекта

```text
lpw_04/
├── dags/
│   ├── videogames_dag.py         
│   ├── datamart_variant_02.sql    
│   └── data/                      
├── screenshots/                   
├── superset/
│   └── dashboard_export.json     
├── pgadmin/
│   └── servers.json               
├── docker-compose.yml            
├── Dockerfile                     
├── requirements.txt               
├── superset_config.py             
├── setup_kaggle.sh               
├── cleanup.sh                                        
└── Readme.md                      
```

**В Git не попадают** (`.gitignore`): `kaggle.json` и `dags/.kaggle/` — личный токен Kaggle (выложенный ключ считается скомпрометированным); `dags/data/*` — скачанный CSV, он воспроизводится задачей `extract_from_kaggle`; `.env` — UID пользователя конкретной ВМ, его создаёт `setup_kaggle.sh`.
