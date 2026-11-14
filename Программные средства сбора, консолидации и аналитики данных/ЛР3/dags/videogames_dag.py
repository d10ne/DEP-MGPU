"""
DAG «Анализ рынка видеоигр» — вариант 2.

Конвейер:  Kaggle  →  RAW (PostgreSQL)  →  STG (PostgreSQL)  →  VIEW-витрина  →  проверки
           (sidtwr/videogames-sales-dataset)
"""

from __future__ import annotations

import csv
import glob
import os
import re
import shutil
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

CONN_ID = "analytics_postgres"                       
KAGGLE_DATASET = "sidtwr/videogames-sales-dataset"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SOURCE_COPY_NAME = "videogames_sales_source.csv"

RAW_TABLE = "raw_videogames"
STG_TABLE = "stg_videogames"
REJECT_TABLE = "etl_rejected_rows"
AUDIT_TABLE = "etl_audit"
VIEW_NAME = "videogames_datamart"
DATAMART_SQL = "datamart_variant_02.sql"             

REQUIRED_COLUMNS = ["name", "platform", "year_of_release", "genre", "publisher", "global_sales"]
COLUMN_ALIASES = {"year": "year_of_release", "release_year": "year_of_release"}

MIN_VALID_YEAR = 1970       
MAX_VALID_YEAR = 2016      
MISSING_MARKERS = {"", "n/a", "na", "nan", "null", "none", "-", "--", "tbd"}
UNKNOWN_LABEL = "Unknown"

_NUMBER_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")
_YEAR_RE = re.compile(r"^\d{4}(\.0+)?$")


def _to_snake(name: str) -> str:
    """'Year_of_Release' / 'Year of Release' → 'year_of_release'."""
    return re.sub(r"[^0-9a-z]+", "_", str(name).lower()).strip("_")


def _canonical_column(name: str) -> str:
    snake = _to_snake(name)
    return COLUMN_ALIASES.get(snake, snake)


def _read_header(path: str) -> list:
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            with open(path, newline="", encoding=encoding) as fh:
                return next(csv.reader(fh))
        except UnicodeDecodeError:
            continue
    return []


def _read_source_csv(path: str):
    """CSV целиком как текст: ни одно значение не интерпретируется (RAW = копия источника)."""
    import pandas as pd

    last_error = None
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return pd.read_csv(path, dtype=str, na_filter=False, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise last_error


def _find_source_csv(root: str) -> str:
    candidates = sorted(glob.glob(os.path.join(root, "**", "*.csv"), recursive=True))
    if not candidates:
        raise AirflowFailException(f"В скачанном датасете нет CSV-файлов: {os.listdir(root)}")
    for path in candidates:
        columns = {_canonical_column(c) for c in _read_header(path)}
        if set(REQUIRED_COLUMNS) <= columns:
            return path
    raise AirflowFailException(
        "Ни в одном CSV нет всех нужных полей "
        f"{REQUIRED_COLUMNS}. Найдены файлы: {[os.path.basename(p) for p in candidates]}. "
        "Состав полей датасета изменился — адаптируйте COLUMN_ALIASES и опишите это в Readme."
    )


@contextmanager
def _cursor():
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def _ensure_service_tables(cur) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {REJECT_TABLE} (
            id               BIGSERIAL PRIMARY KEY,
            run_id           TEXT      NOT NULL,
            raw_id           BIGINT    NOT NULL,
            reason           TEXT      NOT NULL,
            name             TEXT,
            platform         TEXT,
            year_of_release  TEXT,
            genre            TEXT,
            publisher        TEXT,
            global_sales     TEXT,
            global_sales_num NUMERIC(14, 4),
            logged_at        TIMESTAMP NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            id        BIGSERIAL PRIMARY KEY,
            run_id    TEXT      NOT NULL,
            step      TEXT      NOT NULL,
            metric    TEXT      NOT NULL,
            rows_cnt  BIGINT,
            sales_sum NUMERIC(18, 4),
            logged_at TIMESTAMP NOT NULL DEFAULT now()
        );
        """
    )


def _write_audit(cur, run_id: str, step: str, metrics) -> None:
    from psycopg2.extras import execute_values

    cur.execute(f"DELETE FROM {AUDIT_TABLE} WHERE run_id = %s AND step = %s", (run_id, step))
    rows = [(run_id, step, m, int(n), None if s is None else Decimal(s)) for m, n, s in metrics]
    if rows:
        execute_values(
            cur,
            f"INSERT INTO {AUDIT_TABLE} (run_id, step, metric, rows_cnt, sales_sum) VALUES %s",
            rows,
        )


def _clean_text(value):
    if value is None:
        return None
    text = str(value).replace(" ", " ").replace("​", "")
    text = re.sub(r"\[\s*\d+\s*\]", "", text)             # сноски [1], [ 23 ]
    text = re.sub(r"[\s*†‡]+$", "", text)       # «*», «†», «‡» на конце
    text = re.sub(r"\s+", " ", text).strip()              # двойные/краевые пробелы
    return None if text.casefold() in MISSING_MARKERS else text


def _parse_decimal(value):
    text = "" if value is None else str(value).strip()
    return Decimal(text) if _NUMBER_RE.match(text) else None


def _parse_year(value, max_year: int):
    """→ (year | None, причина | None). Пропуск остаётся NULL, а не нулём."""
    text = "" if value is None else str(value).strip()
    if text.casefold() in MISSING_MARKERS:
        return None, "year_missing_in_source"
    if not _YEAR_RE.match(text):
        return None, "year_unparseable_set_null"
    year = int(text.split(".")[0])
    if year < MIN_VALID_YEAR or year > max_year:
        return None, "year_out_of_range_set_null"
    return year, None


def _canonical_spelling(values):
    """{casefold → самое частое написание}: 'nintendo' и 'Nintendo' станут одной категорией."""
    freq = {}
    for v in values:
        freq.setdefault(v.casefold(), Counter())[v] += 1
    return {k: sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] for k, c in freq.items()}


STAT_ORDER = [
    "rows_in_raw",
    "rows_rejected_invalid_global_sales",
    "rows_rejected_missing_platform",
    "rows_rejected_duplicate_game_platform_year",
    "rows_loaded_to_stg",
    "year_missing_in_source",
    "year_unparseable_set_null",
    "year_out_of_range_set_null",
    "genre_missing_set_unknown",
    "publisher_missing_set_unknown",
    "name_missing_row_kept",
    "text_normalized_name",
    "text_normalized_platform",
    "text_normalized_genre",
    "text_normalized_publisher",
    "case_merged_platform",
    "case_merged_genre",
    "case_merged_publisher",
    "genre_unknown_total",
    "publisher_unknown_total",
]


def clean_videogames(raw_rows, max_year: int = MAX_VALID_YEAR):

    stats = {m: [0, Decimal("0")] for m in STAT_ORDER}

    def bump(metric, sales=None):
        entry = stats.setdefault(metric, [0, Decimal("0")])
        entry[0] += 1
        if sales is not None:
            entry[1] += sales

    parsed = []
    for r in raw_rows:
        item = {
            "raw": r,
            "raw_id": r["raw_id"],
            "sales": _parse_decimal(r.get("global_sales")),
            "name": _clean_text(r.get("name")),
            "platform": _clean_text(r.get("platform")),
            "genre": _clean_text(r.get("genre")),
            "publisher": _clean_text(r.get("publisher")),
            "flags": [],                  
        }
        item["year"], year_issue = _parse_year(r.get("year_of_release"), max_year)
        if year_issue:
            item["flags"].append(year_issue)
        for col in ("name", "platform", "genre", "publisher"):
            if item[col] is not None and item[col] != r.get(col):
                item["flags"].append("text_normalized_" + col)     # пробелы, сноски, «*»
        parsed.append(item)

    stats["rows_in_raw"] = [len(parsed), sum((p["sales"] or Decimal("0")) for p in parsed)]

    kept, rejected = [], []

    def reject(p, reason):
        raw = p["raw"]
        rejected.append(
            {
                "raw_id": p["raw_id"],
                "reason": reason,
                "name": raw.get("name"),
                "platform": raw.get("platform"),
                "year_of_release": raw.get("year_of_release"),
                "genre": raw.get("genre"),
                "publisher": raw.get("publisher"),
                "global_sales": raw.get("global_sales"),
                "global_sales_num": p["sales"],
            }
        )
        bump("rows_rejected_" + reason, p["sales"])

    for p in parsed:
        if p["sales"] is None or p["sales"] < 0:
            reject(p, "invalid_global_sales")
        elif p["platform"] is None:
            reject(p, "missing_platform")
        else:
            kept.append(p)

    def is_unknown(v):
        return v is None or v.casefold() == UNKNOWN_LABEL.casefold()

    for col in ("platform", "genre", "publisher"):
        mapping = _canonical_spelling(p[col] for p in kept if not is_unknown(p[col]))
        for p in kept:
            if is_unknown(p[col]):
                continue
            canon = mapping[p[col].casefold()]
            if canon != p[col]:
                p["flags"].append("case_merged_" + col)
                p[col] = canon

    for p in kept:
        for col in ("genre", "publisher"):
            if p[col] is None:
                p["flags"].append(col + "_missing_set_unknown")
            if is_unknown(p[col]):
                p[col] = UNKNOWN_LABEL
        if p["name"] is None:
            p["flags"].append("name_missing_row_kept")

    def dup_key(p):
        # строки без названия не считаем дублями друг друга
        name_key = p["name"].casefold() if p["name"] else "\0%s" % p["raw_id"]
        return (name_key, p["platform"].casefold(), p["year"])

    seen, dup_ids = set(), set()
    for p in sorted(kept, key=lambda q: (-q["sales"], q["raw_id"])):
        key = dup_key(p)
        if key in seen:
            dup_ids.add(p["raw_id"])
        else:
            seen.add(key)

    survivors = []
    for p in kept:
        if p["raw_id"] in dup_ids:
            reject(p, "duplicate_game_platform_year")
        else:
            survivors.append(p)

    stg_rows = []
    for p in survivors:
        for flag in p["flags"]:
            bump(flag, p["sales"])
        if p["genre"] == UNKNOWN_LABEL:
            bump("genre_unknown_total", p["sales"])
        if p["publisher"] == UNKNOWN_LABEL:
            bump("publisher_unknown_total", p["sales"])
        year = p["year"]
        decade = (year // 10) * 10 if year is not None else None      # обогащение
        stg_rows.append((p["raw_id"], p["name"], p["platform"], year, decade,
                         p["genre"], p["publisher"], p["sales"]))
    stg_rows.sort(key=lambda t: t[0])
    rejected.sort(key=lambda d: d["raw_id"])

    stats["rows_loaded_to_stg"] = [len(stg_rows), sum((t[7] for t in stg_rows), Decimal("0"))]
    return stg_rows, rejected, [(m, v[0], v[1]) for m, v in stats.items()]


def extract_from_kaggle(**context):
    import kagglehub

    kaggle_json = os.path.join(os.path.expanduser("~"), ".kaggle", "kaggle.json")
    has_env_creds = os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")
    if not os.path.exists(kaggle_json) and not has_env_creds:
        raise FileNotFoundError(
            f"Не найден {kaggle_json} (и не заданы KAGGLE_USERNAME/KAGGLE_KEY). "
            "Положите kaggle.json в корень проекта и запустите ./setup_kaggle.sh."
        )

    print(f"Скачиваем датасет {KAGGLE_DATASET} ...")
    downloaded_dir = kagglehub.dataset_download(KAGGLE_DATASET)
    print(f"Датасет скачан в {downloaded_dir}; файлы: {os.listdir(downloaded_dir)}")

    source = _find_source_csv(downloaded_dir)
    os.makedirs(DATA_DIR, exist_ok=True)
    destination = os.path.join(DATA_DIR, SOURCE_COPY_NAME)
    shutil.copy2(source, destination)
    print(f"Исходный файл {os.path.basename(source)} скопирован в {destination}")
    return destination                                   # уходит в XCom (return_value)


def load_raw_to_postgres(**context):
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    path = context["ti"].xcom_pull(task_ids="extract_from_kaggle")
    df = _read_source_csv(path)
    df.columns = [_canonical_column(c) for c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise AirflowFailException(f"В источнике нет обязательных полей: {missing}. Есть: {list(df.columns)}")
    if df.columns.duplicated().any():
        raise AirflowFailException(f"Повторяющиеся названия полей в источнике: {list(df.columns)}")
    # служебные поля RAW не должны конфликтовать с полями источника
    df.columns = [("src_" + c if c in ("raw_id", "loaded_at") else c) for c in df.columns]
    print(f"Прочитано {len(df)} строк, {len(df.columns)} полей: {list(df.columns)}")

    run_id = context["run_id"]
    columns = list(df.columns)
    with _cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(RAW_TABLE)))
        cur.execute(
            sql.SQL("CREATE TABLE {} (raw_id BIGSERIAL PRIMARY KEY, {}, loaded_at TIMESTAMP NOT NULL DEFAULT now())").format(
                sql.Identifier(RAW_TABLE),
                sql.SQL(", ").join(sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in columns),
            )
        )
        execute_values(
            cur,
            sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(RAW_TABLE), sql.SQL(", ").join(map(sql.Identifier, columns))
            ).as_string(cur),
            list(df.itertuples(index=False, name=None)),
            page_size=1000,
        )
        cur.execute(
            f"""SELECT COUNT(*),
                       COALESCE(SUM(CASE WHEN btrim(global_sales) ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                                         THEN btrim(global_sales)::numeric END), 0)
                FROM {RAW_TABLE}"""
        )
        raw_cnt, raw_sum = cur.fetchone()
        _ensure_service_tables(cur)
        _write_audit(cur, run_id, "raw", [("rows_loaded_to_raw", raw_cnt, raw_sum)])
    if raw_cnt != len(df):
        raise AirflowFailException(f"В RAW {raw_cnt} строк, в CSV {len(df)}")
    print(f"RAW: загружено {raw_cnt} строк, сумма global_sales = {raw_sum}")


def transform_to_staging(**context):
    """Transform: RAW → STG (типы, нормализация, дубли) + журнал удалённых строк."""
    from psycopg2.extras import execute_values

    run_id = context["run_id"]
    with _cursor() as cur:
        cur.execute(f"SELECT * FROM {RAW_TABLE} ORDER BY raw_id")
        names = [d[0] for d in cur.description]
        raw_rows = [dict(zip(names, row)) for row in cur.fetchall()]

        stg_rows, rejected, stats = clean_videogames(raw_rows)

        cur.execute(f"DROP TABLE IF EXISTS {STG_TABLE} CASCADE")   # вместе с VIEW — её пересоздаёт create_datamart
        cur.execute(
            f"""
            CREATE TABLE {STG_TABLE} (
                raw_id          BIGINT        PRIMARY KEY,   -- ссылка на строку RAW (lineage)
                name            TEXT,
                platform        TEXT          NOT NULL,
                year_of_release INTEGER,                     -- NULL = год неизвестен/недостоверен
                release_decade  INTEGER,                     -- обогащение: 1980, 1990, 2000 ...
                genre           TEXT          NOT NULL,
                publisher       TEXT          NOT NULL,
                global_sales    NUMERIC(10, 2) NOT NULL CHECK (global_sales >= 0)   -- млн копий
            )
            """
        )
        execute_values(
            cur,
            f"""INSERT INTO {STG_TABLE}
                (raw_id, name, platform, year_of_release, release_decade, genre, publisher, global_sales)
                VALUES %s""",
            stg_rows,
            page_size=1000,
        )

        _ensure_service_tables(cur)
        cur.execute(f"DELETE FROM {REJECT_TABLE} WHERE run_id = %s", (run_id,))
        if rejected:
            execute_values(
                cur,
                f"""INSERT INTO {REJECT_TABLE}
                    (run_id, raw_id, reason, name, platform, year_of_release, genre, publisher,
                     global_sales, global_sales_num) VALUES %s""",
                [
                    (run_id, d["raw_id"], d["reason"], d["name"], d["platform"], d["year_of_release"],
                     d["genre"], d["publisher"], d["global_sales"], d["global_sales_num"])
                    for d in rejected
                ],
                page_size=1000,
            )
        _write_audit(cur, run_id, "stg", stats)

    print("Шаги очистки (метрика: строк / сумма global_sales):")
    for metric, n, s in stats:
        print(f"  {metric:<55} {n:>7}  {s}")
    print(f"Журнал {REJECT_TABLE}: {len(rejected)} строк; STG: {len(stg_rows)} строк")


def validate_pipeline(**context):
    run_id = context["run_id"]
    problems = []

    with _cursor() as cur:
        def one(query, params=None):
            cur.execute(query, params)
            return cur.fetchone()

        raw_cnt, raw_sum = one(
            f"""SELECT COUNT(*), COALESCE(SUM(CASE WHEN btrim(global_sales) ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                                                  THEN btrim(global_sales)::numeric END), 0) FROM {RAW_TABLE}"""
        )
        stg_cnt, stg_sum = one(f"SELECT COUNT(*), COALESCE(SUM(global_sales), 0) FROM {STG_TABLE}")
        rej_cnt, rej_sum = one(
            f"SELECT COUNT(*), COALESCE(SUM(global_sales_num), 0) FROM {REJECT_TABLE} WHERE run_id = %s", (run_id,)
        )
        view_cnt, view_sum = one(f"SELECT COUNT(*), COALESCE(SUM(global_sales), 0) FROM {VIEW_NAME}")
        known_sum, = one(f"SELECT COALESCE(SUM(global_sales), 0) FROM {VIEW_NAME} WHERE year_of_release IS NOT NULL")
        unknown_cnt, unknown_sum = one(
            f"SELECT COUNT(*), COALESCE(SUM(global_sales), 0) FROM {VIEW_NAME} WHERE year_of_release IS NULL"
        )
        null_cnt, = one(
            f"""SELECT COUNT(*) FROM {VIEW_NAME}
                WHERE genre IS NULL OR platform IS NULL OR publisher IS NULL OR global_sales IS NULL"""
        )
        dirty_cnt, = one(
            rf"""SELECT COUNT(*) FROM {VIEW_NAME}
                 WHERE genre ~ '^\s|\s$|\s{{2,}}|\[\d+\]' OR platform ~ '^\s|\s$|\s{{2,}}|\[\d+\]'
                    OR publisher ~ '^\s|\s$|\s{{2,}}|\[\d+\]'"""
        )
        case_dups = 0
        for col in ("genre", "platform", "publisher"):
            n, = one(
                f"""SELECT COUNT(*) FROM (SELECT 1 FROM {VIEW_NAME}
                    GROUP BY lower({col}) HAVING COUNT(DISTINCT {col}) > 1) t"""
            )
            case_dups += n
        zero_year, = one(f"SELECT COUNT(*) FROM {VIEW_NAME} WHERE year_of_release = 0")

        checks = [
            ("RAW = STG + журнал (строки)", raw_cnt == stg_cnt + rej_cnt, f"{raw_cnt} = {stg_cnt} + {rej_cnt}"),
            ("RAW = STG + журнал (сумма продаж)", raw_sum == stg_sum + rej_sum, f"{raw_sum} = {stg_sum} + {rej_sum}"),
            ("витрина = STG (строки)", view_cnt == stg_cnt, f"{view_cnt} vs {stg_cnt}"),
            ("витрина = STG (сумма продаж)", view_sum == stg_sum, f"{view_sum} vs {stg_sum}"),
            ("сумма по годам + год неизвестен = итог", known_sum + unknown_sum == view_sum,
             f"{known_sum} + {unknown_sum} vs {view_sum}"),
            ("нет NULL в genre/platform/publisher/global_sales", null_cnt == 0, f"{null_cnt} строк"),
            ("нет лишних пробелов и сносок в категориях", dirty_cnt == 0, f"{dirty_cnt} строк"),
            ("нет категорий, различающихся только регистром", case_dups == 0, f"{case_dups} групп"),
            ("пропуски года не заменены нулём", zero_year == 0, f"{zero_year} строк с годом 0"),
        ]

        print("Сверка конвейера:")
        for title, ok, detail in checks:
            print(f"  [{'OK ' if ok else 'FAIL'}] {title}: {detail}")
            if not ok:
                problems.append(f"{title}: {detail}")
        print(f"  Строк с неизвестным годом (не попадают на линейный график): {unknown_cnt}, продажи {unknown_sum}")

        _ensure_service_tables(cur)
        _write_audit(
            cur, run_id, "validate",
            [
                ("rows_in_datamart", view_cnt, view_sum),
                ("rows_rejected_total", rej_cnt, rej_sum),
                ("rows_year_unknown_in_datamart", unknown_cnt, unknown_sum),
                ("checks_failed", len(problems), None),
            ],
        )

    if problems:
        raise AirflowFailException("Сверка не пройдена: " + "; ".join(problems))
    print("Все проверки пройдены: число записей и суммы продаж сходятся на каждом слое.")


default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="videogames_market_analysis",
    description="Анализ рынка видеоигр (вариант 2): Kaggle → RAW → STG → VIEW → Superset",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,                      # запуск вручную: Trigger DAG
    catchup=False,
    tags=["etl", "kaggle", "videogames", "variant_02"],
    doc_md=__doc__,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_from_kaggle",
        python_callable=extract_from_kaggle,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_to_postgres,
    )

    transform_task = PythonOperator(
        task_id="transform_to_staging",
        python_callable=transform_to_staging,
    )

    create_datamart_task = PostgresOperator(
        task_id="create_datamart",
        postgres_conn_id=CONN_ID,
        sql=DATAMART_SQL,
    )

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
    )

    extract_task >> load_raw_task >> transform_task >> create_datamart_task >> validate_task
