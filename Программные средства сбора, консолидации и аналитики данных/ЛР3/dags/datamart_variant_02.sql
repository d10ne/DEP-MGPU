-- =============================================================================
-- Витрина данных: анализ рынка видеоигр (вариант 2)
-- Источник : Kaggle sidtwr/videogames-sales-dataset
-- Слои     : raw_videogames  ->  stg_videogames  ->  videogames_datamart (этот VIEW)
--
-- Superset подключается именно к этой витрине, а не к stg_*: витрина — контракт
-- между конвейером и дашбордом, поэтому внутренняя структура STG может меняться
-- без поломки чартов.
--
-- Очистка (типы, пропуски, нормализация категорий, дубли) выполнена на слое STG,
-- поэтому здесь нет WHERE: число строк витрины == число строк stg_videogames, а
-- SUM(global_sales) витрины == SUM(global_sales) STG (это проверяет задача
-- validate_pipeline в DAG).
--
-- Поля (названия в нижнем регистре по условию варианта):
--   year_of_release  INTEGER        год выпуска; NULL = год неизвестен/недостоверен
--                                   (пропуски НЕ заменяются нулём)
--   genre            TEXT           жанр
--   platform         TEXT           игровая платформа
--   publisher        TEXT           издатель; 'Unknown' = издатель в источнике не указан
--   global_sales     NUMERIC(10,2)  мировые продажи, млн копий
--
-- Метрики дашборда считаются поверх этих полей:
--   индикатор        SUM(global_sales)
--   линейная         SUM(global_sales) по year_of_release
--   столбчатая       SUM(global_sales) по genre, топ-5
--   круговая         SUM(global_sales) по platform (доли)
--   комбинированная  SUM(global_sales) и COUNT(*) по publisher
--                    (COUNT(*) = число релизов «игра × платформа»)
-- =============================================================================

DROP VIEW IF EXISTS videogames_datamart;

CREATE VIEW videogames_datamart AS
SELECT
    year_of_release,
    genre,
    platform,
    publisher,
    global_sales
FROM stg_videogames;

COMMENT ON VIEW videogames_datamart IS
'Витрина для дашборда «Анализ рынка видеоигр»: год, жанр, платформа, издатель и мировые продажи (млн копий). Источник: stg_videogames.';
