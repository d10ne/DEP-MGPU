# Конфигурация Superset 3.1.1 для лабораторного стенда (монтируется в контейнер superset).
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Метаданные Superset — SQLite в томе superset-home (по умолчанию образа).
# Кэш (метаданные, результаты запросов, состояние фильтров) — в Redis.
REDIS_URL = "redis://redis:6379"


def _cache(db: int, prefix: str) -> dict:
    return {
        "CACHE_TYPE": "RedisCache",
        "CACHE_DEFAULT_TIMEOUT": 300,
        "CACHE_KEY_PREFIX": prefix,
        "CACHE_REDIS_URL": f"{REDIS_URL}/{db}",
    }


CACHE_CONFIG = _cache(0, "superset_")
DATA_CACHE_CONFIG = _cache(1, "superset_data_")
FILTER_STATE_CACHE_CONFIG = _cache(2, "superset_filter_")
EXPLORE_FORM_DATA_CACHE_CONFIG = _cache(3, "superset_explore_")

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
