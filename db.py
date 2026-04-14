from sqlalchemy import create_engine
import config

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = (
            f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}"
            f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
            f"?charset=utf8mb4"
        )
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine
