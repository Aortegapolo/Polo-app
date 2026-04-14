from sqlalchemy import create_engine
import config


def get_engine():
    url = (
        f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
        f"?charset=utf8mb4"
    )
    return create_engine(url)
