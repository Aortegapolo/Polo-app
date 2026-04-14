import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", 3306))
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

REFRESH_HOUR   = int(os.getenv("REFRESH_HOUR", 6))
REFRESH_MINUTE = int(os.getenv("REFRESH_MINUTE", 0))
