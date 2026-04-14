import json
import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler

from cleaning import accesos, pistas, tickets
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")


def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def refresh_accesos():
    logger.info("Actualizando accesos...")
    try:
        data = accesos.clean()
        path = os.path.join(CACHE_DIR, "accesos.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        logger.info(f"accesos.json actualizado: {len(data)} registros")
    except Exception as e:
        logger.error(f"Error actualizando accesos: {e}")


def refresh_pistas():
    logger.info("Actualizando pistas...")
    try:
        data = pistas.clean()
        path = os.path.join(CACHE_DIR, "pistas.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        logger.info(f"pistas.json actualizado: {len(data)} registros")
    except Exception as e:
        logger.error(f"Error actualizando pistas: {e}")


def refresh_tickets():
    logger.info("Actualizando tickets...")
    try:
        data = tickets.clean()
        path = os.path.join(CACHE_DIR, "tickets.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        logger.info(f"tickets.json actualizado: {len(data)} registros")
    except Exception as e:
        logger.error(f"Error actualizando tickets: {e}")


def refresh_all():
    refresh_accesos()
    refresh_pistas()
    refresh_tickets()


def start_scheduler():
    ensure_cache_dir()

    # Ejecutar una vez al arrancar para tener datos desde el primer momento
    refresh_all()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        refresh_all,
        trigger="cron",
        hour=config.REFRESH_HOUR,
        minute=config.REFRESH_MINUTE,
        id="daily_refresh",
    )
    scheduler.start()
    logger.info(
        f"Scheduler iniciado. Actualización diaria a las "
        f"{config.REFRESH_HOUR:02d}:{config.REFRESH_MINUTE:02d}"
    )
    return scheduler
