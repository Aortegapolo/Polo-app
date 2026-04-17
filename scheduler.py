import gc
import json
import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler

from cleaning import accesos, pistas, tickets, reservas
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")


def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _write_cache(filename, data):
    path = os.path.join(CACHE_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)


def refresh_accesos():
    logger.info("Actualizando accesos...")
    try:
        data = accesos.clean()
        _write_cache("accesos.json", data)
        logger.info(f"accesos.json actualizado: {len(data)} registros")
        del data
        gc.collect()
    except Exception as e:
        logger.error(f"Error actualizando accesos: {e}")


def refresh_pistas():
    logger.info("Actualizando pistas...")
    try:
        data = pistas.clean()
        _write_cache("pistas.json", data)
        logger.info(f"pistas.json actualizado: {len(data)} registros")
        del data
        gc.collect()
    except Exception as e:
        logger.error(f"Error actualizando pistas: {e}")


def refresh_tickets():
    logger.info("Actualizando tickets...")
    try:
        data = tickets.clean()
        _write_cache("tickets.json", data)
        logger.info(f"tickets.json actualizado: {len(data)} registros")
        del data
        gc.collect()
    except Exception as e:
        logger.error(f"Error actualizando tickets: {e}")


def refresh_reservas():
    logger.info("Actualizando reservas...")
    try:
        data = reservas.clean()
        _write_cache("reservas.json", data)
        logger.info(f"reservas.json actualizado: {len(data['reservas'])} registros")
        del data
        gc.collect()
    except Exception as e:
        logger.error(f"Error actualizando reservas: {e}")


def refresh_all():
    refresh_accesos()
    refresh_pistas()
    refresh_tickets()
    refresh_reservas()


def start_scheduler():
    ensure_cache_dir()
    refresh_all()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        refresh_all,
        trigger="interval",
        minutes=config.REFRESH_INTERVAL_MINUTES,
        id="periodic_refresh",
    )
    scheduler.start()
    logger.info(f"Scheduler iniciado. Actualización cada {config.REFRESH_INTERVAL_MINUTES} minutos.")
    return scheduler
