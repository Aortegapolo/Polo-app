import json
import os
from flask import Flask, jsonify, send_from_directory, abort

from scheduler import start_scheduler

BASE_DIR  = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")

app = Flask(__name__)

# Arranca el scheduler al importar el módulo (funciona con gunicorn y python app.py)
scheduler = start_scheduler()


# ── PÁGINAS HTML ──────────────────────────────────────────────────

@app.route("/")
@app.route("/accesos")
def accesos_page():
    return send_from_directory(BASE_DIR, "accesos.html")


@app.route("/pistas")
def pistas_page():
    return send_from_directory(BASE_DIR, "pistas.html")


@app.route("/tickets")
def tickets_page():
    return send_from_directory(BASE_DIR, "tickets.html")


# ── API ───────────────────────────────────────────────────────────

def _serve_cache(filename):
    path = os.path.join(CACHE_DIR, filename)
    if not os.path.exists(path):
        abort(503, description="Datos no disponibles todavía. El servidor está generando la caché inicial.")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)


@app.route("/api/accesos")
def api_accesos():
    return _serve_cache("accesos.json")


@app.route("/api/pistas")
def api_pistas():
    return _serve_cache("pistas.json")


@app.route("/api/tickets")
def api_tickets():
    return _serve_cache("tickets.json")


# ── ARRANQUE ──────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=5000, debug=False)
    finally:
        scheduler.shutdown()
