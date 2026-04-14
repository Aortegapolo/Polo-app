from sqlalchemy import text
from db import get_engine


def clean():
    """
    Consulta y limpia los datos de la tabla biostar_event.

    Devuelve una lista de dicts con las columnas:
        fecha         : str  "YYYY-MM-DD HH:MM:SS"
        dispositivo   : str  nombre del dispositivo de acceso
        grupo_usuario : str  grupo al que pertenece el usuario (pendiente join)
        evento        : str  tipo de evento registrado
    """
    query = text("""
        SELECT
            DATE_FORMAT(datetime, '%Y-%m-%d %H:%i:%s') AS fecha,
            device_name                                 AS dispositivo,
            NULL                                        AS grupo_usuario,
            CAST(event_id AS CHAR)                      AS evento
        FROM biostar_event
        WHERE datetime >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
        ORDER BY datetime ASC
    """)

    with get_engine().connect() as conn:
        rows = conn.execute(query).mappings().all()

    return [dict(r) for r in rows]
