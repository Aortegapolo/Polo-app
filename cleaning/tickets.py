from sqlalchemy import text
from db import get_engine


def clean():
    """
    Consulta y limpia los datos de la tabla ticket + joins.

    Devuelve una lista de dicts con las columnas:
        id          : str   identificador del ticket
        creado      : str   "DD-MM-YYYY"
        espera      : int   horas desde creación hasta ahora
        area        : str   área del ticket
        categoria   : str   tipo/categoría del ticket
        responsable : str   nombre del responsable
        asunto      : str   asunto del ticket
        socio       : str   nombre del socio que lo creó
        respuesta   : str   "Pendiente" o "Ok"
        estado      : str   "Abierto" o "Cerrado"
    """
    query = text("""
        SELECT
            CAST(t.id AS CHAR)                              AS id,
            DATE_FORMAT(t.created, '%d-%m-%Y')              AS creado,
            COALESCE(TIMESTAMPDIFF(HOUR, t.created, NOW()), 0) AS espera,
            ta.name                                         AS area,
            tt.name                                         AS categoria,
            u.name                                          AS responsable,
            t.subject                                       AS asunto,
            CONCAT(c.first_name, ' ', c.last_name)          AS socio,
            CASE t.status WHEN 1 THEN 'Pendiente' ELSE 'Ok' END      AS respuesta,
            CASE t.status WHEN 3 THEN 'Cerrado'   ELSE 'Abierto' END AS estado
        FROM ticket t
        LEFT JOIN client      c  ON t.client_id = c.id
        LEFT JOIN user        u  ON t.user_id   = u.id
        LEFT JOIN ticket_area ta ON t.area_id   = ta.id
        LEFT JOIN ticket_type tt ON t.type_id   = tt.id
        WHERE t.created >= '2026-01-01'
          AND t.created <= NOW()
    """)

    with get_engine().connect() as conn:
        rows = conn.execute(query).mappings().all()

    return [dict(r) for r in rows]
