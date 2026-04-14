import pandas as pd
from db import get_engine


# Mapeo de status numérico a los valores de texto que espera el dashboard
STATUS_RESPUESTA = {1: 'Pendiente', 2: 'Ok', 3: 'Ok'}
STATUS_ESTADO    = {1: 'Abierto',   2: 'Abierto', 3: 'Cerrado'}


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

    Pasos de limpieza: pendiente de definir por el usuario.
    """
    engine = get_engine()

    query = """
        SELECT
            t.id,
            t.created,
            TIMESTAMPDIFF(HOUR, t.created, NOW())   AS espera,
            ta.name                                  AS area,
            tt.name                                  AS categoria,
            u.name                                   AS responsable,
            t.subject                                AS asunto,
            CONCAT(c.first_name, ' ', c.last_name)  AS socio,
            t.status
        FROM ticket t
        LEFT JOIN client      c  ON t.client_id = c.id
        LEFT JOIN user        u  ON t.user_id   = u.id
        LEFT JOIN ticket_area ta ON t.area_id   = ta.id
        LEFT JOIN ticket_type tt ON t.type_id   = tt.id
    """
    df = pd.read_sql(query, engine)

    # ── LIMPIEZA ─────────────────────────────────────────────────
    # Los pasos de limpieza se añadirán aquí según indicaciones

    # ── FORMATEO ─────────────────────────────────────────────────
    df['creado']    = pd.to_datetime(df['created']).dt.strftime('%d-%m-%Y')
    df['espera']    = df['espera'].fillna(0).astype(int)
    df['respuesta'] = df['status'].map(STATUS_RESPUESTA)
    df['estado']    = df['status'].map(STATUS_ESTADO)
    df['id']        = df['id'].astype(str)

    # ── SALIDA ───────────────────────────────────────────────────
    out = df[['id', 'creado', 'espera', 'area', 'categoria',
              'responsable', 'asunto', 'socio', 'respuesta', 'estado']]
    out = out.where(pd.notnull(out), other=None)
    return out.to_dict(orient='records')
