import pandas as pd
from db import get_engine


def clean():
    """
    Consulta y limpia los datos de la tabla biostar_event.

    Devuelve una lista de dicts con las columnas:
        fecha         : str  "YYYY-MM-DD HH:MM:SS"
        dispositivo   : str  nombre del dispositivo de acceso
        grupo_usuario : str  grupo al que pertenece el usuario
        evento        : str  tipo de evento registrado

    Pasos de limpieza: pendiente de definir por el usuario.
    """
    engine = get_engine()

    # ── EXTRACCIÓN ────────────────────────────────────────────────
    # Columnas reales de biostar_event:
    #   datetime    → fecha
    #   device_name → dispositivo
    #   client_name → usuario (pendiente: grupo_usuario vendrá de join con client)
    #   event_id    → identificador numérico del tipo de evento
    query = """
        SELECT
            datetime    AS fecha,
            device_name AS dispositivo,
            client_name AS usuario,
            event_id
        FROM biostar_event
    """
    df = pd.read_sql(query, engine)

    # ── LIMPIEZA ─────────────────────────────────────────────────
    # Los pasos de limpieza se añadirán aquí según indicaciones

    # ── SALIDA ───────────────────────────────────────────────────
    # grupo_usuario: pendiente de join con tabla client
    # De momento se deja vacío para que el dashboard lo trate como "No Data"
    df['grupo_usuario'] = None
    df['evento'] = df['event_id'].astype(str)
    df['fecha']  = df['fecha'].astype(str)

    return df[["fecha", "dispositivo", "grupo_usuario", "evento"]].to_dict(orient="records")
