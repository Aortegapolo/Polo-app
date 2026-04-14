import pandas as pd
from db import get_engine


def _minutos_a_hhmm(minutos):
    """Convierte minutos desde medianoche (int) a string 'HH:MM'."""
    h = minutos // 60
    m = minutos % 60
    return f"{h:02d}:{m:02d}"


def clean():
    """
    Consulta y limpia los datos de pistas combinando dos fuentes:

      1. book_occupation  → fuente principal con la mayoría de registros.
      2. book_reservation → registros complementarios sin cliente asociado
                            (client_id IS NULL) que no aparecen en book_occupation.

    Devuelve una lista de dicts con las columnas:
        id       : str  ID del registro
        pista    : str  nombre de la pista
        cat      : str  categoría (Padel, Tenis, ...)
        tipo     : str  tipo de reserva (Online, Sección, ...)
        nsocio   : str  número de socio (puede ser None)
        nombre   : str  nombre del socio (puede ser None)
        fecha    : str  "DD-MM-YYYY"
        checkin  : str  "HH:MM"
        checkout : str  "HH:MM"
    """
    engine = get_engine()

    # ── FUENTE 1: book_occupation ─────────────────────────────────
    query_occupation = """
        SELECT
            bo.id,
            bi.name      AS pista,
            bc.name      AS cat,
            brt.name     AS tipo,
            br.n_socio   AS nsocio,
            br.name      AS nombre,
            bo.check_in,
            bo.check_out
        FROM book_occupation bo
        JOIN  book_item             bi  ON bo.item_id        = bi.id
        JOIN  book_category         bc  ON bi.category_id    = bc.id
        LEFT JOIN book_reservation_type brt ON bo.type_id    = brt.id
        LEFT JOIN book_reservation  br  ON bo.reservation_id = br.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND bo.check_in > '2026-01-01 00:00:00'
          AND bo.check_in <= NOW()
        ORDER BY bo.check_in ASC
    """
    df_occ = pd.read_sql(query_occupation, engine)

    df_occ['check_in']  = pd.to_datetime(df_occ['check_in'])
    df_occ['check_out'] = pd.to_datetime(df_occ['check_out'])

    df_occ['fecha']    = df_occ['check_in'].dt.strftime('%d-%m-%Y')
    df_occ['checkin']  = df_occ['check_in'].dt.strftime('%H:%M')
    df_occ['checkout'] = df_occ['check_out'].dt.strftime('%H:%M')

    # ── FUENTE 2: book_reservation (client_id IS NULL) ────────────
    query_reservation = """
        SELECT
            br.id,
            bi.name      AS pista,
            bc.name      AS cat,
            brt.name     AS tipo,
            br.n_socio   AS nsocio,
            br.name      AS nombre,
            br.reservation_date,
            br.time_in,
            br.time_end
        FROM book_reservation br
        JOIN  book_category         bc  ON br.category_id = bc.id
        LEFT JOIN book_item         bi  ON br.item_id     = bi.id
        LEFT JOIN book_reservation_type brt ON br.type_id = brt.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND br.reservation_date > '2025-12-31 00:00:00'
          AND br.reservation_date <= NOW()
          AND br.client_id IS NULL
          AND br.id NOT IN (
            SELECT reservation_id 
            FROM book_occupation 
            WHERE reservation_id IS NOT NULL
        )
        ORDER BY br.reservation_date ASC, br.time_in ASC
    """
    df_res = pd.read_sql(query_reservation, engine)

    df_res['reservation_date'] = pd.to_datetime(df_res['reservation_date'])

    df_res['fecha']    = df_res['reservation_date'].dt.strftime('%d-%m-%Y')
    df_res['checkin']  = df_res['time_in'].apply(_minutos_a_hhmm)
    df_res['checkout'] = df_res['time_end'].apply(_minutos_a_hhmm)

    # ── LIMPIEZA ─────────────────────────────────────────────────
    # Los pasos de limpieza se añadirán aquí según indicaciones

    # ── UNIÓN Y SALIDA ───────────────────────────────────────────
    cols = ['id', 'pista', 'cat', 'tipo', 'nsocio', 'nombre', 'fecha', 'checkin', 'checkout']

    df_occ['id'] = 'O' + df_occ['id'].astype(str)
    df_res['id'] = 'R' + df_res['id'].astype(str)

    out = pd.concat(
        [df_occ[cols], df_res[cols]],
        ignore_index=True
    )

    out['id'] = out['id'].astype(str)

    # Reemplazar NaN por None para que serialice como null en JSON
    out = out.where(pd.notnull(out), other=None)
    return out.to_dict(orient='records')
