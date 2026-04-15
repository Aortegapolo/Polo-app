from sqlalchemy import text
from db import get_engine


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

    query_occupation = text("""
        SELECT
            CONCAT('O', CAST(bo.id AS CHAR))        AS id,
            bi.name                                  AS pista,
            bc.name                                  AS cat,
            brt.name                                 AS tipo,
            CAST(br.n_socio AS CHAR)                 AS nsocio,
            br.name                                  AS nombre,
            DATE_FORMAT(bo.check_in,  '%d-%m-%Y')   AS fecha,
            DATE_FORMAT(bo.check_in,  '%H:%i')       AS checkin,
            DATE_FORMAT(bo.check_out, '%H:%i')       AS checkout
        FROM book_occupation bo
        JOIN  book_item             bi  ON bo.item_id        = bi.id
        JOIN  book_category         bc  ON bi.category_id    = bc.id
        LEFT JOIN book_reservation_type brt ON bo.type_id    = brt.id
        LEFT JOIN book_reservation  br  ON bo.reservation_id = br.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND bo.check_in > DATE_SUB(NOW(), INTERVAL 12 MONTH)
          AND bo.check_in <= NOW()
        ORDER BY bo.check_in ASC
    """)

    query_reservation = text("""
        SELECT
            CONCAT('R', CAST(br.id AS CHAR))                        AS id,
            bi.name                                                  AS pista,
            bc.name                                                  AS cat,
            brt.name                                                 AS tipo,
            CAST(br.n_socio AS CHAR)                                 AS nsocio,
            br.name                                                  AS nombre,
            DATE_FORMAT(br.reservation_date, '%d-%m-%Y')            AS fecha,
            TIME_FORMAT(SEC_TO_TIME(br.time_in  * 60), '%H:%i')     AS checkin,
            TIME_FORMAT(SEC_TO_TIME(br.time_end * 60), '%H:%i')     AS checkout
        FROM book_reservation br
        JOIN  book_category         bc  ON br.category_id = bc.id
        LEFT JOIN book_item         bi  ON br.item_id     = bi.id
        LEFT JOIN book_reservation_type brt ON br.type_id = brt.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND br.reservation_date > DATE_SUB(NOW(), INTERVAL 12 MONTH)
          AND br.reservation_date <= NOW()
          AND br.client_id IS NULL
          AND br.id NOT IN (
              SELECT reservation_id
              FROM book_occupation
              WHERE reservation_id IS NOT NULL
          )
        ORDER BY br.reservation_date ASC, br.time_in ASC
    """)

    with engine.connect() as conn:
        rows_occ = [dict(r) for r in conn.execute(query_occupation).mappings().all()]
        rows_res = [dict(r) for r in conn.execute(query_reservation).mappings().all()]

    return rows_occ + rows_res
