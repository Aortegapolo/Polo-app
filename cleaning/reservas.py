import pandas as pd
from db import get_engine


def _min_to_hhmm(minutes):
    """Convierte minutos desde medianoche a string 'HH:MM'."""
    if minutes is None:
        return ''
    h = int(minutes) // 60
    m = int(minutes) % 60
    return f"{h:02d}:{m:02d}"


def clean():
    """
    Cruza reservas Online de Padel con sus ocupaciones correspondientes.

    Devuelve un dict con:
        kpis            : totales y porcentajes
        ranking_titular : titulares que reservan y no viene nadie
        ranking_persona : personas que aparecen en reserva pero no en ocupación
        reservas        : lista detallada de cada reserva con su estado
    """
    engine = get_engine()

    # ── QUERY RESERVAS ONLINE ─────────────────────────────────────
    query_reservas = """
        SELECT
            br.id                                                       AS reserva_id,
            bc.name                                                     AS cat,
            bi.name                                                     AS pista,
            br.reservation_date,
            br.time_in,
            br.time_end,
            TIME_FORMAT(SEC_TO_TIME(br.time_in  * 60), '%%H:%%i')        AS checkin,
            TIME_FORMAT(SEC_TO_TIME(br.time_end * 60), '%%H:%%i')       AS checkout,
            CAST(c_titular.n_socio AS CHAR)                             AS titular_nsocio,
            CONCAT(c_titular.first_name, ' ', c_titular.last_name,
                   ' ', IFNULL(c_titular.second_last_name, ''))         AS titular_nombre,
            CAST(c_partner.n_socio AS CHAR)                             AS partner_nsocio,
            CONCAT(c_partner.first_name, ' ', c_partner.last_name,
                   ' ', IFNULL(c_partner.second_last_name, ''))         AS partner_nombre
        FROM book_reservation br
        JOIN  book_category          bc  ON br.category_id = bc.id
        JOIN  book_reservation_type  brt ON br.type_id     = brt.id
        LEFT JOIN book_item          bi  ON br.item_id     = bi.id
        LEFT JOIN client c_titular        ON br.client_id  = c_titular.id
        LEFT JOIN book_reservation_partner brp ON br.id    = brp.reservation_id
        LEFT JOIN client c_partner        ON brp.client_id = c_partner.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND brt.name  = 'Online'
          AND br.canceled IS NULL
          AND br.exclude_client = 0
          AND br.reservation_date >= '2026-01-01'
          AND br.reservation_date <= NOW()
        ORDER BY br.reservation_date ASC, br.id ASC
    """

    # ── QUERY OCUPACIONES ─────────────────────────────────────────
    query_ocupaciones = """
        SELECT
            bo.id                                                       AS id_ocupacion,
            bo.reservation_id                                           AS id_reserva,
            CAST(c.n_socio AS CHAR)                                     AS nsocio,
            CONCAT(c.first_name, ' ', c.last_name,
                   ' ', IFNULL(c.second_last_name, ''))                 AS nombre
        FROM book_occupation bo
        JOIN  book_item             bi  ON bo.item_id      = bi.id
        JOIN  book_category         bc  ON bi.category_id  = bc.id
        LEFT JOIN book_occupation_client boc ON bo.id      = boc.book_occupation_id
        LEFT JOIN client c ON boc.client_id                = c.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND bo.check_in >= '2026-01-01'
          AND bo.check_in <= NOW()
          AND bo.reservation_id IS NOT NULL
    """

    # ── QUERY OCUPACIONES SIN RESERVA ────────────────────────────
    query_ocp_sin_res = """
        SELECT COUNT(DISTINCT bo.id) AS cnt
        FROM book_occupation bo
        JOIN  book_item             bi  ON bo.item_id      = bi.id
        JOIN  book_category         bc  ON bi.category_id  = bc.id
        JOIN  book_reservation_type brt ON bo.type_id      = brt.id
        WHERE bc.name IN ('Padel', 'Tenis')
          AND brt.name = 'Online'
          AND bo.check_in >= '2026-01-01'
          AND bo.check_in <= NOW()
          AND (
              bo.reservation_id IS NULL
              OR bo.reservation_id NOT IN (
                  SELECT id FROM book_reservation
                  WHERE canceled IS NULL
                    AND exclude_client = 0
                    AND reservation_date >= '2026-01-01'
                    AND reservation_date <= NOW()
              )
          )
    """

    df_res        = pd.read_sql(query_reservas, engine)
    df_occ        = pd.read_sql(query_ocupaciones, engine)
    df_ocp_sin    = pd.read_sql(query_ocp_sin_res, engine)
    ocp_sin_reserva = int(df_ocp_sin['cnt'].iloc[0])

    # ── AGRUPAR RESERVAS ──────────────────────────────────────────
    # Una fila por partner → agrupar por reserva_id
    reservas = {}
    for _, row in df_res.iterrows():
        rid = int(row['reserva_id'])
        if rid not in reservas:
            end_dt    = pd.Timestamp(row['reservation_date']).normalize() + pd.Timedelta(minutes=int(row['time_end'] or 0))
            ha_pasado = bool(end_dt < pd.Timestamp.now())
            reservas[rid] = {
                'reserva_id':     rid,
                'cat':            row['cat'] or '',
                'pista':          (row['pista'] or '').strip(),
                'fecha':          pd.to_datetime(row['reservation_date']).strftime('%d-%m-%Y'),
                'checkin':        row['checkin'] or '',
                'checkout':       row['checkout'] or '',
                'titular':        (row['titular_nombre'] or '').strip(),
                'titular_nsocio': row['titular_nsocio'],
                'partners':       [],
                'partner_nsocio': [],
                'ha_pasado':      ha_pasado,
            }
        pns = row['partner_nsocio']
        if pns is not None and pns not in reservas[rid]['partner_nsocio']:
            reservas[rid]['partners'].append((row['partner_nombre'] or '').strip())
            reservas[rid]['partner_nsocio'].append(pns)

    # ── AGRUPAR OCUPACIONES ───────────────────────────────────────
    # { reservation_id: { socios: set, nombres: {nsocio: nombre} } }
    ocupaciones = {}
    for _, row in df_occ.iterrows():
        rid = row['id_reserva']
        if rid is None:
            continue
        rid = int(rid)
        if rid not in ocupaciones:
            ocupaciones[rid] = {'socios': set(), 'nombres': {}, 'id_occ': int(row['id_ocupacion'])}
        ns = row['nsocio']
        if ns is not None:
            ocupaciones[rid]['socios'].add(str(ns))
            ocupaciones[rid]['nombres'][str(ns)] = (row['nombre'] or '').strip()

    # ── CRUZAR Y CLASIFICAR ───────────────────────────────────────
    result = []
    for rid, res in reservas.items():
        # Conjunto de socios en la reserva
        res_socios = set()
        if res['titular_nsocio']:
            res_socios.add(str(res['titular_nsocio']))
        for ns in res['partner_nsocio']:
            if ns:
                res_socios.add(str(ns))

        occ_data   = ocupaciones.get(rid)
        ocupada    = occ_data is not None
        occ_socios = occ_data['socios'] if occ_data else set()

        incoherente = ocupada and (res_socios != occ_socios)
        ausentes    = res_socios - occ_socios

        result.append({
            **res,
            'ocupada':         ocupada,
            'incoherente':     incoherente,
            'id_ocupacion':    occ_data['id_occ'] if occ_data else None,
            'ocupantes':       list(occ_data['nombres'].values()) if occ_data else [],
            'ausentes_nsocio': list(ausentes),
        })

    # ── KPIs ──────────────────────────────────────────────────────
    total        = len(result)
    no_ocupadas  = sum(1 for r in result if not r['ocupada'] and r['ha_pasado'])
    incoherentes = sum(1 for r in result if r['incoherente'])

    kpis = {
        'total_online':        total,
        'no_ocupadas':         no_ocupadas,
        'pct_no_ocupadas':     round(no_ocupadas      / total * 100, 1) if total else 0.0,
        'incoherentes':        incoherentes,
        'pct_incoherentes':    round(incoherentes     / total * 100, 1) if total else 0.0,
        'ocp_sin_reserva':     ocp_sin_reserva,
        'pct_ocp_sin_reserva': round(ocp_sin_reserva  / total * 100, 1) if total else 0.0,
    }

    # ── RANKING TITULAR (solo no ocupadas) ────────────────────────
    titular_count = {}
    for r in result:
        if not r['ocupada'] and r['ha_pasado']:
            ns  = r['titular_nsocio']
            key = ns or '__sin_socio__'
            if key not in titular_count:
                titular_count[key] = {'nsocio': ns, 'nombre': r['titular'], 'count': 0}
            titular_count[key]['count'] += 1

    ranking_titular = sorted(titular_count.values(), key=lambda x: x['count'], reverse=True)[:20]

    # ── RANKING PERSONA (no ocupadas + incoherentes) ──────────────
    # Mapa nsocio → nombre para lookup
    nsocio_nombre = {}
    for r in result:
        if r['titular_nsocio']:
            nsocio_nombre[r['titular_nsocio']] = r['titular']
        for ns, nm in zip(r['partner_nsocio'], r['partners']):
            if ns:
                nsocio_nombre[ns] = nm

    persona_count = {}
    for r in result:
        if (not r['ocupada'] and r['ha_pasado']) or r['incoherente']:
            res_socios = set()
            if r['titular_nsocio']:
                res_socios.add(str(r['titular_nsocio']))
            for ns in r['partner_nsocio']:
                if ns:
                    res_socios.add(str(ns))

            occ_socios = ocupaciones[r['reserva_id']]['socios'] if r['reserva_id'] in ocupaciones else set()
            ausentes   = res_socios - occ_socios

            for ns in ausentes:
                if ns not in persona_count:
                    persona_count[ns] = {
                        'nsocio': ns,
                        'nombre': nsocio_nombre.get(ns, ''),
                        'count':  0,
                    }
                persona_count[ns]['count'] += 1

    ranking_persona = sorted(persona_count.values(), key=lambda x: x['count'], reverse=True)[:20]

    return {
        'kpis':            kpis,
        'ranking_titular': ranking_titular,
        'ranking_persona': ranking_persona,
        'reservas':        result,
    }
