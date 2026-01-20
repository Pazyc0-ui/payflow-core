import sqlite3
from datetime import datetime, date
from typing import List, Tuple, Optional, Dict, Any

DB_PATH = "azyco_pagos.db"


def _parse_date_yyyy_mm_dd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_datetime_yyyy_mm_dd_hh_mm_ss(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _score_candidate(pago: sqlite3.Row, venta: sqlite3.Row) -> float:
    """
    Calcula un puntaje de compatibilidad entre un pago y una venta.
    Mayor puntaje = mayor probabilidad de que correspondan.
    """

    score = 0.0

    # 1) Folio en referencia / concepto
    folio = (venta["folio"] or "").strip().lower()
    folio_sin_guiones = folio.replace("-", "").replace(" ", "")

    ref = (pago["referencia"] or "").lower()
    ref_amp = (pago["referencia_ampliada"] or "").lower()
    concepto = (pago["concepto"] or "").lower()

    texto_pago = f"{ref} {ref_amp} {concepto}"
    texto_pago_sin_guiones = texto_pago.replace("-", "").replace(" ", "")

    if folio and (folio in texto_pago or folio_sin_guiones in texto_pago_sin_guiones):
        score += 70.0  # match muy fuerte por folio

    # 2) Diferencia de fechas
    fecha_pago = _parse_date_yyyy_mm_dd(pago["fecha_operacion"])
    fecha_venta = None
    if venta["fecha_creacion"]:
        # fecha_creacion viene como "YYYY-MM-DD HH:MM:SS"
        fecha_venta_dt = _parse_datetime_yyyy_mm_dd_hh_mm_ss(venta["fecha_creacion"])
        if fecha_venta_dt:
            fecha_venta = fecha_venta_dt.date()

    if fecha_pago and fecha_venta:
        dias = abs((fecha_pago - fecha_venta).days)
        if dias == 0:
            score += 20.0
        elif dias == 1:
            score += 10.0
        elif dias <= 3:
            score += 5.0

    # 3) Antigüedad de la venta respecto al pago (ventas recientes tienen prioridad)
    if fecha_pago and venta["fecha_creacion"]:
        venta_dt = _parse_datetime_yyyy_mm_dd_hh_mm_ss(venta["fecha_creacion"])
        if venta_dt:
            pago_dt = datetime.combine(fecha_pago, datetime.min.time())
            diff_horas = (pago_dt - venta_dt).total_seconds() / 3600.0
            if 0 <= diff_horas <= 4:
                score += 10.0
            elif 0 <= diff_horas <= 24:
                score += 5.0

    # 4) Estado de la venta
    if venta["estado_banco"] == "EN_ESPERA_CONCILIACION":
        score += 15.0

    return score


def run_conciliacion() -> int:
    """
    Motor de conciliación "inteligente".
    Recorre pagos_detectados PENDIENTES y trata de emparejarlos con ventas.

    Reglas base:
      - mismo banco/cuenta (cuenta_bancaria_id)
      - mismo monto (tolerancia centavos)
      - si hay UNA sola venta candidata por monto+cuenta -> MATCH directo
      - si hay varias candidatas -> usamos score (folio en referencia, fechas, etc.)
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Obtener pagos pendientes
    cur.execute(
        """
        SELECT *
        FROM pagos_detectados
        WHERE estado_conciliacion = 'PENDIENTE'
        ORDER BY fecha_operacion ASC, id ASC
        """
    )
    pagos = cur.fetchall()

    matches = 0

    for p in pagos:
        cuenta_bancaria_id = p["cuenta_bancaria_id"]
        if cuenta_bancaria_id is None:
            # Sin cuenta ligada, mejor no arriesgar
            continue

        monto_pago = p["monto"]
        if monto_pago is None:
            continue

        # 2. Buscar ventas candidatas por monto + cuenta + estado != PAGADO
        cur.execute(
            """
            SELECT *
            FROM ventas
            WHERE cuenta_bancaria_id = ?
            AND estado_banco != 'PAGADO'
            AND ABS(monto - ?) < 0.01
            """,
            (cuenta_bancaria_id, monto_pago),
        )
        ventas_posibles = cur.fetchall()

        if not ventas_posibles:
            # No hay ninguna venta que coincida en monto + cuenta
            continue

        # 🔹 CASO 1: Solo hay UNA venta candidata -> MATCH directo (sin score)
        if len(ventas_posibles) == 1:
            v = ventas_posibles[0]

            cur.execute(
                """
                UPDATE pagos_detectados
                SET estado_conciliacion = 'MATCH',
                    venta_id = ?
                WHERE id = ?
                """,
                (v["id"], p["id"]),
            )

            cur.execute(
                """
                UPDATE ventas
                SET estado_banco = 'PAGADO',
                    fecha_ultimo_cambio = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (v["id"],),
            )

            matches += 1
            continue  # pasar al siguiente pago

        # 🔹 CASO 2: Varias ventas candidatas -> usar score
        scored: List[Tuple[float, sqlite3.Row]] = []
        for v in ventas_posibles:
            s = _score_candidate(p, v)
            scored.append((s, v))

        # Ordenar por score descendente
        scored.sort(key=lambda x: x[0], reverse=True)

        mejor_score, mejor_venta = scored[0]

        MIN_SCORE_AUTOMATICO = 20.0

        # Si el mejor score es muy bajo, no tomamos decisión automática
        if mejor_score < MIN_SCORE_AUTOMATICO:
            cur.execute(
                """
                UPDATE pagos_detectados
                SET estado_conciliacion = 'REVISAR'
                WHERE id = ?
                """,
                (p["id"],),
            )
            continue

        # ¿Hay más de un candidato con score cercano?
        if len(scored) > 1:
            segundo_score = scored[1][0]
            if segundo_score >= mejor_score * 0.7:
                # Ambiguo -> REVISAR
                cur.execute(
                    """
                    UPDATE pagos_detectados
                    SET estado_conciliacion = 'REVISAR'
                    WHERE id = ?
                    """,
                    (p["id"],),
                )
                continue

        # 4. Si llegamos aquí, tenemos un candidato claro -> MATCH
        cur.execute(
            """
            UPDATE pagos_detectados
            SET estado_conciliacion = 'MATCH',
                venta_id = ?
            WHERE id = ?
            """,
            (mejor_venta["id"], p["id"]),
        )

        cur.execute(
            """
            UPDATE ventas
            SET estado_banco = 'PAGADO',
                fecha_ultimo_cambio = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (mejor_venta["id"],),
        )

        matches += 1

    conn.commit()
    conn.close()
    return matches
