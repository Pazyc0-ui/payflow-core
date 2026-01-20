import sqlite3

DB_PATH = "azyco_pagos.db"

TABLAS_A_LIMPIAR = [
    "pagos_detectados",
    "ventas",
    "archivos_movimientos",  # si tu tabla se llama distinto, cámbialo
]

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for tabla in TABLAS_A_LIMPIAR:
        try:
            print(f"Limpiando tabla {tabla}...")
            cur.execute(f"DELETE FROM {tabla};")
        except Exception as e:
            print(f"No se pudo limpiar {tabla}: {e}")

    # Opcional: reordenar IDs (no es obligatorio)
    # OJO: solo haz esto si no te importa que los IDs se reciclen
    try:
        cur.execute("DELETE FROM sqlite_sequence WHERE name IN (?, ?, ?);", TABLAS_A_LIMPIAR)
    except Exception as e:
        print("No se pudo resetear sqlite_sequence (no es grave):", e)

    conn.commit()
    conn.close()
    print("Listo. Datos de movimientos borrados, usuarios y cuentas intactos.")

if __name__ == "__main__":
    main()
