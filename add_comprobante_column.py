import sqlite3

DB_PATH = "azyco_pagos.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Intentamos agregar la columna. Si ya existe, ignoramos el error.
    try:
        cur.execute("ALTER TABLE ventas ADD COLUMN comprobante_filename TEXT;")
        print("Columna comprobante_filename agregada a ventas.")
    except Exception as e:
        print("Posiblemente la columna ya existe:", e)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
