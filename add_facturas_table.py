import sqlite3

DB_PATH = "azyco_pagos.db"

schema = """
CREATE TABLE IF NOT EXISTS facturas (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    folio           TEXT NOT NULL,
    cliente_nombre  TEXT,
    rfc             TEXT,
    monto_total     REAL NOT NULL,
    moneda          TEXT NOT NULL DEFAULT 'MXN',
    fecha_emision   DATE NOT NULL,
    venta_id        INTEGER,
    estado          TEXT NOT NULL DEFAULT 'EMITIDA',
    creado_en       DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (venta_id) REFERENCES ventas(id)
);

CREATE INDEX IF NOT EXISTS idx_facturas_folio ON facturas(folio);
CREATE INDEX IF NOT EXISTS idx_facturas_fecha ON facturas(fecha_emision);
"""

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(schema)
    conn.commit()
    conn.close()
    print("Tabla facturas creada/actualizada correctamente.")

if __name__ == "__main__":
    main()
