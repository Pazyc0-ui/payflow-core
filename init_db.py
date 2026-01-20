import sqlite3

DB_PATH = "azyco_pagos.db"

schema = """
CREATE TABLE IF NOT EXISTS usuarios (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre          TEXT NOT NULL,
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    rol             TEXT NOT NULL CHECK (rol IN ('vendedor', 'admin', 'direccion')),
    activo          INTEGER NOT NULL DEFAULT 1,
    creado_en       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cuentas_bancarias (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    banco           TEXT NOT NULL,
    alias           TEXT NOT NULL,
    numero_cuenta   TEXT,
    clabe           TEXT,
    moneda          TEXT NOT NULL DEFAULT 'MXN',
    activa          INTEGER NOT NULL DEFAULT 1,
    creado_en       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ventas (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    folio                   TEXT NOT NULL,
    cliente_nombre          TEXT NOT NULL,
    monto                   REAL NOT NULL,
    moneda                  TEXT NOT NULL DEFAULT 'MXN',
    cuenta_bancaria_id      INTEGER NOT NULL,
    vendedor_id             INTEGER NOT NULL,
    estado_banco            TEXT NOT NULL CHECK (
                                estado_banco IN ('PENDIENTE', 'EN_ESPERA_CONCILIACION', 'PAGADO', 'REVISAR')
                            ) DEFAULT 'PENDIENTE',
    fecha_creacion          DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_ultimo_cambio     DATETIME DEFAULT CURRENT_TIMESTAMP,
    nota                    TEXT,
    FOREIGN KEY (cuenta_bancaria_id) REFERENCES cuentas_bancarias(id),
    FOREIGN KEY (vendedor_id)        REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS pagos_detectados (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    banco                   TEXT NOT NULL,
    cuenta_bancaria_id      INTEGER,
    fecha_operacion         DATE NOT NULL,
    hora_operacion          TEXT,
    monto                   REAL NOT NULL,
    moneda                  TEXT NOT NULL DEFAULT 'MXN',
    referencia              TEXT,
    referencia_ampliada     TEXT,
    concepto                TEXT,
    saldo_posterior         REAL,
    fuente_archivo          TEXT,
    hash_unico              TEXT UNIQUE,
    estado_conciliacion     TEXT NOT NULL CHECK (
                                estado_conciliacion IN ('PENDIENTE', 'MATCH', 'REVISAR')
                            ) DEFAULT 'PENDIENTE',
    venta_id                INTEGER,
    creado_en               DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cuenta_bancaria_id) REFERENCES cuentas_bancarias(id),
    FOREIGN KEY (venta_id)             REFERENCES ventas(id)
);
"""

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(schema)
    conn.commit()
    conn.close()
    print("Base de datos creada en", DB_PATH)

if __name__ == "__main__":
    init_db()
