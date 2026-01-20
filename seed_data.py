import sqlite3
from werkzeug.security import generate_password_hash

DB_PATH = "azyco_pagos.db"

def seed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Usuarios (Noemí admin + vendedor demo)
    usuarios = [
        ("Noemí", "noemi@azyco.com", "admin123", "admin"),
        ("Vendedor Demo", "vendedor@azyco.com", "vendedor123", "vendedor"),
    ]

    for nombre, email, raw_password, rol in usuarios:
        password_hash = generate_password_hash(raw_password)
        try:
            cur.execute(
                """
                INSERT INTO usuarios (nombre, email, password_hash, rol)
                VALUES (?, ?, ?, ?)
                """,
                (nombre, email, password_hash, rol),
            )
        except sqlite3.IntegrityError:
            # Ya existe el usuario
            pass

    # Cuentas bancarias (ajusta alias / número según tus datos reales)
    cuentas = [
        ("BBVA", "BBVA Cuenta 1", "0102520877", None),
        ("BBVA", "BBVA Cuenta 2", "XXXXXXXXXX2", None),
        ("BBVA", "BBVA Cuenta 3", "XXXXXXXXXX3", None),
        ("BANAMEX", "Banamex Cuenta 1", "YYYYYYYYYY1", None),
        ("BANORTE", "Banorte Cuenta 1", "ZZZZZZZZZZ1", None),
    ]

    for banco, alias, numero_cuenta, clabe in cuentas:
        cur.execute(
            """
            INSERT INTO cuentas_bancarias (banco, alias, numero_cuenta, clabe)
            VALUES (?, ?, ?, ?)
            """,
            (banco, alias, numero_cuenta, clabe),
        )

    conn.commit()
    conn.close()
    print("Datos de prueba insertados.")

if __name__ == "__main__":
    seed()
