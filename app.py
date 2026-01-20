from flask import Flask, render_template, request, redirect, url_for, session, g
from flask import Response
import sqlite3
from werkzeug.security import check_password_hash
from functools import wraps
from datetime import datetime, date
import pandas as pd
import hashlib
import numpy as np
from modules.conciliacion import run_conciliacion
import os
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import send_from_directory

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
COMPROBANTES_FOLDER = os.path.join(BASE_DIR, "uploads", "comprobantes")
os.makedirs(COMPROBANTES_FOLDER, exist_ok=True)

ALLOWED_COMPROBANTES = {"png", "jpg", "jpeg", "pdf"}

def allowed_comprobante(filename: str) -> bool:
    if "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_COMPROBANTES


DB_PATH = "azyco_pagos.db"

app = Flask(__name__)
app.secret_key = "cambia_esto_por_algo_mas_seguro"  # cambia en producción

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()
def login_required(view_func):
    @wraps(view_func)
    def wrapped_view(**kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return view_func(**kwargs)
    return wrapped_view

def role_required(*roles):
    def decorator(view_func):
        @wraps(view_func)
        def wrapped_view(**kwargs):
            if "user_id" not in session:
                return redirect(url_for("login"))
            if session.get("user_rol") not in roles:
                return redirect(url_for("index"))
            return view_func(**kwargs)
        return wrapped_view
    return decorator


def roles_required(*roles):
    def decorator(view):
        @wraps(view)
        def wrapped_view(**kwargs):
            if "user_id" not in session:
                return redirect(url_for("login"))
            if session.get("rol") not in roles:
                return "No autorizado", 403
            return view(**kwargs)
        return wrapped_view
    return decorator



# ---------- Rutas de autenticación ----------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")

        db = get_db()
        cur = db.execute(
            "SELECT * FROM usuarios WHERE email = ? AND activo = 1",
            (email,)
        )
        user = cur.fetchone()

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["user_rol"] = user["rol"]
            session["user_nombre"] = user["nombre"]

            # Redirigir según rol
            if user["rol"] == "admin":
                return redirect(url_for("dashboard_admin"))
            elif user["rol"] == "vendedor":
                return redirect(url_for("dashboard_vendedor"))
            else:
                return redirect(url_for("dashboard_direccion"))
        else:
            error = "Usuario o contraseña incorrectos"
            return render_template("login.html", error=error)

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------- Dashboards básicos (placeholders) ----------

@app.route("/")
def index():
    if "user_id" not in session:
        return redirect(url_for("login"))

    rol = session.get("user_rol")
    if rol == "admin":
        return redirect(url_for("dashboard_admin"))
    elif rol == "vendedor":
        return redirect(url_for("dashboard_vendedor"))
    else:
        return redirect(url_for("dashboard_direccion"))

@app.route("/dashboard/vendedor")
@role_required("vendedor")
def dashboard_vendedor():
    return render_template("dashboard_vendedor.html")

@app.route("/conciliar")
@role_required("admin")
def conciliar():
    total = run_conciliacion()
    return render_template("conciliacion_resultado.html", total=total)

@app.route("/dashboard/admin")
@role_required("admin")
def dashboard_admin():
    return render_template("dashboard_admin.html")


@app.route("/dashboard/direccion")
@role_required("direccion")
def dashboard_direccion():
    return render_template("dashboard_direccion.html")

# ---------- Rutas de ventas (vendedor) ----------

@app.route("/ventas")
@role_required("vendedor")
def ventas_listado():
    db = get_db()
    vendedor_id = session.get("user_id")

    cur = db.execute(
        """
        SELECT v.*, c.alias AS cuenta_alias, c.banco
        FROM ventas v
        JOIN cuentas_bancarias c ON v.cuenta_bancaria_id = c.id
        WHERE v.vendedor_id = ?
        ORDER BY v.fecha_creacion DESC
        """,
        (vendedor_id,),
    )
    ventas = cur.fetchall()

    return render_template("ventas_listado.html", ventas=ventas)

@app.route("/comprobantes/<int:venta_id>")
@roles_required("admin", "vendedor", "direccion")
def descargar_comprobante(venta_id):
    db = get_db()
    cur = db.execute(
        """
        SELECT id, vendedor_id, comprobante_filename
        FROM ventas
        WHERE id = ?
        """,
        (venta_id,),
    )
    venta = cur.fetchone()
    if not venta:
        return "Venta no encontrada", 404

    # Control de acceso: si es vendedor, sólo su propia venta
    rol = session.get("rol")
    user_id = session.get("user_id")
    if rol == "vendedor" and venta["vendedor_id"] != user_id:
        return "No autorizado", 403

    filename = venta["comprobante_filename"]
    if not filename:
        return "Esta venta no tiene comprobante cargado.", 404

    filepath = os.path.join(COMPROBANTES_FOLDER, filename)
    if not os.path.exists(filepath):
        return "Archivo de comprobante no encontrado en el servidor.", 404

    return send_from_directory(COMPROBANTES_FOLDER, filename, as_attachment=False)



@app.route("/ventas/nueva", methods=["GET", "POST"])
@role_required("vendedor")
def ventas_nueva():
    db = get_db()
    vendedor_id = session.get("user_id")

    # Obtener cuentas bancarias activas para que el vendedor elija
    cur = db.execute(
        """
        SELECT id, banco, alias
        FROM cuentas_bancarias
        WHERE activa = 1
        ORDER BY banco, alias
        """
    )
    cuentas = cur.fetchall()

    if request.method == "POST":
        folio = request.form.get("folio")
        cliente_nombre = request.form.get("cliente_nombre")
        monto = request.form.get("monto")
        cuenta_bancaria_id = request.form.get("cuenta_bancaria_id")
        nota = request.form.get("nota")

        # Validación muy básica
        errors = []
        if not folio:
            errors.append("El folio es obligatorio.")
        if not cliente_nombre:
            errors.append("El nombre del cliente es obligatorio.")
        if not monto:
            errors.append("El monto es obligatorio.")
        else:
            try:
                monto = float(monto)
            except ValueError:
                errors.append("El monto debe ser numérico.")

        if not cuenta_bancaria_id:
            errors.append("Debes seleccionar una cuenta bancaria.")

        if errors:
            return render_template(
                "ventas_nueva.html",
                cuentas=cuentas,
                errors=errors,
                folio=folio,
                cliente_nombre=cliente_nombre,
                monto=request.form.get("monto"),
                cuenta_bancaria_id=cuenta_bancaria_id,
                nota=nota,
            )

        # Insertar en la BD
        ahora = datetime.now().isoformat(sep=" ", timespec="seconds")
        db.execute(
            """
            INSERT INTO ventas (
                folio, cliente_nombre, monto, cuenta_bancaria_id,
                vendedor_id, estado_banco, fecha_creacion, fecha_ultimo_cambio, nota
            )
            VALUES (?, ?, ?, ?, ?, 'PENDIENTE', ?, ?, ?)
            """,
            (
                folio,
                cliente_nombre,
                monto,
                cuenta_bancaria_id,
                vendedor_id,
                ahora,
                ahora,
                nota,
            ),
        )
        db.commit()
        try:
            run_conciliacion()
        except Exception:
            # Para prototipo, si falla la conciliación no tiramos la creación de la venta
            pass

        return redirect(url_for("ventas_listado"))

    return render_template("ventas_nueva.html", cuentas=cuentas)

@app.route("/ventas/<int:venta_id>/editar", methods=["GET", "POST"])
@role_required("vendedor")
def venta_editar(venta_id):
    db = get_db()
    vendedor_id = session.get("user_id")

    # Traer la venta y asegurar que pertenece a este vendedor
    cur = db.execute(
        """
        SELECT v.*, c.alias AS cuenta_alias, c.banco
        FROM ventas v
        JOIN cuentas_bancarias c ON v.cuenta_bancaria_id = c.id
        WHERE v.id = ? AND v.vendedor_id = ?
        """,
        (venta_id, vendedor_id),
    )
    venta = cur.fetchone()
    if not venta:
        return "Venta no encontrada o no pertenece a este agente de cobranza", 404

    # Solo permitir editar si NO está pagada
    if venta["estado_banco"] == "PAGADO":
        return "No se puede editar una venta ya pagada.", 400

    # Cuentas bancarias para el combo
    cur = db.execute(
        """
        SELECT id, banco, alias
        FROM cuentas_bancarias
        WHERE activa = 1
        ORDER BY banco, alias
        """
    )
    cuentas = cur.fetchall()

    errores = []
    mensaje_ok = None

    if request.method == "POST":
        folio = request.form.get("folio")
        cliente_nombre = request.form.get("cliente_nombre")
        monto = request.form.get("monto")
        cuenta_bancaria_id = request.form.get("cuenta_bancaria_id")
        nota = request.form.get("nota")

        # Validaciones básicas
        if not folio:
            errores.append("El folio es obligatorio.")
        if not cliente_nombre:
            errores.append("El nombre del cliente es obligatorio.")
        if not monto:
            errores.append("El monto es obligatorio.")
        else:
            try:
                monto = float(monto)
            except ValueError:
                errores.append("El monto debe ser numérico.")

        if not cuenta_bancaria_id:
            errores.append("Debes seleccionar una cuenta bancaria.")

        if not errores:
            db.execute(
                """
                UPDATE ventas
                SET folio = ?,
                    cliente_nombre = ?,
                    monto = ?,
                    cuenta_bancaria_id = ?,
                    nota = ?,
                    fecha_ultimo_cambio = CURRENT_TIMESTAMP
                WHERE id = ? AND vendedor_id = ?
                """,
                (
                    folio,
                    cliente_nombre,
                    monto,
                    cuenta_bancaria_id,
                    nota,
                    venta_id,
                    vendedor_id,
                ),
            )
            db.commit()
            mensaje_ok = "Venta actualizada correctamente."

            # Volver a leer la venta actualizada
            cur = db.execute(
                """
                SELECT v.*, c.alias AS cuenta_alias, c.banco
                FROM ventas v
                JOIN cuentas_bancarias c ON v.cuenta_bancaria_id = c.id
                WHERE v.id = ? AND v.vendedor_id = ?
                """,
                (venta_id, vendedor_id),
            )
            venta = cur.fetchone()

    return render_template(
        "ventas_editar.html",
        venta=venta,
        cuentas=cuentas,
        errores=errores,
        mensaje_ok=mensaje_ok,
    )

@app.route("/ventas/<int:venta_id>/eliminar", methods=["POST"])
@role_required("vendedor")
def venta_eliminar(venta_id):
    db = get_db()
    vendedor_id = session.get("user_id")

    # Verificar que la venta sea del vendedor y NO esté pagada
    cur = db.execute(
        """
        SELECT *
        FROM ventas
        WHERE id = ? AND vendedor_id = ?
        """,
        (venta_id, vendedor_id),
    )
    venta = cur.fetchone()
    if not venta:
        return "Venta no encontrada o no pertenece a este agente de cobranza", 404

    if venta["estado_banco"] == "PAGADO":
        return "No se puede eliminar una venta ya pagada.", 400

    # Por seguridad, también podríamos verificar que no haya pago ligado, pero en tu lógica
    # una venta ligada a pago siempre termina en estado PAGADO.
    db.execute("DELETE FROM ventas WHERE id = ? AND vendedor_id = ?", (venta_id, vendedor_id))
    db.commit()

    return redirect(url_for("ventas_listado"))


@app.route("/ventas/<int:venta_id>", methods=["GET", "POST"])
@role_required("vendedor")
def venta_detalle(venta_id):
    db = get_db()
    vendedor_id = session.get("user_id")

    # Traer la venta y asegurar que pertenece a este vendedor
    cur = db.execute(
        """
        SELECT v.*, c.alias AS cuenta_alias, c.banco
        FROM ventas v
        JOIN cuentas_bancarias c ON v.cuenta_bancaria_id = c.id
        WHERE v.id = ? AND v.vendedor_id = ?
        """,
        (venta_id, vendedor_id),
    )
    venta = cur.fetchone()
    if not venta:
        return "Venta no encontrada o no pertenece a este vendedor", 404

    mensaje_ok = None
    errores = []

    if request.method == "POST":
        accion = request.form.get("accion")

        if accion == "cliente_pago":
            # Marcamos la venta como EN_ESPERA_CONCILIACION
            db.execute(
                """
                UPDATE ventas
                SET estado_banco = 'EN_ESPERA_CONCILIACION',
                    fecha_ultimo_cambio = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (venta_id,),
            )
            db.commit()

            # Intentar conciliación automática (por si el pago ya estaba detectado)
            try:
                run_conciliacion()
            except Exception as e:
                errores.append(f"Error al ejecutar conciliación automática: {e}")

        elif accion == "subir_comprobante":
            archivo = request.files.get("comprobante")
            if not archivo or archivo.filename == "":
                errores.append("Debes seleccionar un archivo de comprobante.")
            elif not allowed_comprobante(archivo.filename):
                errores.append("Tipo de archivo no permitido. Usa PNG, JPG, JPEG o PDF.")
            else:
                original_name = secure_filename(archivo.filename)
                ext = original_name.rsplit(".", 1)[1].lower()
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"venta_{venta_id}_{timestamp}.{ext}"
                filepath = os.path.join(COMPROBANTES_FOLDER, filename)

                # Borrar comprobante anterior si existía
                if venta["comprobante_filename"]:
                    anterior = os.path.join(COMPROBANTES_FOLDER, venta["comprobante_filename"])
                    if os.path.exists(anterior):
                        try:
                            os.remove(anterior)
                        except Exception:
                            pass

                # Guardar nuevo archivo
                archivo.save(filepath)

                # Guardar en BD
                db.execute(
                    """
                    UPDATE ventas
                    SET comprobante_filename = ?, fecha_ultimo_cambio = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (filename, venta_id),
                )
                db.commit()
                mensaje_ok = "Comprobante cargado correctamente."

        # Volver a cargar la venta actualizada
        cur = db.execute(
            """
            SELECT v.*, c.alias AS cuenta_alias, c.banco
            FROM ventas v
            JOIN cuentas_bancarias c ON v.cuenta_bancaria_id = c.id
            WHERE v.id = ? AND v.vendedor_id = ?
            """,
            (venta_id, vendedor_id),
        )
        venta = cur.fetchone()

        if accion == "cliente_pago":
            if venta["estado_banco"] == "PAGADO":
                mensaje_ok = "✅ El sistema encontró el pago en el banco y la venta ya está marcada como PAGADA."
            else:
                mensaje_ok = "Se marcó la venta como 'EN ESPERA DE CONCILIACIÓN'. Aún no se detecta el pago en banco."

    # --------- Parsear detalle de venta rápida desde nota ---------
    detalle_venta_rapida = []
    nota = venta["nota"] or ""
    if "Venta rápida" in nota:
        lineas = nota.splitlines()
        for linea in lineas:
            linea = linea.strip()
            if not linea.startswith("Doc "):
                continue
            try:
                sin_prefijo = linea[4:]
                parte_doc, resto = sin_prefijo.split(":", 1)
                documento = parte_doc.strip()

                resto = resto.strip()
                if "(original" in resto:
                    monto_edit_str, parte_original = resto.split("(original", 1)
                    monto_edit = float(monto_edit_str.strip())
                    monto_orig_str = parte_original.replace(")", "").strip()
                    monto_orig = float(monto_orig_str)
                else:
                    monto_edit = float(resto.strip())
                    monto_orig = monto_edit

                detalle_venta_rapida.append(
                    {
                        "documento": documento,
                        "neto_editado": monto_edit,
                        "neto_original": monto_orig,
                    }
                )
            except Exception:
                continue

    # Buscar última actualización de pagos para la cuenta de esta venta
    cur = db.execute(
        """
        SELECT MAX(creado_en) AS ultima_actualizacion
        FROM pagos_detectados
        WHERE cuenta_bancaria_id = ?
        """,
        (venta["cuenta_bancaria_id"],),
    )
    row_update = cur.fetchone()
    ultima_actualizacion = row_update["ultima_actualizacion"] if row_update and row_update["ultima_actualizacion"] else None

    return render_template(
        "venta_detalle.html",
        venta=venta,
        mensaje_ok=mensaje_ok,
        errores=errores,
        ultima_actualizacion=ultima_actualizacion,
        detalle_venta_rapida=detalle_venta_rapida,
    )


# ---------- Rutas de pagos (admin / Noemí) ----------

@app.route("/pagos/subir", methods=["GET", "POST"])
@role_required("admin")
def pagos_subir():
    db = get_db()
    mensaje_ok = None
    errores = []

    if request.method == "POST":
        banco_sel = request.form.get("banco")
        archivo = request.files.get("archivo")

        if not banco_sel:
            errores.append("Debes seleccionar un banco.")
        if not archivo or archivo.filename == "":
            errores.append("Debes seleccionar un archivo de movimientos.")

        if not errores:
            filename = archivo.filename.lower()

            try:
                insertados = 0

                # =========================
                # BANCO: BBVA (Excel)
                # =========================
                if banco_sel == "BBVA":
                    if not (filename.endswith(".xlsb") or filename.endswith(".xlsx") or filename.endswith(".xls")):
                        errores.append("Para BBVA usa un archivo Excel (.xls, .xlsx o .xlsb).")
                    else:
                        import pandas as pd
                        import hashlib
                        import numpy as np

                        # Leer Excel de BBVA
                        if filename.endswith(".xlsb"):
                            df = pd.read_excel(archivo, engine="pyxlsb")
                        else:
                            df = pd.read_excel(archivo)

                        # Cuenta de AZYCO (columna 1 en la fila de encabezado original)
                        cuenta_azyco = str(df.columns[1]).strip()

                        # Encabezado real está en la fila 0
                        header_row = df.iloc[0]
                        data = df.iloc[1:].copy()
                        data.columns = header_row

                        if "Abono" not in data.columns:
                            errores.append("No se encontró la columna 'Abono' en el archivo de BBVA.")
                        else:
                            data["Abono"] = pd.to_numeric(data["Abono"], errors="coerce")
                            data["Saldo"] = pd.to_numeric(data.get("Saldo", pd.Series(dtype=float)), errors="coerce")

                            data = data[data["Abono"].notna() & (data["Abono"] > 0)]

                            # Convertir fecha Excel (número) a fecha real
                            if "Fecha Operación" in data.columns:
                                numeros_fecha = pd.to_numeric(data["Fecha Operación"], errors="coerce")
                                base_date = pd.Timestamp("1899-12-30")
                                fechas = numeros_fecha.apply(
                                    lambda x: base_date + pd.Timedelta(days=float(x))
                                    if not pd.isna(x) else pd.NaT
                                )
                            else:
                                fechas = pd.Series([pd.NaT] * len(data), index=data.index)

                            # Buscar cuenta BBVA (por numero_cuenta)
                            cur = db.execute(
                                """
                                SELECT id FROM cuentas_bancarias
                                WHERE banco = 'BBVA' AND numero_cuenta = ?
                                """,
                                (cuenta_azyco,),
                            )
                            row_cuenta = cur.fetchone()
                            cuenta_bancaria_id = row_cuenta["id"] if row_cuenta else None

                            for idx, row in data.iterrows():
                                fecha = fechas.loc[idx] if idx in fechas.index else pd.NaT
                                if pd.isna(fecha):
                                    continue
                                fecha_str = fecha.date().isoformat()

                                monto = float(row["Abono"]) if not pd.isna(row["Abono"]) else None
                                if monto is None:
                                    continue

                                referencia = str(row.get("Referencia", "")).strip()
                                ref_amp = str(row.get("Referencia Ampliada", "")).strip()
                                concepto = str(row.get("Concepto", "")).strip()
                                saldo_post = row.get("Saldo", None)
                                if not pd.isna(saldo_post):
                                    saldo_post = float(saldo_post)
                                else:
                                    saldo_post = None

                                raw = f"BBVA|{cuenta_azyco}|{fecha_str}|{monto}|{referencia}|{ref_amp}|{saldo_post}"
                                hash_unico = hashlib.sha256(raw.encode("utf-8")).hexdigest()

                                db.execute(
                                    """
                                    INSERT OR IGNORE INTO pagos_detectados (
                                        banco, cuenta_bancaria_id, fecha_operacion, hora_operacion,
                                        monto, referencia, referencia_ampliada, concepto,
                                        saldo_posterior, fuente_archivo, hash_unico
                                    )
                                    VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        "BBVA",
                                        cuenta_bancaria_id,
                                        fecha_str,
                                        monto,
                                        referencia,
                                        ref_amp,
                                        concepto,
                                        saldo_post,
                                        archivo.filename,
                                        hash_unico,
                                    ),
                                )
                                insertados += 1

                # =========================
                # BANCO: BANAMEX (CSV)
                # =========================
                elif banco_sel == "BANAMEX":
                    if not filename.endswith(".csv"):
                        errores.append("Para Banamex usa un archivo CSV.")
                    else:
                        import pandas as pd
                        import hashlib

                        df = pd.read_csv(archivo, encoding="latin1")

                        col0 = df.columns[0]
                        # fila donde empieza el detalle (la que dice 'Fecha')
                        hdr_idx = df[df[col0] == "Fecha"].index[0]
                        header = df.iloc[hdr_idx]
                        data = df.iloc[hdr_idx + 1 :].copy()
                        data.columns = header

                        # Solo depósitos
                        data = data[data["Fecha"].notna()]
                        data = data[
                            data["Depósitos"].notna()
                            & (data["Depósitos"].astype(str).str.strip() != "-")
                        ]

                        def clean_amount(x):
                            s = str(x)
                            s = (
                                s.replace("$", "")
                                .replace(",", "")
                                .strip()
                            )
                            if not s:
                                return None
                            return float(s)

                        data["monto"] = data["Depósitos"].apply(clean_amount)
                        data["fecha"] = pd.to_datetime(
                            data["Fecha"], format="%d/%m/%Y"
                        ).dt.date

                        # Buscar cuenta Banamex (primer cuenta activa)
                        cur = db.execute(
                            """
                            SELECT id FROM cuentas_bancarias
                            WHERE banco = 'BANAMEX' AND activa = 1
                            ORDER BY id
                            LIMIT 1
                            """
                        )
                        row_cuenta = cur.fetchone()
                        cuenta_bancaria_id = row_cuenta["id"] if row_cuenta else None

                        for _, row in data.iterrows():
                            if row["monto"] is None:
                                continue

                            fecha_str = row["fecha"].isoformat()
                            monto = row["monto"]
                            concepto = str(row.get("Descripción", "")).strip()
                            referencia = ""  # el CSV no trae referencia corta clara
                            saldo_post = None  # opcional

                            raw = f"BANAMEX|{cuenta_bancaria_id}|{fecha_str}|{monto}|{referencia}|{concepto}|{saldo_post}"
                            hash_unico = hashlib.sha256(raw.encode("utf-8")).hexdigest()

                            db.execute(
                                """
                                INSERT OR IGNORE INTO pagos_detectados (
                                    banco, cuenta_bancaria_id, fecha_operacion, hora_operacion,
                                    monto, referencia, referencia_ampliada, concepto,
                                    saldo_posterior, fuente_archivo, hash_unico
                                )
                                VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    "BANAMEX",
                                    cuenta_bancaria_id,
                                    fecha_str,
                                    monto,
                                    referencia,
                                    "",  # referencia ampliada
                                    concepto,
                                    saldo_post,
                                    archivo.filename,
                                    hash_unico,
                                ),
                            )
                            insertados += 1

                # =========================
                # BANCO: BANORTE (CSV)
                # =========================
                elif banco_sel == "BANORTE":
                    if not filename.endswith(".csv"):
                        errores.append("Para Banorte usa un archivo CSV.")
                    else:
                        import pandas as pd
                        import hashlib

                        df = pd.read_csv(archivo, encoding="latin1")

                        # columna de depósitos (nombre con acentos raros)
                        dep_col = [c for c in df.columns if "DEP" in c.upper()][0]

                        df_dep = df[
                            df[dep_col].notna()
                            & (df[dep_col].astype(str).str.strip() != "-")
                        ].copy()

                        def clean_amount(x):
                            s = str(x)
                            s = (
                                s.replace("$", "")
                                .replace(",", "")
                                .strip()
                            )
                            if not s or s == "-":
                                return None
                            return float(s)

                        df_dep["monto"] = df_dep[dep_col].apply(clean_amount)
                        df_dep["fecha"] = pd.to_datetime(
                            df_dep["FECHA"], format="%d/%m/%Y"
                        ).dt.date

                        # Buscar cuenta Banorte (primer cuenta activa)
                        cur = db.execute(
                            """
                            SELECT id FROM cuentas_bancarias
                            WHERE banco = 'BANORTE' AND activa = 1
                            ORDER BY id
                            LIMIT 1
                            """
                        )
                        row_cuenta = cur.fetchone()
                        cuenta_bancaria_id = row_cuenta["id"] if row_cuenta else None

                        for _, row in df_dep.iterrows():
                            if row["monto"] is None:
                                continue

                            fecha_str = row["fecha"].isoformat()
                            monto = row["monto"]
                            referencia = str(row.get("REFERENCIA", "")).strip()
                            concepto = str(row.get("DESCRIPCIÓN", "")).strip()
                            saldo_post = None  # podríamos parsear 'SALDO' si hace falta

                            raw = f"BANORTE|{cuenta_bancaria_id}|{fecha_str}|{monto}|{referencia}|{concepto}|{saldo_post}"
                            hash_unico = hashlib.sha256(raw.encode("utf-8")).hexdigest()

                            db.execute(
                                """
                                INSERT OR IGNORE INTO pagos_detectados (
                                    banco, cuenta_bancaria_id, fecha_operacion, hora_operacion,
                                    monto, referencia, referencia_ampliada, concepto,
                                    saldo_posterior, fuente_archivo, hash_unico
                                )
                                VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    "BANORTE",
                                    cuenta_bancaria_id,
                                    fecha_str,
                                    monto,
                                    referencia,
                                    "",
                                    concepto,
                                    saldo_post,
                                    archivo.filename,
                                    hash_unico,
                                ),
                            )
                            insertados += 1

                else:
                    errores.append("Banco no reconocido.")

                db.commit()

                try:
                    run_conciliacion()
                except Exception as e:
                    # Para no tronar la carga si algo pasa en conciliación
                    print(f"Error al ejecutar conciliación automática después de subir movimientos: {e}")

                return redirect(url_for("pagos_detectados_listado"))

                return render_template("pagos_subir.html")

                if insertados > 0:
                    mensaje_ok = f"Archivo procesado. Pagos detectados insertados (o ignorados si ya existían): {insertados}"
                elif not errores:
                    mensaje_ok = "El archivo se procesó pero no se encontraron depósitos nuevos."

                # Ejecutar conciliación automática después de cualquier carga
                if not errores:
                    try:
                        from modules.conciliacion import run_conciliacion
                        nuevos_matches = run_conciliacion()
                        if nuevos_matches > 0:
                            mensaje_ok += f" | Conciliación automática: {nuevos_matches} ventas marcadas como PAGADO."
                    except Exception as e:
                        errores.append(f"Error al ejecutar la conciliación automática: {e}")

            except Exception as e:
                errores.append(f"Error al procesar el archivo: {e}")

    return render_template(
        "pagos_subir.html",
        mensaje_ok=mensaje_ok,
        errores=errores
    )



@app.route("/venta-rapida", methods=["GET", "POST"])
@role_required("admin","vendedor")
def venta_rapida():
    db = get_db()
    mensaje_ok = None
    errores = []

    # 1) Obtener vendedores y cuentas activas para el formulario
    cur = db.execute(
        """
        SELECT id, nombre
        FROM usuarios
        WHERE rol = 'vendedor'
        ORDER BY nombre
        """
    )
    vendedores = cur.fetchall()

    cur = db.execute(
        """
        SELECT id, banco, alias
        FROM cuentas_bancarias
        WHERE activa = 1
        ORDER BY banco, alias
        """
    )
    cuentas = cur.fetchall()

    # Distinguimos paso 1 (subir archivo) y paso 2 (confirmar selección)
    step = request.form.get("step", "1")

    if request.method == "POST" and step == "1":
        # Paso 1: subir archivo y mostrar facturas
        archivo = request.files.get("archivo")
        vendedor_id = request.form.get("vendedor_id")
        cuenta_bancaria_id = request.form.get("cuenta_bancaria_id")

        if not archivo or archivo.filename == "":
            errores.append("Debes seleccionar un archivo de antigüedad de saldos (Excel).")
        if not vendedor_id:
            errores.append("Debes seleccionar un vendedor.")
        if not cuenta_bancaria_id:
            errores.append("Debes seleccionar una cuenta bancaria.")

        if not errores:
            filename = archivo.filename.lower()
            if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
                errores.append("El archivo debe ser Excel (.xlsx o .xls).")
            else:
                try:
                    import pandas as pd
                    df = pd.read_excel(archivo)

                    col_cliente = "Receivables Aging Schedule Details"
                    col_doc = "Unnamed: 2"
                    col_neto = "Unnamed: 11"

                    if col_cliente not in df.columns or col_doc not in df.columns or col_neto not in df.columns:
                        errores.append("El formato del Excel no coincide con el esperado (antigüedad de saldos).")
                    else:
                        facturas = []
                        cliente_actual = None

                        for _, row in df.iterrows():
                            cliente_val = row.get(col_cliente)

                            # Detectar renglón de cliente (ej. '21-100074 JAIME ...')
                            if isinstance(cliente_val, str) and "-" in cliente_val and "Agente" not in cliente_val and "Organización" not in cliente_val and "Balance" not in cliente_val:
                                cliente_actual = cliente_val.strip()
                                continue

                            doc = row.get(col_doc)
                            neto = row.get(col_neto)

                            # Filtrar filas de factura: doc no nulo, neto numérico positivo
                            if pd.notna(doc) and pd.notna(neto):
                                try:
                                    neto_val = float(neto)
                                except Exception:
                                    continue
                                if neto_val <= 0:
                                    continue

                                facturas.append({
                                    "cliente": cliente_actual or "",
                                    "documento": str(doc).strip(),
                                    "neto": neto_val,
                                })

                        if not facturas:
                            errores.append("No se encontraron facturas en el archivo.")
                        else:
                            # Renderizar vista de selección (paso 2)
                            return render_template(
                                "venta_rapida_preview.html",
                                vendedores=vendedores,
                                cuentas=cuentas,
                                vendedor_id=vendedor_id,
                                cuenta_bancaria_id=cuenta_bancaria_id,
                                facturas=facturas,
                            )
                except Exception as e:
                    errores.append(f"Error al leer el archivo: {e}")

    elif request.method == "POST" and step == "2":
        # Paso 2: crear ventas en lote a partir de las facturas seleccionadas
        vendedor_id = request.form.get("vendedor_id")
        cuenta_bancaria_id = request.form.get("cuenta_bancaria_id")

        if not vendedor_id or not cuenta_bancaria_id:
            errores.append("Falta vendedor o cuenta bancaria al procesar la venta rápida.")
        else:
            # Reconstruir facturas desde los campos del formulario
            import json

            facturas_raw = request.form.get("facturas_json")
            if not facturas_raw:
                errores.append("No se recibió el detalle de facturas.")
            else:
                facturas = json.loads(facturas_raw)
                seleccion_indices = request.form.getlist("seleccion")

                if not seleccion_indices:
                    errores.append("Debes seleccionar al menos una factura.")
                else:
                    # Agrupar por cliente
                    from collections import defaultdict

                    por_cliente = defaultdict(list)

                    for idx_str in seleccion_indices:
                        idx = int(idx_str)
                        f = facturas[idx]
                        # tomar el neto editado
                        monto_edit_str = request.form.get(f"monto_{idx}")
                        try:
                            monto_edit = float(monto_edit_str)
                        except Exception:
                            monto_edit = f["neto"]

                        detalle = {
                            "documento": f["documento"],
                            "neto_original": f["neto"],
                            "neto_editado": monto_edit,
                        }
                        por_cliente[f["cliente"]].append(detalle)

                    creadas = 0

                    from datetime import datetime
                    ahora = datetime.now().isoformat(sep=" ", timespec="seconds")

                    for cliente, det_list in por_cliente.items():
                        total_cliente = sum(d["neto_editado"] for d in det_list)
                        if total_cliente <= 0:
                            continue

                        # Generar un folio de venta rápida (ej. VR-21100074-20251126-01)
                        codigo = cliente.split()[0] if cliente else "VR"
                        codigo = codigo.replace("-", "")
                        folio = f"VR-{codigo}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

                        # Nota con el detalle de facturas
                        lineas = []
                        for d in det_list:
                            lineas.append(f"Doc {d['documento']}: {d['neto_editado']:.2f} (original {d['neto_original']:.2f})")
                        nota = "Venta rápida.\nCliente: " + (cliente or "SIN NOMBRE") + "\n" + "\n".join(lineas)

                        db.execute(
                            """
                            INSERT INTO ventas (
                                folio, cliente_nombre, monto, cuenta_bancaria_id,
                                vendedor_id, estado_banco, fecha_creacion, fecha_ultimo_cambio, nota
                            )
                            VALUES (?, ?, ?, ?, ?, 'PENDIENTE', ?, ?, ?)
                            """,
                            (
                                folio,
                                cliente,
                                total_cliente,
                                cuenta_bancaria_id,
                                vendedor_id,
                                ahora,
                                ahora,
                                nota,
                            ),
                        )
                        creadas += 1

                    db.commit()

                    # Ejecutar conciliación automática por si ya existen pagos
                    try:
                        from modules.conciliacion import run_conciliacion
                        run_conciliacion()
                    except Exception:
                        pass

                    mensaje_ok = f"Ventas rápidas creadas: {creadas}"

    return render_template(
        "venta_rapida_upload.html",
        vendedores=vendedores,
        cuentas=cuentas,
        errores=errores,
        mensaje_ok=mensaje_ok,
    )


@app.route("/pagos/detectados")
@role_required("admin")
def pagos_detectados_listado():
    db = get_db()

    # Filtros desde la URL (?banco=BBVA&estado=PENDIENTE&...)
    banco = request.args.get("banco", "").strip()
    estado = request.args.get("estado", "").strip()
    fecha_desde = request.args.get("fecha_desde", "").strip()
    fecha_hasta = request.args.get("fecha_hasta", "").strip()
    monto_str = request.args.get("monto", "").strip()

    condiciones = []
    params = []

    # Armamos WHERE dinámico
    if banco:
        condiciones.append("p.banco = ?")
        params.append(banco)

    if estado:
        condiciones.append("p.estado_conciliacion = ?")
        params.append(estado)

    if fecha_desde:
        condiciones.append("date(p.fecha_operacion) >= date(?)")
        params.append(fecha_desde)

    if fecha_hasta:
        condiciones.append("date(p.fecha_operacion) <= date(?)")
        params.append(fecha_hasta)

    if monto_str:
        try:
            monto_val = float(monto_str)
            # Igualdad con tolerancia de centavos
            condiciones.append("ABS(p.monto - ?) < 0.01")
            params.append(monto_val)
        except ValueError:
            # Si no es numérico, ignoramos el filtro
            monto_val = None

    where_clause = ""
    if condiciones:
        where_clause = "WHERE " + " AND ".join(condiciones)

    query = f"""
        SELECT 
            p.*,
            c.alias AS cuenta_alias,
            v.folio AS venta_folio
        FROM pagos_detectados p
        LEFT JOIN cuentas_bancarias c ON p.cuenta_bancaria_id = c.id
        LEFT JOIN ventas v ON p.venta_id = v.id
        {where_clause}
        ORDER BY p.fecha_operacion DESC, p.id DESC
    """

    cur = db.execute(query, params)
    pagos = cur.fetchall()

    return render_template(
        "pagos_detectados.html",
        pagos=pagos,
        filtro_banco=banco,
        filtro_estado=estado,
        filtro_fecha_desde=fecha_desde,
        filtro_fecha_hasta=fecha_hasta,
        filtro_monto=monto_str,
    )



@app.route("/pagos/detectados/<int:pago_id>", methods=["GET", "POST"])
@role_required("admin")
def pago_detalle(pago_id):
    db = get_db()

    # Traer el pago
    cur = db.execute(
        """
        SELECT 
            p.*,
            c.alias AS cuenta_alias,
            v.folio AS venta_folio,
            v.cliente_nombre AS venta_cliente,
            v.monto AS venta_monto,
            v.nota AS venta_nota
        FROM pagos_detectados p
        LEFT JOIN cuentas_bancarias c ON p.cuenta_bancaria_id = c.id
        LEFT JOIN ventas v ON p.venta_id = v.id
        WHERE p.id = ?
        """,
        (pago_id,),
    )
    pago = cur.fetchone()
    if not pago:
        return "Pago no encontrado", 404

    mensaje_ok = None
    errores = []

    # Función auxiliar para obtener ventas candidatas
    def obtener_candidatos(pago_row):
        if pago_row["cuenta_bancaria_id"] is None or pago_row["monto"] is None:
            return []
        cur_local = db.execute(
            """
            SELECT *
            FROM ventas
            WHERE cuenta_bancaria_id = ?
            AND estado_banco IN ('PENDIENTE', 'EN_ESPERA_CONCILIACION')
            AND ABS(monto - ?) < 0.01
            ORDER BY fecha_creacion DESC
            LIMIT 30
            """,
            (pago_row["cuenta_bancaria_id"], pago_row["monto"]),
        )
        return cur_local.fetchall()

    if request.method == "POST":
        # 1) Caso nuevo: asociar directo desde la lista (por venta_id)
        venta_id_directo = request.form.get("venta_id_directo")

        if venta_id_directo:
            try:
                venta_id_directo = int(venta_id_directo)
            except ValueError:
                errores.append("ID de venta no válido.")
            else:
                cur = db.execute(
                    """
                    SELECT *
                    FROM ventas
                    WHERE id = ?
                    AND estado_banco IN ('PENDIENTE', 'EN_ESPERA_CONCILIACION')
                    """,
                    (venta_id_directo,),
                )
                venta = cur.fetchone()
                if not venta:
                    errores.append("La venta seleccionada no existe o ya no está pendiente.")
                else:
                    db.execute(
                        """
                        UPDATE pagos_detectados
                        SET estado_conciliacion = 'MATCH',
                            venta_id = ?
                        WHERE id = ?
                        """,
                        (venta["id"], pago_id),
                    )
                    db.execute(
                        """
                        UPDATE ventas
                        SET estado_banco = 'PAGADO',
                            fecha_ultimo_cambio = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (venta["id"],),
                    )
                    db.commit()
                    mensaje_ok = f"Pago asociado correctamente a la venta con folio {venta['folio']}."
        else:
            # 2) Caso clásico: búsqueda por folio manual
            folio_buscar = request.form.get("folio")

            if not folio_buscar:
                errores.append("Debes escribir un folio de venta.")
            else:
                cur = db.execute(
                    """
                    SELECT *
                    FROM ventas
                    WHERE folio = ?
                    """,
                    (folio_buscar,),
                )
                venta = cur.fetchone()
                if not venta:
                    errores.append(f"No se encontró ninguna venta con folio {folio_buscar}.")
                else:
                    db.execute(
                        """
                        UPDATE pagos_detectados
                        SET estado_conciliacion = 'MATCH',
                            venta_id = ?
                        WHERE id = ?
                        """,
                        (venta["id"], pago_id),
                    )
                    db.execute(
                        """
                        UPDATE ventas
                        SET estado_banco = 'PAGADO',
                            fecha_ultimo_cambio = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (venta["id"],),
                    )
                    db.commit()
                    mensaje_ok = f"Pago asociado correctamente a la venta con folio {folio_buscar}."

        # Volver a leer el pago actualizado (ya con posible venta ligada)
        cur = db.execute(
            """
            SELECT 
                p.*,
                c.alias AS cuenta_alias,
                v.folio AS venta_folio,
                v.cliente_nombre AS venta_cliente,
                v.monto AS venta_monto,
                v.nota AS venta_nota,
                v.comprobante_filename AS venta_comprobante
            FROM pagos_detectados p
            LEFT JOIN cuentas_bancarias c ON p.cuenta_bancaria_id = c.id
            LEFT JOIN ventas v ON p.venta_id = v.id
            WHERE p.id = ?
            """,
            (pago_id,),
        )
        pago = cur.fetchone()

    # --------- Parsear detalle de venta rápida de la venta asociada ---------
    detalle_venta_rapida = []
    if pago["venta_id"] and pago["venta_nota"] and "Venta rápida" in pago["venta_nota"]:
        nota = pago["venta_nota"]
        lineas = nota.splitlines()
        for linea in lineas:
            linea = linea.strip()
            if not linea.startswith("Doc "):
                continue
            # Formato: "Doc XYZ: 123.45 (original 130.00)"
            try:
                sin_prefijo = linea[4:]  # quitar "Doc "
                parte_doc, resto = sin_prefijo.split(":", 1)
                documento = parte_doc.strip()

                resto = resto.strip()
                if "(original" in resto:
                    monto_edit_str, parte_original = resto.split("(original", 1)
                    monto_edit = float(monto_edit_str.strip())
                    monto_orig_str = parte_original.replace(")", "").strip()
                    monto_orig = float(monto_orig_str)
                else:
                    monto_edit = float(resto.strip())
                    monto_orig = monto_edit

                detalle_venta_rapida.append(
                    {
                        "documento": documento,
                        "neto_editado": monto_edit,
                        "neto_original": monto_orig,
                    }
                )
            except Exception:
                continue

    # Candidatos de venta (sólo si el pago NO está ya asociado)
    if pago["venta_id"] is None:
        candidatos_venta = obtener_candidatos(pago)
    else:
        candidatos_venta = []

    return render_template(
        "pago_detalle.html",
        pago=pago,
        mensaje_ok=mensaje_ok,
        errores=errores,
        candidatos_venta=candidatos_venta,
        detalle_venta_rapida=detalle_venta_rapida,
    )

@app.route("/cierre-diario", methods=["GET"])
@role_required("admin")
def cierre_diario():
    db = get_db()

    fecha_str = request.args.get("fecha")
    if not fecha_str:
        fecha_str = date.today().isoformat()

    export = request.args.get("export", "").strip()

    # --------- EXPORTAR CORTE APLICADO (CSV) ---------
    if export == "corte":
        import csv
        from io import StringIO

        # Pagos en MATCH de ese día, con su venta y cuenta
        cur = db.execute(
            """
            SELECT
                v.folio AS venta_folio,
                v.cliente_nombre,
                v.monto AS monto_venta,
                v.nota AS venta_nota,
                v.comprobante_filename AS venta_comprobante
                c.alias AS cuenta_alias
            FROM pagos_detectados p
            JOIN ventas v ON p.venta_id = v.id
            LEFT JOIN cuentas_bancarias c ON v.cuenta_bancaria_id = c.id
            WHERE p.estado_conciliacion = 'MATCH'
              AND date(p.fecha_operacion) = date(?)
            ORDER BY v.cliente_nombre, v.folio
            """,
            (fecha_str,),
        )
        rows = cur.fetchall()

        def parse_detalle_venta_rapida(nota):
            """
            Devuelve lista de dicts con: documento, neto_editado, neto_original
            a partir de la nota de una venta rápida.
            """
            if not nota or "Venta rápida" not in nota:
                return []
            detalle = []
            for linea in nota.splitlines():
                linea = linea.strip()
                if not linea.startswith("Doc "):
                    continue
                try:
                    sin_prefijo = linea[4:]  # quitar "Doc "
                    parte_doc, resto = sin_prefijo.split(":", 1)
                    documento = parte_doc.strip()

                    resto = resto.strip()
                    if "(original" in resto:
                        monto_edit_str, parte_original = resto.split("(original", 1)
                        monto_edit = float(monto_edit_str.strip())
                        monto_orig_str = parte_original.replace(")", "").strip()
                        monto_orig = float(monto_orig_str)
                    else:
                        monto_edit = float(resto.strip())
                        monto_orig = monto_edit

                    detalle.append(
                        {
                            "documento": documento,
                            "neto_editado": monto_edit,
                            "neto_original": monto_orig,
                        }
                    )
                except Exception:
                    continue
            return detalle

        output = StringIO()
        writer = csv.writer(output)

        # Encabezados
        writer.writerow(["Cliente", "Folio", "Documento", "Cuenta", "Monto"])

        for r in rows:
            cliente = r["cliente_nombre"] or ""
            folio = r["venta_folio"] or ""
            cuenta = r["cuenta_alias"] or ""
            nota = r["venta_nota"] or ""
            monto_venta = r["monto_venta"] or 0.0

            detalle = parse_detalle_venta_rapida(nota)

            if detalle:
                # Venta rápida: una fila por documento
                for d in detalle:
                    writer.writerow([
                        cliente,
                        folio,
                        d["documento"],
                        cuenta,
                        f"{d['neto_editado']:.2f}",
                    ])
            else:
                # Venta normal: una fila única, documento vacío
                writer.writerow([
                    cliente,
                    folio,
                    "",
                    cuenta,
                    f"{monto_venta:.2f}",
                ])

        csv_data = output.getvalue()
        output.close()

        filename = f"corte_aplicado_{fecha_str}.csv"
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    # --------- MODO NORMAL: mostrar pantalla de cierre diario ---------

    # Ventas del día
    cur = db.execute(
        """
        SELECT *
        FROM ventas
        WHERE date(fecha_creacion) = date(?)
        ORDER BY fecha_creacion ASC
        """,
        (fecha_str,),
    )
    ventas_dia = cur.fetchall()

    total_ventas = len(ventas_dia)
    total_monto_ventas = sum(v["monto"] for v in ventas_dia) if ventas_dia else 0.0

    ventas_pagadas = [v for v in ventas_dia if v["estado_banco"] == "PAGADO"]
    ventas_pendientes = [v for v in ventas_dia if v["estado_banco"] != "PAGADO"]

    total_pagadas = len(ventas_pagadas)
    total_pendientes = len(ventas_pendientes)

    # Pagos del día
    cur = db.execute(
        """
        SELECT p.*, v.folio AS venta_folio
        FROM pagos_detectados p
        LEFT JOIN ventas v ON p.venta_id = v.id
        WHERE date(p.fecha_operacion) = date(?)
        ORDER BY p.fecha_operacion ASC, p.id ASC
        """,
        (fecha_str,),
    )
    pagos_dia = cur.fetchall()

    total_pagos = len(pagos_dia)
    total_monto_pagos = sum(p["monto"] for p in pagos_dia) if pagos_dia else 0.0

    pagos_con_venta = [p for p in pagos_dia if p["venta_id"] is not None]
    pagos_sin_venta = [p for p in pagos_dia if p["venta_id"] is None]

    total_pagos_con_venta = len(pagos_con_venta)
    total_pagos_sin_venta = len(pagos_sin_venta)

    return render_template(
        "cierre_diario.html",
        fecha_str=fecha_str,
        total_ventas=total_ventas,
        total_monto_ventas=total_monto_ventas,
        ventas_pagadas=ventas_pagadas,
        ventas_pendientes=ventas_pendientes,
        total_pagadas=total_pagadas,
        total_pendientes=total_pendientes,
        total_pagos=total_pagos,
        total_monto_pagos=total_monto_pagos,
        pagos_con_venta=pagos_con_venta,
        pagos_sin_venta=pagos_sin_venta,
        total_pagos_con_venta=total_pagos_con_venta,
        total_pagos_sin_venta=total_pagos_sin_venta,
    )



if __name__ == "__main__":
    app.run(debug=True)

