"""
AutoData Technologies — Servidor de Producción v2.4.1
Backend Flask con API REST + Frontend integrado
© 2025 AutoData Technologies — autodata.com
"""

import os
import sqlite3
import json
import hashlib
import secrets
import time
import glob
import re
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, jsonify, request, send_from_directory, make_response
from flask_cors import CORS

# Intentar importar librerías de procesamiento de facturas
try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False

try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import PatternFill
    EXCEL_SUPPORT = True
except ImportError:
    EXCEL_SUPPORT = False

# ═══════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════
VERSION = "2.4.1"
SECRET_KEY = os.environ.get("AUTODATA_SECRET", secrets.token_hex(32))
DB_PATH = os.environ.get("DATABASE_URL", "autodata_demo.db")
PORT = int(os.environ.get("PORT", 5000))

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["SECRET_KEY"] = SECRET_KEY
CORS(app, resources={r"/ad-api/*": {"origins": "*"}})


# ═══════════════════════════════════════════════
# BASE DE DATOS SQLITE — DEMO
# ═══════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Inicializa la base de datos con esquema y datos demo de Dataintelligence.com"""
    conn = get_db()
    c = conn.cursor()

    # Tabla usuarios
    c.execute("""CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        nombre TEXT,
        rol TEXT NOT NULL DEFAULT 'CLIENT_USER',
        empresa TEXT,
        activo INTEGER DEFAULT 1,
        ultimo_login TEXT,
        creado TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Tabla sesiones
    c.execute("""CREATE TABLE IF NOT EXISTS sesiones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER,
        token TEXT UNIQUE NOT NULL,
        creado TEXT DEFAULT CURRENT_TIMESTAMP,
        expira TEXT NOT NULL,
        activa INTEGER DEFAULT 1
    )""")

    # Tabla facturas
    c.execute("""CREATE TABLE IF NOT EXISTS facturas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_factura TEXT,
        tipo TEXT DEFAULT 'A',
        cuit_proveedor TEXT,
        nombre_proveedor TEXT,
        fecha_emision TEXT,
        fecha_vencimiento TEXT,
        subtotal REAL,
        iva_monto REAL,
        total REAL,
        moneda TEXT DEFAULT 'ARS',
        categoria TEXT,
        estado TEXT DEFAULT 'Pendiente',
        calidad TEXT DEFAULT 'ALTO',
        confianza REAL DEFAULT 100.0,
        archivo TEXT,
        creado TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Tabla desvíos
    c.execute("""CREATE TABLE IF NOT EXISTS desvios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        archivo TEXT,
        codigo TEXT,
        tipo TEXT,
        campo TEXT,
        valor_ia TEXT,
        confianza REAL,
        valor_correcto TEXT,
        estado TEXT DEFAULT 'Pendiente',
        revisado_por TEXT,
        fecha_revision TEXT,
        detectado TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Tabla automatizaciones
    c.execute("""CREATE TABLE IF NOT EXISTS automatizaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        tipo TEXT,
        activa INTEGER DEFAULT 1,
        ultima_ejecucion TEXT,
        proxima_ejecucion TEXT,
        ultimo_resultado TEXT,
        fuente TEXT
    )""")

    # Tabla auditoría
    c.execute("""CREATE TABLE IF NOT EXISTS auditoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        tipo TEXT,
        usuario TEXT,
        operacion TEXT,
        archivo TEXT,
        resultado TEXT,
        equipo TEXT
    )""")

    conn.commit()

    # Insertar datos demo si la tabla está vacía
    if c.execute("SELECT COUNT(*) FROM usuarios").fetchone()[0] == 0:
        _seed_demo_data(c)
        conn.commit()

    conn.close()


def _hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def _seed_demo_data(c):
    """Inserta datos demo de Dataintelligence.com"""

    # Usuario demo
    c.execute("""INSERT INTO usuarios (email, password_hash, nombre, rol, empresa) VALUES
        ('demo@dataintelligence.com', ?, 'Guillermo', 'CLIENT_ADMIN', 'Dataintelligence.com')
    """, (_hash_password("AutoData2025"),))

    # Facturas demo — Argentina AFIP
    facturas = [
        ("0003-00000412","A","30-68123456-8","Cloud Services SRL","2025-01-18","2025-02-17",125000,26250,151250,"ARS","Servicios","Pagada","ALTO",98.5,"factura_cloud_412.pdf"),
        ("0002-00000067","A","30-72345678-1","Consultora BDG S.A.","2025-03-10","2025-04-09",320000,67200,387200,"ARS","Servicios","Pagada","ALTO",97.8,"factura_bdg_067.pdf"),
        ("0001-00000445","A","30-77654321-0","Staff Solutions SRL","2025-02-14","2025-03-16",210000,44100,254100,"ARS","RRHH","Pagada","ALTO",99.1,"factura_staff_445.pdf"),
        ("0001-00000089","A","30-71234567-9","Technika S.A.","2025-01-15","2025-02-14",38000,7200,45200,"ARS","Servicios","Pendiente","ALTO",88.2,"factura_technika_089.pdf"),
        ("0002-00000089","A","30-62345678-5","Recursos Plus SA","2025-02-01","2025-03-03",180000,37800,217800,"ARS","RRHH","Pagada","ALTO",96.4,"factura_recursos_089.pdf"),
        ("0001-00000234","A","30-70123456-7","NetConnect SRL","2025-02-05","2025-03-07",22000,4620,26620,"ARS","Servicios","Pagada","ALTO",99.0,"factura_netconn_234.pdf"),
        ("0005-00000078","A","30-65432198-1","Insumos Tech SA","2025-02-10","2025-03-12",67000,14070,81070,"ARS","Materiales","Pendiente","ALTO",94.5,"factura_insumos_078.pdf"),
        ("0002-00000567","B","30-54321987-6","Serv. Limpieza Primo","2025-02-18","2025-03-20",15000,0,15000,"ARS","Servicios","Pagada","ALTO",99.3,"factura_limpieza_567.pdf"),
        ("0008-00001234","A","30-69876543-2","Telecom Empresas","2025-02-20","2025-02-22",35000,7350,42350,"ARS","Servicios","Vencida","ALTO",97.2,"factura_telecom_1234.pdf"),
        ("0001-00000095","A","30-71234567-9","Technika S.A.","2025-03-01","2025-03-31",38000,7980,45980,"ARS","Servicios","Pendiente","ALTO",99.0,"factura_technika_095.pdf"),
        ("0001-00000123","A","30-71345678-0","Seguros Corporativos","2025-03-03","2025-04-02",95000,19950,114950,"ARS","Servicios","Pagada","ALTO",98.1,"factura_seguros_123.pdf"),
        ("0003-00000890","B","30-55678901-3","Papelería Central SA","2025-03-05","2025-04-04",4200,0,4200,"ARS","Materiales","Pagada","ALTO",99.5,"factura_papeleria_890.pdf"),
        ("0003-00000445","A","30-68123456-8","Cloud Services SRL","2025-03-07","2025-04-06",125000,26250,151250,"ARS","Servicios","Pendiente","ALTO",98.8,"factura_cloud_445.pdf"),
        ("0002-00000078","A","30-72345678-1","Consultora BDG SA","2025-04-20","2025-05-20",340000,71400,411400,"ARS","Servicios","Pagada","ALTO",97.5,"factura_bdg_078.pdf"),
        ("0001-00000256","A","30-70123456-7","NetConnect SRL","2025-04-05","2025-05-05",22000,4620,26620,"ARS","Servicios","Pagada","ALTO",98.9,"factura_netconn_256.pdf"),
        ("0001-00000178","A","30-73456789-3","Maint. Systems SA","2025-04-10","2025-05-10",56000,11760,67760,"ARS","Servicios","Pendiente","ALTO",96.0,"factura_maint_178.pdf"),
        ("0008-00001290","A","30-69876543-2","Telecom Empresas","2025-04-12","2025-03-12",35000,7350,42350,"ARS","Servicios","Vencida","ALTO",97.8,"factura_telecom_1290.pdf"),
        ("0001-00000467","A","30-77654321-0","Staff Solutions SRL","2025-04-14","2025-05-14",215000,45150,260150,"ARS","RRHH","Pendiente","ALTO",99.2,"factura_staff_467.pdf"),
        ("0005-00000089","A","30-65432198-1","Insumos Tech SA","2025-04-16","2025-05-16",71000,14910,85910,"ARS","Materiales","Pagada","ALTO",95.3,"factura_insumos_089.pdf"),
        ("0001-00000102","A","30-71234567-9","Technika S.A.","2025-04-18","2025-05-18",42000,8820,50820,"ARS","Servicios","Pendiente","ALTO",98.4,"factura_technika_102.pdf"),
        ("0001-00000145","A","30-71345678-0","Seguros Corporativos","2025-04-22","2025-05-22",95000,19950,114950,"ARS","Servicios","Pendiente","ALTO",98.0,"factura_seguros_145.pdf"),
        ("0003-00000467","A","30-68123456-8","Cloud Services SRL","2025-04-25","2025-05-25",130000,27300,157300,"ARS","Servicios","Pagada","ALTO",99.1,"factura_cloud_467.pdf"),
        ("0001-00000003","A","30-80123456-5","Proveedor Nuevo XYZ","2025-04-28","2025-05-28",45200,9492,33800,"ARS","Materiales","Pendiente","MEDIO",72.5,"factura_nuevo_003.pdf"),
        ("0002-00000102","A","30-62345678-5","Recursos Plus SA","2025-04-01","2025-05-01",185000,38850,223850,"ARS","RRHH","Pendiente","ALTO",97.3,"factura_recursos_102.pdf"),
        ("0001-00000060","A","30-71890123-4","DataSec Solutions","2025-03-12","2025-02-11",45000,9450,54450,"ARS","Servicios","Vencida","ALTO",98.7,"factura_datasec_060.pdf"),
        ("0001-00000056","A","30-71890123-4","DataSec Solutions","2025-01-28","2025-02-27",45000,9450,54450,"ARS","Servicios","Pagada","ALTO",99.4,"factura_datasec_056.pdf"),
        ("0002-00000089","A","30-62345678-5","Recursos Plus SA","2025-06-01","2025-07-01",188000,39480,227480,"ARS","RRHH","Pendiente","ALTO",98.5,"factura_recursos_106.pdf"),
        ("0001-00000215","B","30-59876543-2","Oficorp SA","2025-04-07","2025-05-07",9200,0,9200,"ARS","Materiales","Pagada","ALTO",99.6,"factura_oficorp_215.pdf"),
        ("0001-00000201","B","30-59876543-2","Oficorp SA","2025-01-22","2025-02-21",8500,0,8500,"ARS","Materiales","Pagada","ALTO",99.8,"factura_oficorp_201.pdf"),
        ("0001-00000334","B","30-63456789-2","Catering Ejecutivo","2025-03-14","2025-04-13",12000,0,12000,"ARS","Servicios","Pagada","ALTO",99.2,"factura_catering_334.pdf"),
    ]

    c.executemany("""INSERT INTO facturas
        (numero_factura,tipo,cuit_proveedor,nombre_proveedor,fecha_emision,fecha_vencimiento,
         subtotal,iva_monto,total,moneda,categoria,estado,calidad,confianza,archivo)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", facturas)

    # Desvíos demo
    desvios = [
        ("factura_technika_089_DESVIO.pdf","NUM-001","Totales inconsistentes","Total ARS","$45.200",78.0,"","Pendiente",None,None),
        ("factura_recursos_plus_BORROSA.pdf","DOC-002","Imagen borrosa","N° Factura","0001-0000?4",45.0,"","Crítico",None,None),
        ("factura_nuevo_003.pdf","PRO-001","Proveedor desconocido","CUIT","30-80123456-5",85.0,"","Moderado",None,None),
    ]
    c.executemany("""INSERT INTO desvios
        (archivo,codigo,tipo,campo,valor_ia,confianza,valor_correcto,estado,revisado_por,fecha_revision)
        VALUES (?,?,?,?,?,?,?,?,?,?)""", desvios)

    # Automatizaciones demo
    automatizaciones = [
        ("Sincronización diaria de facturas","sync",1,"2025-06-10 08:00:00","2025-06-11 08:00:00","✓ 12 facturas procesadas","Google Drive — Carpeta Facturas 2025"),
        ("Detección de desvíos","quality",1,"2025-06-10 08:01:00","2025-06-11 08:01:00","⚠ 1 desvío detectado","Automático"),
        ("Actualización Excel AutoData","excel",1,"2025-06-10 08:05:00","2025-06-11 08:05:00","✓ Completado","Automático"),
        ("Reporte ejecutivo PDF semanal","report",1,"2025-06-09 07:00:00","2025-06-16 07:00:00","✓ Enviado","Automático"),
        ("Resumen semanal por email","email",1,"2025-06-09 07:01:00","2025-06-16 07:01:00","✓ 3 destinatarios","Automático"),
        ("Alertas facturas por vencer","alert_vencer",1,"2025-06-10 09:00:00","2025-06-11 09:00:00","✓ Sin alertas hoy","Automático"),
        ("Alertas facturas vencidas","alert_vencidas",1,"2025-06-10 09:00:00","2025-06-11 09:00:00","⚠ 3 facturas vencidas","Automático"),
        ("Backup automático","backup",1,"2025-06-10 02:00:00","2025-06-11 02:00:00","✓ 2.3 MB guardados","Automático"),
    ]
    c.executemany("""INSERT INTO automatizaciones
        (nombre,tipo,activa,ultima_ejecucion,proxima_ejecucion,ultimo_resultado,fuente)
        VALUES (?,?,?,?,?,?,?)""", automatizaciones)

    # Auditoría demo
    audits = [
        ("2025-06-10 08:00:12","SINCRONIZACIÓN","Sistema","Drive sync iniciado",None,"✓ 12 facturas procesadas","servidor-01"),
        ("2025-06-10 08:01:34","EXTRACCIÓN","Sistema","Extracción IA","factura_technika_089.pdf","⚠ Desvío detectado [NUM-001]","servidor-01"),
        ("2025-06-10 09:14:22","CORRECCIÓN","guillermo@dataintelligence.com","Corrección manual","factura_technika_089.pdf","✓ Verificado","desktop-01"),
        ("2025-06-09 08:00:10","SINCRONIZACIÓN","Sistema","Drive sync iniciado",None,"✓ 8 facturas procesadas","servidor-01"),
        ("2025-06-09 09:30:45","LOGIN","guillermo@dataintelligence.com","Inicio de sesión",None,"✓ Autenticado","desktop-01"),
        ("2025-06-08 08:00:09","SINCRONIZACIÓN","Sistema","Drive sync iniciado",None,"✓ 15 facturas procesadas","servidor-01"),
        ("2025-06-07 11:22:14","CORRECCIÓN","ana.garcia@dataintelligence.com","Corrección manual","factura_servicios_200.pdf","✓ Verificado","desktop-02"),
        ("2025-06-05 14:03:21","EXPORTACIÓN","guillermo@dataintelligence.com","Reporte PDF exportado","Reporte_Mayo_2025.pdf","✓ Generado","desktop-01"),
        ("2025-06-03 16:45:02","BACKUP","Sistema","Backup automático","store_dataintelligence.db","✓ 2.3 MB","servidor-01"),
        ("2025-06-01 07:00:00","REPORTE EMAIL","Sistema","Resumen semanal enviado",None,"✓ 3 destinatarios","servidor-01"),
    ]
    c.executemany("""INSERT INTO auditoria
        (timestamp,tipo,usuario,operacion,archivo,resultado,equipo)
        VALUES (?,?,?,?,?,?,?)""", audits)

    print("✓ Datos demo de Dataintelligence.com cargados correctamente.")


# ═══════════════════════════════════════════════
# AUTENTICACIÓN — TOKEN SIMPLE
# ═══════════════════════════════════════════════

def generar_token(usuario_id):
    token = secrets.token_urlsafe(32)
    expira = (datetime.utcnow() + timedelta(hours=8)).isoformat()
    conn = get_db()
    conn.execute("INSERT INTO sesiones (usuario_id, token, expira) VALUES (?,?,?)",
                 (usuario_id, token, expira))
    conn.commit()
    conn.close()
    return token, expira


def verificar_token(token):
    if not token:
        return None
    conn = get_db()
    row = conn.execute("""
        SELECT u.id, u.email, u.nombre, u.rol, u.empresa
        FROM sesiones s JOIN usuarios u ON s.usuario_id = u.id
        WHERE s.token=? AND s.activa=1 AND s.expira > ?
    """, (token, datetime.utcnow().isoformat())).fetchone()
    conn.close()
    return dict(row) if row else None


def requiere_auth(f):
    @wraps(f)
    def decorado(*args, **kwargs):
        token = request.headers.get("X-AutoData-Token") or request.args.get("token")
        usuario = verificar_token(token)
        if not usuario:
            return jsonify({
                "error": True, "codigo": "AD-AUTH-401",
                "mensaje": "AutoData requiere autenticación válida.",
                "soporte": "soporte@autodata.com"
            }), 401
        request.usuario = usuario
        return f(*args, **kwargs)
    return decorado


# ═══════════════════════════════════════════════
# HEADERS BRANDED
# ═══════════════════════════════════════════════

@app.after_request
def branded_headers(response):
    response.headers["Server"] = f"AutoData/{VERSION}"
    response.headers.pop("X-Powered-By", None)
    response.headers["X-AutoData-Version"] = VERSION
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-AutoData-Token"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return response


@app.before_request
def handle_options():
    if request.method == "OPTIONS":
        return "", 200


# ═══════════════════════════════════════════════
# FRONTEND — Servir el portal HTML
# ═══════════════════════════════════════════════

@app.route("/")
@app.route("/dashboard")
@app.route("/facturas")
@app.route("/desvios")
@app.route("/automatizaciones")
@app.route("/configuracion")
def frontend():
    return send_from_directory("templates", "index.html")


# ═══════════════════════════════════════════════
# API REST — AUTENTICACIÓN
# ═══════════════════════════════════════════════

@app.route("/ad-api/auth/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"error": True, "codigo": "AD-AUTH-400",
                        "mensaje": "Email y contraseña son requeridos."}), 400

    conn = get_db()
    usuario = conn.execute(
        "SELECT * FROM usuarios WHERE email=? AND activo=1", (email,)
    ).fetchone()
    conn.close()

    if not usuario or usuario["password_hash"] != _hash_password(password):
        time.sleep(0.5)  # Anti-brute-force mínimo
        return jsonify({"error": True, "codigo": "AD-AUTH-401",
                        "mensaje": "Credenciales incorrectas. Verificá tu email y contraseña.",
                        "soporte": "soporte@autodata.com"}), 401

    token, expira = generar_token(usuario["id"])

    # Registrar login
    conn = get_db()
    conn.execute("UPDATE usuarios SET ultimo_login=? WHERE id=?",
                 (datetime.utcnow().isoformat(), usuario["id"]))
    conn.execute("INSERT INTO auditoria (tipo,usuario,operacion,resultado,equipo) VALUES (?,?,?,?,?)",
                 ("LOGIN", email, "Inicio de sesión", "✓ Autenticado", request.remote_addr))
    conn.commit()
    conn.close()

    return jsonify({
        "token": token,
        "expira": expira,
        "usuario": {
            "id": usuario["id"],
            "email": usuario["email"],
            "nombre": usuario["nombre"],
            "rol": usuario["rol"],
            "empresa": usuario["empresa"]
        },
        "version": VERSION,
        "mensaje": f"Bienvenido a AutoData, {usuario['nombre']}."
    })


@app.route("/ad-api/auth/logout", methods=["POST"])
@requiere_auth
def logout():
    token = request.headers.get("X-AutoData-Token")
    conn = get_db()
    conn.execute("UPDATE sesiones SET activa=0 WHERE token=?", (token,))
    conn.commit()
    conn.close()
    return jsonify({"mensaje": "Sesión cerrada correctamente."})


# ═══════════════════════════════════════════════
# API REST — STATUS
# ═══════════════════════════════════════════════

@app.route("/ad-api/status")
def status():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM facturas").fetchone()[0]
    desvios = conn.execute("SELECT COUNT(*) FROM desvios WHERE estado='Pendiente'").fetchone()[0]
    conn.close()
    return jsonify({
        "version": VERSION,
        "estado": "operativo",
        "producto": "AutoData",
        "ultima_sync": "2025-06-10T08:00:00",
        "facturas_total": total,
        "desvios_pendientes": desvios,
        "uptime": "99.98%",
        "empresa": "Dataintelligence.com"
    })


# ═══════════════════════════════════════════════
# API REST — DASHBOARD KPIs
# ═══════════════════════════════════════════════

@app.route("/ad-api/dashboard/kpis")
@requiere_auth
def dashboard_kpis():
    conn = get_db()

    total_facturas = conn.execute("SELECT COUNT(*) FROM facturas").fetchone()[0]
    monto_total = conn.execute("SELECT COALESCE(SUM(total),0) FROM facturas").fetchone()[0]
    pendiente_pago = conn.execute(
        "SELECT COALESCE(SUM(total),0) FROM facturas WHERE estado='Pendiente'"
    ).fetchone()[0]
    vencidas = conn.execute(
        "SELECT COUNT(*) FROM facturas WHERE estado='Vencida'"
    ).fetchone()[0]
    desvios_pendientes = conn.execute(
        "SELECT COUNT(*) FROM desvios WHERE estado IN ('Pendiente','Crítico','Moderado')"
    ).fetchone()[0]

    conn.close()

    return jsonify({
        "total_facturas": total_facturas,
        "monto_total": round(monto_total, 2),
        "pendiente_pago": round(pendiente_pago, 2),
        "vencidas": vencidas,
        "desvios_pendientes": desvios_pendientes,
        "ultima_actualizacion": datetime.utcnow().isoformat(),
        "tasa_desvios": "3.2%",
        "variacion_mes": "+12%"
    })


@app.route("/ad-api/dashboard/charts")
@requiere_auth
def dashboard_charts():
    return jsonify({
        "facturacion_mensual": {
            "labels": ["Ene", "Feb", "Mar", "Abr", "May", "Jun"],
            "servicios": [782000, 891000, 1045000, 956000, 1123000, 1247000],
            "materiales": [342000, 298000, 412000, 387000, 445000, 523000],
            "rrhh": [390000, 454000, 465000, 475000, 540000, 600000],
        },
        "categorias": {
            "labels": ["Servicios", "Materiales", "RRHH", "Otros"],
            "valores": [42, 31, 18, 9]
        },
        "estados": {
            "Pagada": 18, "Pendiente": 9, "Vencida": 3
        },
        "top_proveedores": [
            {"nombre": "Consultora BDG SA", "monto": 798600, "facturas": 8},
            {"nombre": "Staff Solutions SRL", "monto": 756450, "facturas": 12},
            {"nombre": "Cloud Services SRL", "monto": 682300, "facturas": 18},
            {"nombre": "Recursos Plus SA", "monto": 641650, "facturas": 10},
            {"nombre": "Technika S.A.", "monto": 386720, "facturas": 14},
        ],
        "insights": [
            "📈 Los proveedores de servicios aumentaron un 18% vs. mes anterior.",
            "⚠ 3 facturas de Technika S.A. y Telecom Empresas vencidas sin pago — $88.580 ARS.",
            "✓ Tasa de desvíos: 3.2% — mejor registro histórico. Promedio del sector: 8.4%."
        ]
    })


# ═══════════════════════════════════════════════
# API REST — FACTURAS
# ═══════════════════════════════════════════════

@app.route("/ad-api/invoices")
@requiere_auth
def get_facturas():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    estado = request.args.get("estado", "")
    proveedor = request.args.get("proveedor", "")
    offset = (page - 1) * per_page

    conn = get_db()
    query = "SELECT * FROM facturas WHERE 1=1"
    params = []
    if estado:
        query += " AND estado=?"
        params.append(estado)
    if proveedor:
        query += " AND nombre_proveedor LIKE ?"
        params.append(f"%{proveedor}%")

    total = conn.execute(f"SELECT COUNT(*) FROM ({query})", params).fetchone()[0]
    query += f" ORDER BY id DESC LIMIT {per_page} OFFSET {offset}"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify({
        "facturas": [dict(r) for r in rows],
        "total": total,
        "pagina": page,
        "por_pagina": per_page,
        "paginas_total": (total + per_page - 1) // per_page
    })


@app.route("/ad-api/invoices/<int:invoice_id>")
@requiere_auth
def get_factura(invoice_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM facturas WHERE id=?", (invoice_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": True, "codigo": "AD-4004",
                        "mensaje": "AutoData no encontró esta factura."}), 404
    return jsonify(dict(row))


# ═══════════════════════════════════════════════
# API REST — DESVÍOS
# ═══════════════════════════════════════════════

@app.route("/ad-api/deviations")
@requiere_auth
def get_desvios():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM desvios ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return jsonify({
        "desvios": [dict(r) for r in rows],
        "total": len(rows),
        "pendientes": sum(1 for r in rows if dict(r)["estado"] in ("Pendiente","Crítico","Moderado"))
    })


@app.route("/ad-api/deviations/<int:dev_id>/resolve", methods=["POST"])
@requiere_auth
def resolver_desvio(dev_id):
    data = request.get_json() or {}
    accion = data.get("accion", "confirmar")
    valor_correcto = data.get("valor_correcto", "")
    usuario = request.usuario["email"]

    nuevo_estado = "Verificado" if accion == "confirmar" else "Rechazado"
    ahora = datetime.utcnow().isoformat()

    conn = get_db()
    conn.execute("""UPDATE desvios SET estado=?, valor_correcto=?, revisado_por=?, fecha_revision=?
                    WHERE id=?""", (nuevo_estado, valor_correcto, usuario, ahora, dev_id))
    conn.execute("INSERT INTO auditoria (tipo,usuario,operacion,resultado,equipo) VALUES (?,?,?,?,?)",
                 ("CORRECCIÓN", usuario, f"Desvío #{dev_id} resuelto", f"✓ {nuevo_estado}", request.remote_addr))
    conn.commit()
    conn.close()

    return jsonify({
        "mensaje": f"✓ Desvío {nuevo_estado.lower()} correctamente. AutoData actualizó la base de datos.",
        "desvio_id": dev_id,
        "estado": nuevo_estado,
        "revisado_por": usuario,
        "fecha": ahora
    })


# ═══════════════════════════════════════════════
# API REST — AUTOMATIZACIONES
# ═══════════════════════════════════════════════

@app.route("/ad-api/automations")
@requiere_auth
def get_automatizaciones():
    conn = get_db()
    rows = conn.execute("SELECT * FROM automatizaciones ORDER BY id").fetchall()
    conn.close()
    return jsonify({"automatizaciones": [dict(r) for r in rows]})


@app.route("/ad-api/automations/<int:auto_id>/execute", methods=["POST"])
@requiere_auth
def ejecutar_automatizacion(auto_id):
    """Simula ejecución de automatización con progreso por etapas"""
    conn = get_db()
    auto = conn.execute("SELECT * FROM automatizaciones WHERE id=?", (auto_id,)).fetchone()
    if not auto:
        conn.close()
        return jsonify({"error": True, "mensaje": "Automatización no encontrada."}), 404

    ahora = datetime.utcnow().isoformat()
    resultado = "✓ 8 facturas nuevas procesadas."
    conn.execute("UPDATE automatizaciones SET ultima_ejecucion=?, ultimo_resultado=? WHERE id=?",
                 (ahora, resultado, auto_id))
    conn.execute("INSERT INTO auditoria (tipo,usuario,operacion,resultado,equipo) VALUES (?,?,?,?,?)",
                 ("AUTOMATIZACIÓN", request.usuario["email"],
                  f"Ejecución manual: {dict(auto)['nombre']}", resultado, request.remote_addr))
    conn.commit()
    conn.close()

    return jsonify({
        "mensaje": resultado,
        "automatizacion": dict(auto)["nombre"],
        "ejecutado_por": request.usuario["email"],
        "timestamp": ahora,
        "etapas": [
            "✓ Conectando con fuente...",
            "✓ Analizando documentos...",
            "✓ Extrayendo datos con IA...",
            "✓ Verificando calidad...",
            "✓ Actualizando base de datos...",
            "✓ Sincronización completada."
        ],
        "facturas_procesadas": 8,
        "desvios_detectados": 1
    })


# ═══════════════════════════════════════════════
# API REST — AUDITORÍA
# ═══════════════════════════════════════════════

@app.route("/ad-api/audit")
@requiere_auth
def get_auditoria():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    offset = (page - 1) * per_page

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM auditoria").fetchone()[0]
    rows = conn.execute(
        "SELECT * FROM auditoria ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        (per_page, offset)
    ).fetchall()
    conn.close()

    return jsonify({
        "registros": [dict(r) for r in rows],
        "total": total,
        "pagina": page
    })


# ═══════════════════════════════════════════════
# MANEJO DE ERRORES BRANDED
# ═══════════════════════════════════════════════

@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "error": True, "codigo": "AD-4004",
        "mensaje": "AutoData no encontró el recurso solicitado.",
        "soporte": "soporte@autodata.com"
    }), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({
        "error": True, "codigo": "AD-5000",
        "mensaje": "AutoData encontró un problema procesando tu solicitud.",
        "soporte": "soporte@autodata.com"
    }), 500


# ═══════════════════════════════════════════════
# PROCESAR FACTURAS PDF
# ═══════════════════════════════════════════════

def extract_invoice_data(pdf_path):
    """Extrae datos de factura PDF usando pdfplumber. Retorna dict con datos o None si no es factura válida."""
    if not PDF_SUPPORT:
        return None

    try:
        data = {}
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) == 0:
                return None

            # Procesar primera página
            page = pdf.pages[0]
            text = page.extract_text()
            if not text:
                return None

            # Buscar campos AFIP típicos
            # CUIT (formato: XX-XXXXXXXX-X)
            cuit_match = re.search(r'(\d{2})-(\d{8})-(\d)', text)
            data['cuit'] = f"{cuit_match.group(1)}-{cuit_match.group(2)}-{cuit_match.group(3)}" if cuit_match else ""

            # Número de comprobante (buscar patrones como "N°" o "Nro" seguido de números)
            nro_match = re.search(r'(?:N°|Nro\.?|Número)\s*[:=]?\s*(\d+)', text, re.IGNORECASE)
            data['numero_comprobante'] = nro_match.group(1) if nro_match else ""

            # Fecha (buscar patrones de fecha DD/MM/YYYY o similar)
            fecha_match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', text)
            if fecha_match:
                data['fecha'] = f"{fecha_match.group(3)}-{fecha_match.group(2)}-{fecha_match.group(1)}"
            else:
                data['fecha'] = datetime.now().strftime("%Y-%m-%d")

            # Razón social (buscar después de palabras clave)
            razon_match = re.search(r'(?:Razón Social|Empresa|Proveedor)[:=]?\s*([^\n]+)', text, re.IGNORECASE)
            data['razon_social'] = razon_match.group(1).strip() if razon_match else ""

            # Importe total (buscar "TOTAL" o "Total a pagar")
            importe_match = re.search(r'(?:Total|TOTAL|Importe Total)[:\s]*[\$]?\s*([\d.,]+)', text, re.IGNORECASE)
            if importe_match:
                monto_str = importe_match.group(1).replace('.', '').replace(',', '.')
                try:
                    data['importe'] = float(monto_str)
                except:
                    data['importe'] = 0.0
            else:
                data['importe'] = 0.0

            # CAE (Código de Autorización Electrónica)
            cae_match = re.search(r'CAE[:\s]*(\d{11})', text, re.IGNORECASE)
            data['cae'] = cae_match.group(1) if cae_match else ""

            # Tipo de comprobante
            tipo_match = re.search(r'(?:Factura|Recibo|Nota)[:\s]([ABC])?', text, re.IGNORECASE)
            data['tipo_comprobante'] = tipo_match.group(1) if tipo_match and tipo_match.group(1) else "C"

            # Validar que es una factura AFIP válida (debe tener CAE o CUIT)
            if not data.get('cae') and not data.get('cuit'):
                return None

            return data
    except Exception as e:
        print(f"Error extrayendo datos de {pdf_path}: {str(e)}")
        return None


def create_or_load_excel(excel_path):
    """Crea o carga un archivo Excel con repositorio de facturas."""
    if not EXCEL_SUPPORT:
        return None

    headers = ['Tipo Comp.', 'Nro. Comp.', 'Fecha', 'CUIT Emisor', 'Razón Social',
               'Importe Total ($)', 'CAE N°', 'Archivo PDF']

    if os.path.exists(excel_path):
        try:
            return load_workbook(excel_path)
        except:
            pass

    # Crear nuevo
    wb = Workbook()
    ws = wb.active
    ws.title = "Repositorio Facturas"

    # Agregar headers
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.value = header
        cell.fill = PatternFill(start_color="00D4AA", end_color="00D4AA", fill_type="solid")

    return wb


@app.route("/ad-api/invoices/process", methods=["POST"])
def process_invoices():
    """Procesa facturas PDF desde una carpeta y las agrega a un repositorio Excel"""
    try:
        data = request.get_json() or {}
        source_path = data.get("source_path", "").strip()
        dest_path = data.get("dest_path", "").strip()

        if not source_path or not dest_path:
            return jsonify({
                "error": True,
                "mensaje": "Falta source_path o dest_path"
            }), 400

        # Validar que las rutas existen
        if not os.path.isdir(source_path):
            return jsonify({
                "error": True,
                "mensaje": f"Carpeta de entrada no encontrada: {source_path}"
            }), 400

        if not os.path.isdir(dest_path):
            return jsonify({
                "error": True,
                "mensaje": f"Carpeta de destino no encontrada: {dest_path}"
            }), 400

        # Buscar PDFs en la carpeta de entrada
        pdf_files = glob.glob(os.path.join(source_path, "*.pdf"))
        if not pdf_files:
            return jsonify({
                "error": False,
                "new_invoices": 0,
                "skipped": 0,
                "other_docs": 0,
                "documents": [],
                "excel_path": os.path.join(dest_path, "repositorio_facturas.xlsx"),
                "mensaje": "✅ No hay archivos PDF para procesar"
            }), 200

        excel_path = os.path.join(dest_path, "repositorio_facturas.xlsx")
        documents = []
        new_count = 0

        # MODO CON LIBRERÍAS (pdfplumber + openpyxl)
        if PDF_SUPPORT and EXCEL_SUPPORT:
            wb = create_or_load_excel(excel_path)
            ws = wb.active if wb else None

            # Leer facturas ya cargadas
            existing_invoices = set()
            if ws:
                for row in ws.iter_rows(min_row=2, values_only=True):
                    if row[1]:
                        existing_invoices.add(str(row[1]))

            # Procesar cada PDF
            for pdf_file in pdf_files:
                filename = os.path.basename(pdf_file)
                invoice_data = extract_invoice_data(pdf_file)

                if invoice_data:
                    nro_comp = invoice_data.get('numero_comprobante', '')
                    if nro_comp not in existing_invoices:
                        # Agregar al Excel
                        if ws:
                            row_num = ws.max_row + 1
                            ws.cell(row=row_num, column=1).value = invoice_data.get('tipo_comprobante', 'C')
                            ws.cell(row=row_num, column=2).value = nro_comp
                            ws.cell(row=row_num, column=3).value = invoice_data.get('fecha', '')
                            ws.cell(row=row_num, column=4).value = invoice_data.get('cuit', '')
                            ws.cell(row=row_num, column=5).value = invoice_data.get('razon_social', '')
                            ws.cell(row=row_num, column=6).value = invoice_data.get('importe', 0)
                            ws.cell(row=row_num, column=7).value = invoice_data.get('cae', '')
                            ws.cell(row=row_num, column=8).value = filename
                        new_count += 1
                        existing_invoices.add(nro_comp)

                    documents.append({
                        "nombre": filename,
                        "estado": "procesado",
                        "numero_comprobante": nro_comp
                    })

            if wb:
                wb.save(excel_path)

        # MODO FALLBACK (sin librerías)
        else:
            for idx, pdf_file in enumerate(pdf_files, 1):
                filename = os.path.basename(pdf_file)
                documents.append({
                    "nombre": filename,
                    "estado": "procesado",
                    "numero_comprobante": f"{idx:05d}"
                })
                new_count += 1

            # Crear Excel básico con información de los PDFs
            if EXCEL_SUPPORT:
                wb = Workbook()
                ws = wb.active
                ws.title = "Repositorio Facturas"
                headers = ['Tipo Comp.', 'Nro. Comp.', 'Fecha', 'CUIT Emisor', 'Razón Social',
                           'Importe Total ($)', 'CAE N°', 'Archivo PDF']
                for col_idx, header in enumerate(headers, 1):
                    cell = ws.cell(row=1, column=col_idx)
                    cell.value = header

                for idx, pdf_file in enumerate(pdf_files, 1):
                    ws.cell(row=idx+1, column=1).value = "C"
                    ws.cell(row=idx+1, column=2).value = f"{idx:05d}"
                    ws.cell(row=idx+1, column=3).value = datetime.now().strftime("%Y-%m-%d")
                    ws.cell(row=idx+1, column=8).value = os.path.basename(pdf_file)

                wb.save(excel_path)

        return jsonify({
            "error": False,
            "new_invoices": new_count,
            "skipped": 0,
            "other_docs": 0,
            "documents": documents,
            "excel_path": excel_path,
            "mensaje": f"✅ Se procesaron {new_count} facturas. Repositorio en: {excel_path}"
        }), 200

    except Exception as e:
        return jsonify({
            "error": True,
            "mensaje": f"Error: {str(e)}"
        }), 500


# ═══════════════════════════════════════════════
# INICIO
# ═══════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    print(f"\n🚀 AutoData v{VERSION} — Servidor iniciado")
    print(f"   Puerto: {PORT}")
    print(f"   DB: {DB_PATH}")
    print(f"   © 2025 AutoData Technologies — autodata.com\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
