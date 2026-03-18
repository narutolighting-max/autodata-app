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
    from openpyxl.styles import PatternFill, Font, Alignment
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
# PROCESAR FACTURAS PDF (upload desde navegador)
# ═══════════════════════════════════════════════

REPO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
EXCEL_FILE = os.path.join(REPO_DIR, "repositorio_facturas.xlsx")
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(REPO_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

# 21 columnas del repositorio (A-U)
EXCEL_HEADERS = [
    'Tipo Comp.', 'Punto Vta.', 'Nro. Comp.', 'Fecha Emisión',
    'CUIT Emisor', 'Razón Social Emisor', 'Domicilio Emisor', 'Cond. IVA Emisor',
    'CUIT Cliente', 'Razón Social Cliente', 'Domicilio Cliente', 'Cond. IVA Cliente',
    'Cond. Venta', 'Per. Desde', 'Per. Hasta', 'Vto. Pago',
    'Producto / Servicio', 'Importe Total ($)', 'CAE N°', 'Vto. CAE', 'Archivo PDF'
]

# Grupos de headers (fila 3 con merge)
HEADER_GROUPS = [
    ('A3', 'C3', 'COMPROBANTE'),
    ('D3', 'H3', 'EMISOR'),
    ('I3', 'M3', 'CLIENTE'),
    ('N3', 'P3', 'PERÍODO / VTO.'),
    ('Q3', 'Q3', 'DETALLE'),
    ('R3', 'S3', 'IMPORTES / CAE'),
    ('T3', 'U3', 'ARCHIVO'),
]

COL_WIDTHS = {
    'A': 12, 'B': 10, 'C': 14, 'D': 14, 'E': 18, 'F': 28,
    'G': 36, 'H': 18, 'I': 18, 'J': 32, 'K': 40, 'L': 20,
    'M': 12, 'N': 12, 'O': 12, 'P': 12, 'Q': 24, 'R': 18,
    'S': 20, 'T': 12, 'U': 44
}

YELLOW_FILL = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
DARK_BLUE = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
MED_BLUE = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
WHITE_BOLD_13 = Font(name="Arial", size=13, bold=True, color="FFFFFF")
WHITE_BOLD_9 = Font(name="Arial", size=9, bold=True, color="FFFFFF")
DATA_FONT = Font(name="Arial", size=9)
CENTER = Alignment(horizontal="center", vertical="center")
LEFT = Alignment(horizontal="left", vertical="center")


def get_or_create_excel():
    if not EXCEL_SUPPORT:
        return None
    if os.path.exists(EXCEL_FILE):
        try:
            return load_workbook(EXCEL_FILE)
        except Exception as e:
            print(f"Error cargando Excel: {e}")

    wb = Workbook()
    ws = wb.active
    ws.title = "Repositorio Facturas"

    # Row 1: Title (merged A1:U1)
    ws.merge_cells('A1:U1')
    title_cell = ws['A1']
    title_cell.value = "REPOSITORIO DE FACTURAS"
    title_cell.font = WHITE_BOLD_13
    title_cell.fill = DARK_BLUE
    title_cell.alignment = CENTER

    # Row 3: Group headers (merged)
    for start, end, label in HEADER_GROUPS:
        if start != end:
            ws.merge_cells(f'{start}:{end}')
        cell = ws[start]
        cell.value = label
        cell.font = WHITE_BOLD_9
        cell.fill = DARK_BLUE
        cell.alignment = CENTER

    # Row 4: Sub-headers
    for col_idx, header in enumerate(EXCEL_HEADERS, 1):
        cell = ws.cell(row=4, column=col_idx)
        cell.value = header
        cell.font = WHITE_BOLD_9
        cell.fill = MED_BLUE
        cell.alignment = CENTER

    # Column widths
    for col_letter, width in COL_WIDTHS.items():
        ws.column_dimensions[col_letter].width = width

    # Freeze panes below headers
    ws.freeze_panes = 'A5'

    # Create "Otros Documentos" sheet
    ws2 = wb.create_sheet("Otros Documentos")
    ws2.merge_cells('A1:E1')
    ws2['A1'].value = "DOCUMENTOS NO RELACIONADOS A FACTURACIÓN"
    ws2['A1'].font = WHITE_BOLD_13
    ws2['A1'].fill = DARK_BLUE
    ws2['A1'].alignment = CENTER
    ws2.merge_cells('A2:E2')
    ws2['A2'].value = "Archivos encontrados que NO son comprobantes de venta"
    ws2['A2'].font = Font(name="Arial", size=9, italic=True, color="666666")

    otros_headers = ['Nombre del Archivo', 'Tipo', 'Observación', 'Fecha Detección', 'Acción Sugerida']
    for col_idx, h in enumerate(otros_headers, 1):
        cell = ws2.cell(row=4, column=col_idx)
        cell.value = h
        cell.font = WHITE_BOLD_9
        cell.fill = MED_BLUE
        cell.alignment = CENTER
    ws2.column_dimensions['A'].width = 42
    ws2.column_dimensions['B'].width = 10
    ws2.column_dimensions['C'].width = 60
    ws2.column_dimensions['D'].width = 16
    ws2.column_dimensions['E'].width = 36
    ws2.freeze_panes = 'A5'

    # Create "Resumen" sheet
    ws3 = wb.create_sheet("Resumen")
    ws3['A1'].value = "RESUMEN EJECUTIVO"
    ws3['A1'].font = WHITE_BOLD_13
    ws3['A1'].fill = DARK_BLUE
    ws3.merge_cells('A1:B1')
    ws3['A1'].alignment = CENTER
    ws3['A2'].value = "Concepto"
    ws3['A2'].font = WHITE_BOLD_9
    ws3['A2'].fill = MED_BLUE
    ws3['B2'].value = "Valor"
    ws3['B2'].font = WHITE_BOLD_9
    ws3['B2'].fill = MED_BLUE
    ws3['A3'].value = "Cantidad de Facturas"
    ws3['B3'] = "=COUNTA('Repositorio Facturas'!C5:C9999)"
    ws3['A4'].value = "Importe total facturado ($)"
    ws3['B4'] = "=SUM('Repositorio Facturas'!R5:R9999)"
    ws3.column_dimensions['A'].width = 30
    ws3.column_dimensions['B'].width = 24

    wb.save(EXCEL_FILE)
    return wb


def get_existing_invoices(ws):
    existing = set()
    if ws:
        for row in ws.iter_rows(min_row=5, values_only=True):
            if row and len(row) > 2 and row[2]:
                existing.add(str(row[2]).strip())
    return existing


def get_existing_otros(ws):
    existing = set()
    if ws:
        for row in ws.iter_rows(min_row=5, values_only=True):
            if row and row[0]:
                existing.add(str(row[0]).strip())
    return existing


def extract_invoice_data(pdf_path):
    if not PDF_SUPPORT:
        return None
    try:
        data = {}
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) == 0:
                return None
            text = pdf.pages[0].extract_text()
            if not text:
                return None

            # Tipo comprobante (Factura A/B/C, Nota de Credito, Recibo)
            tipo_match = re.search(r'(FACTURA|Factura|RECIBO|NOTA DE CR[ÉE]DITO|NOTA DE D[ÉE]BITO)\s*([ABC])?', text, re.IGNORECASE)
            if tipo_match:
                tipo_name = tipo_match.group(1).strip().title()
                tipo_letra = tipo_match.group(2).upper() if tipo_match.group(2) else ""
                data['tipo'] = f"{tipo_name} {tipo_letra}".strip()
            else:
                data['tipo'] = ""

            # Punto de venta y Nro comprobante (separados)
            pv_match = re.search(r'Punto de Venta:\s*(\d+)\s*Comp\.?\s*Nro:?\s*(\d+)', text, re.IGNORECASE)
            if pv_match:
                data['punto_vta'] = pv_match.group(1).zfill(5)
                data['nro_comp'] = pv_match.group(2).zfill(8)
            else:
                nro_match = re.search(r'(?:Comp\.?\s*Nro|N[°º]|Nro\.?|N[úu]mero)[\s.:]*(\d[\d-]*\d)', text, re.IGNORECASE)
                data['nro_comp'] = nro_match.group(1).strip() if nro_match else ""
                data['punto_vta'] = ""

            # Fecha emision
            fecha_em = re.search(r'(?:Fecha\s*de\s*Emisi[óo]n|Fecha\s*Emisi[óo]n)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
            if not fecha_em:
                fecha_em = re.search(r'Fecha[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
            data['fecha'] = fecha_em.group(1) if fecha_em else ""

            # CUITs (XX-XXXXXXXX-X)
            cuit_all = re.findall(r'(\d{2}-\d{8}-\d)', text)
            data['cuit_emisor'] = cuit_all[0] if len(cuit_all) > 0 else ""
            data['cuit_cliente'] = cuit_all[1] if len(cuit_all) > 1 else ""

            # Razones sociales
            razones = re.findall(r'(?:Raz[óo]n Social|Apellido y Nombre|Denominaci[óo]n)[:\s]*([^\n]+)', text, re.IGNORECASE)
            data['razon_social_emisor'] = razones[0].strip()[:80] if len(razones) > 0 else ""
            data['razon_social_cliente'] = razones[1].strip()[:80] if len(razones) > 1 else ""

            # Domicilios
            domicilios = re.findall(r'(?:Domicilio\s*(?:Comercial)?|Direcci[óo]n)[:\s]*([^\n]+)', text, re.IGNORECASE)
            data['domicilio_emisor'] = domicilios[0].strip()[:100] if len(domicilios) > 0 else ""
            data['domicilio_cliente'] = domicilios[1].strip()[:100] if len(domicilios) > 1 else ""

            # Condicion IVA
            iva_all = re.findall(r'(?:Condici[óo]n\s*(?:frente al\s*)?IVA|Cond\.?\s*IVA)[:\s]*([^\n]+)', text, re.IGNORECASE)
            data['cond_iva_emisor'] = iva_all[0].strip()[:40] if len(iva_all) > 0 else ""
            data['cond_iva_cliente'] = iva_all[1].strip()[:40] if len(iva_all) > 1 else ""

            # Condicion de venta
            cond_vta = re.search(r'(?:Condici[óo]n\s*de\s*Venta|Cond\.?\s*Venta)[:\s]*([^\n]+)', text, re.IGNORECASE)
            data['cond_venta'] = cond_vta.group(1).strip()[:30] if cond_vta else ""

            # Periodos
            per_desde = re.search(r'(?:Per[íi]odo\s*(?:Facturado\s*)?Desde|Desde)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
            per_hasta = re.search(r'(?:Per[íi]odo\s*(?:Facturado\s*)?Hasta|Hasta)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
            data['per_desde'] = per_desde.group(1) if per_desde else ""
            data['per_hasta'] = per_hasta.group(1) if per_hasta else ""

            # Vto pago
            vto_pago = re.search(r'(?:Vto\.?\s*(?:de\s*)?Pago|Vencimiento\s*(?:del\s*)?Pago|Fecha\s*Vto\.?\s*Pago)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
            data['vto_pago'] = vto_pago.group(1) if vto_pago else ""

            # Producto/Servicio
            prod_match = re.search(r'(?:Descripci[óo]n|Concepto|Servicio|Producto)[:\s]*([^\n]+)', text, re.IGNORECASE)
            data['producto'] = prod_match.group(1).strip()[:100] if prod_match else ""

            # Importe total
            imp_match = re.search(r'(?:Importe\s*Total|Total)[:\s$]*\$?\s*([\d.,]+)', text, re.IGNORECASE)
            if imp_match:
                monto_str = imp_match.group(1).replace('.', '').replace(',', '.')
                try:
                    data['importe'] = float(monto_str)
                except Exception:
                    data['importe'] = 0.0
            else:
                data['importe'] = 0.0

            # CAE
            cae_match = re.search(r'CAE[:\s]*(\d{10,14})', text, re.IGNORECASE)
            data['cae'] = cae_match.group(1) if cae_match else ""

            # Vto CAE
            vto_cae = re.search(r'(?:Vto\.?\s*(?:de\s*)?CAE|Fecha\s*Vto\.?\s*CAE)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
            data['vto_cae'] = vto_cae.group(1) if vto_cae else ""

            return data

    except Exception as e:
        print(f"Error extrayendo PDF {pdf_path}: {e}")
        return None


def add_invoice_row(ws, row_num, data, filename):
    """Agrega una fila de factura. Pinta en amarillo los campos vacios."""
    fields = [
        ('tipo', 1), ('punto_vta', 2), ('nro_comp', 3), ('fecha', 4),
        ('cuit_emisor', 5), ('razon_social_emisor', 6), ('domicilio_emisor', 7),
        ('cond_iva_emisor', 8), ('cuit_cliente', 9), ('razon_social_cliente', 10),
        ('domicilio_cliente', 11), ('cond_iva_cliente', 12), ('cond_venta', 13),
        ('per_desde', 14), ('per_hasta', 15), ('vto_pago', 16),
        ('producto', 17), ('importe', 18), ('cae', 19), ('vto_cae', 20)
    ]
    missing = []
    for key, col in fields:
        cell = ws.cell(row=row_num, column=col)
        val = data.get(key, "")
        cell.value = val
        cell.font = DATA_FONT
        cell.alignment = LEFT if col > 4 else CENTER
        if not val and val != 0:
            cell.fill = YELLOW_FILL
            missing.append(EXCEL_HEADERS[col - 1])

    # Col U = Archivo PDF
    cell_u = ws.cell(row=row_num, column=21)
    cell_u.value = filename
    cell_u.font = DATA_FONT
    cell_u.alignment = LEFT
    return missing


def add_otros_doc(wb, filename, observacion):
    """Agrega un doc no-factura a la hoja Otros Documentos."""
    if "Otros Documentos" not in wb.sheetnames:
        return
    ws2 = wb["Otros Documentos"]
    # Check duplicates
    existing = get_existing_otros(ws2)
    if filename in existing:
        return
    row_num = ws2.max_row + 1
    if row_num < 5:
        row_num = 5
    ext = os.path.splitext(filename)[1].upper().replace('.', '') or 'Desconocido'
    ws2.cell(row=row_num, column=1).value = filename
    ws2.cell(row=row_num, column=1).font = DATA_FONT
    ws2.cell(row=row_num, column=2).value = ext
    ws2.cell(row=row_num, column=2).font = DATA_FONT
    ws2.cell(row=row_num, column=3).value = observacion
    ws2.cell(row=row_num, column=3).font = DATA_FONT
    ws2.cell(row=row_num, column=4).value = datetime.now().strftime("%d/%m/%Y")
    ws2.cell(row=row_num, column=4).font = DATA_FONT
    ws2.cell(row=row_num, column=5).value = "Mover a otra carpeta"
    ws2.cell(row=row_num, column=5).font = DATA_FONT


@app.route("/ad-api/invoices/upload-process", methods=["POST"])
def upload_and_process():
    try:
        files = request.files.getlist("pdfs")
        if not files or len(files) == 0:
            return jsonify({"error": True, "mensaje": "No se recibieron archivos PDF"}), 400

        wb = get_or_create_excel()
        ws = wb.active if wb else None
        existing = get_existing_invoices(ws) if ws else set()

        documents = []
        new_count = 0
        skip_count = 0
        other_count = 0

        for f in files:
            filename = f.filename or "sin_nombre.pdf"

            if not filename.lower().endswith('.pdf'):
                documents.append({"nombre": filename, "estado": "no_pdf", "detalle": "No es PDF"})
                add_otros_doc(wb, filename, "No es un archivo PDF")
                other_count += 1
                continue

            tmp_path = os.path.join(UPLOAD_DIR, filename)
            f.save(tmp_path)

            try:
                invoice_data = extract_invoice_data(tmp_path)

                if invoice_data and invoice_data.get('nro_comp') and invoice_data.get('cae'):
                    nro = invoice_data['nro_comp']

                    if nro in existing:
                        documents.append({
                            "nombre": filename,
                            "estado": "duplicado",
                            "detalle": f"Comp. {nro} ya existe"
                        })
                        skip_count += 1
                    else:
                        if ws:
                            row_num = ws.max_row + 1
                            if row_num < 5:
                                row_num = 5
                            missing = add_invoice_row(ws, row_num, invoice_data, filename)

                        existing.add(nro)
                        new_count += 1
                        detail = f"Comp. {nro} - ${invoice_data.get('importe', 0):,.2f}"
                        if missing:
                            detail += f" (campos faltantes: {', '.join(missing[:3])})"
                        documents.append({
                            "nombre": filename,
                            "estado": "procesado",
                            "detalle": detail
                        })
                else:
                    obs = "No es un comprobante de facturación (AFIP). No tiene CAE ni datos de emisor/cliente CUIT."
                    add_otros_doc(wb, filename, obs)
                    documents.append({
                        "nombre": filename,
                        "estado": "no_factura",
                        "detalle": "Sin CAE/CUIT - no es factura AFIP"
                    })
                    other_count += 1

            except Exception as e:
                documents.append({
                    "nombre": filename,
                    "estado": "error",
                    "detalle": str(e)[:60]
                })
                other_count += 1
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        if wb:
            wb.save(EXCEL_FILE)

        return jsonify({
            "error": False,
            "new_invoices": new_count,
            "skipped": skip_count,
            "other_docs": other_count,
            "documents": documents,
            "excel_download": "/ad-api/invoices/download-excel",
            "mensaje": f"{new_count} facturas nuevas cargadas. {skip_count} duplicadas. {other_count} otros."
        }), 200

    except Exception as e:
        return jsonify({
            "error": True,
            "mensaje": f"Error procesando: {str(e)}"
        }), 500


@app.route("/ad-api/invoices/download-excel")
def download_excel():
    if os.path.exists(EXCEL_FILE):
        return send_from_directory(
            REPO_DIR, "repositorio_facturas.xlsx",
            as_attachment=True, download_name="repositorio_facturas.xlsx"
        )
    return jsonify({"error": True, "mensaje": "El repositorio aún no fue creado"}), 404


@app.route("/ad-api/invoices/stats")
def invoice_stats():
    if not os.path.exists(EXCEL_FILE) or not EXCEL_SUPPORT:
        return jsonify({"total": 0, "exists": False})
    try:
        wb = load_workbook(EXCEL_FILE, read_only=True)
        ws = wb.active
        total = ws.max_row - 4 if ws.max_row > 4 else 0
        wb.close()
        return jsonify({"total": total, "exists": True})
    except Exception:
        return jsonify({"total": 0, "exists": True})


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
