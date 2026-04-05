from fastapi import FastAPI, Form, Request, Query, Depends, HTTPException, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from datetime import datetime, timedelta, date
import io, csv
import secrets
import os
import sys
from jinja2 import Environment, FileSystemLoader

from db_compat import get_db, INTEGRITY_ERRORS, is_postgres

# ==================== ŚCIEŻKI DLA PAKOWANEGO .EXE ====================
def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(__file__)

def get_db_path():
    base = get_base_path()
    return os.path.join(base, "database.db")

def get_templates_path():
    base = get_base_path()
    return os.path.join(base, "templates")

def get_resources_path():
    """Zwraca ścieżkę do folderu zasobów (CSV, assets itp)"""
    base = get_base_path()
    resources = os.path.join(base, "resources")
    # Jeśli resources nie istnieje, zwróć base (dla backward compatibility)
    if os.path.isdir(resources):
        return resources
    return base

# ==================== APLIKACJA ====================
app = FastAPI()
# Na Render z gunicorn -w 4 każdy worker musi mieć ten sam secret (ustaw zmienną SESSION_SECRET).
_session_secret = os.environ.get("SESSION_SECRET") or secrets.token_urlsafe(32)
app.add_middleware(SessionMiddleware, secret_key=_session_secret)
app.mount("/static", StaticFiles(directory=os.path.join(get_base_path(), "static")), name="static")

# Ręczne utworzenie środowiska Jinja2 (bez cache – działa w embedded)
# Inicjalizujemy po zdefiniowaniu get_templates_path()
env = None

def init_jinja_env():
    global env
    if env is None:
        env = Environment(
            loader=FileSystemLoader(get_templates_path()),
            autoescape=True,
            cache_size=0
        )

def render_template(name: str, context: dict):
    init_jinja_env()
    template = env.get_template(name)
    return HTMLResponse(template.render(context))

# ----------------- AUTH DEPENDENCY -----------------
def get_current_user(request: Request):
    username = request.session.get("username")
    role = request.session.get("role")
    if username and role:
        return {"username": username, "role": role}
    return None

def require_auth(user: dict = Depends(get_current_user)):
    if not user:
        raise HTTPException(status_code=303, detail="Zaloguj się", headers={"Location": "/login"})
    return user

def require_admin(user: dict = Depends(get_current_user)):
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Brak dostępu")
    return user

def require_manager_or_admin(user: dict = Depends(get_current_user)):
    if not user or user.get("role") not in ["admin", "manager"]:
        raise HTTPException(status_code=403, detail="Brak dostępu")
    return user

def is_ajax(request: Request) -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"

# ----------------- HELPERY -----------------
# get_db: db_compat (SQLite lokalnie lub PostgreSQL z DATABASE_URL)

def dodaj_operacje(cur, typ, farba, ilosc, polka, uwagi="", farba_id=None):
    cur.execute("""
        INSERT INTO operacje (data, typ, farba, ilosc, polka, uwagi, farba_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        typ, farba, ilosc, polka, uwagi, farba_id
    ))

def log_production_operation(cur, operation_type, description, machine=None, plan_id=None, user="system"):
    cur.execute("""
        INSERT INTO production_log (operation_type, description, machine, plan_id, user, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    """, (operation_type, description, machine, plan_id, user))


# Klucze powiązane z INSERT INTO notifications — wyłączalne w panelu admina
NOTIFICATION_EVENT_LABELS = {
    "CALL_MANAGER": "Wezwanie kierownika",
    "CALL_MIXING": "Wezwanie operatora mieszalni",
    "CALL_PREPRESS": "Wezwanie prepress",
    "PROBLEM_REPORT": "Zgłoszenie problemu na maszynie",
    "REPORT_PRINT_CALLED": "Wywołanie raportu zadruku (powiadomienie do kierownika)",
    "REPORT_PRODUCTION_CALLED": "Wywołanie raportu produkcji (powiadomienie do kierownika)",
    "JOB_STARTED": "Rozpoczęcie zlecenia (powiadomienie do kierownika)",
    "JOB_COMPLETED": "Zakończenie zlecenia (powiadomienie do kierownika)",
    "ASSORTMENT_CONFIRMED": "Zatwierdzenie asortymentu (powiadomienie do kierownika)",
    "RAPORT_ZADRUKU_ZAPISANY": "Zapisany raport kontroli zadruku (powiadomienie)",
    "RAPORT_PRODUKCJI_ZAPISANY": "Zapisany raport produkcji (powiadomienie)",
}


def is_notification_enabled(cur, event_key: str) -> bool:
    cur.execute("SELECT enabled FROM notification_settings WHERE event_key=?", (event_key,))
    row = cur.fetchone()
    if row is None:
        return True
    return bool(row["enabled"])


def insert_notification_if_enabled(cur, event_key: str, machine, plan_id, message, target_role, created_by):
    if not is_notification_enabled(cur, event_key):
        return
    cur.execute(
        "INSERT INTO notifications (machine, plan_id, message, target_role, created_by) VALUES (?, ?, ?, ?, ?)",
        (machine, plan_id, message, target_role, created_by),
    )


def log_domain_event(cur, event_type: str, actor_user: str, machine=None, plan_id=None, lub_number=None, payload=None):
    cur.execute(
        """INSERT INTO events (event_type, actor_user, machine, plan_id, lub_number, payload, created_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
        (event_type, actor_user, machine, plan_id, lub_number, payload),
    )


def seed_notification_settings_rows(cur):
    for key in NOTIFICATION_EVENT_LABELS:
        cur.execute(
            "INSERT OR IGNORE INTO notification_settings (event_key, enabled) VALUES (?, 1)",
            (key,),
        )


def normalize_shift_label(shift_val: str) -> str:
    if not shift_val:
        return "dzien"
    s = str(shift_val).lower().strip()
    if s in ("1", "dzien", "dzień", "day"):
        return "dzien"
    if s in ("noc", "night", "2"):
        return "noc"
    return s


def resolve_plan_id_for_job(cur, machine: str, job_number: str):
    cur.execute(
        """SELECT id FROM production_plans
           WHERE order_number=? AND machine=? AND status IN ('planned','in_progress')
           ORDER BY id DESC LIMIT 1""",
        (job_number, machine.upper()),
    )
    row = cur.fetchone()
    return row["id"] if row else None


def enrich_plans_with_lub_materials(cur, plan_rows):
    """Do każdego planu dokleja listy farb i polimerów wg lub_number (dla widoku operator/prepress)."""
    if not plan_rows:
        return []
    from collections import defaultdict
    lubs = {p["lub_number"] for p in plan_rows if p["lub_number"]}
    farby_by_lub = defaultdict(list)
    pol_by_lub = defaultdict(list)
    if lubs:
        placeholders = ",".join("?" * len(lubs))
        t = tuple(lubs)
        cur.execute("SELECT * FROM farby WHERE lub IN (%s)" % placeholders, t)
        for f in cur.fetchall():
            fd = dict(f)
            fd["mag_alert"] = alert_daty(f["data_produkcji"]) if f["data_produkcji"] else "ok"
            farby_by_lub[f["lub"]].append(fd)
        cur.execute("SELECT * FROM polymers WHERE lub IN (%s)" % placeholders, t)
        for po in cur.fetchall():
            pol_by_lub[po["lub"]].append(po)
    out = []
    for p in plan_rows:
        d = dict(p)
        lub = d.get("lub_number")
        d["farby"] = farby_by_lub.get(lub, []) if lub else []
        d["polimery"] = pol_by_lub.get(lub, []) if lub else []
        st = d.get("assortment_prep_status")
        if not st:
            d["assortment_prep_status"] = "pending"
        out.append(d)
    return out


def alert_daty(data_val):
    """Akceptuje str (SQLite) lub date/datetime (PostgreSQL / sterowniki DB)."""
    if not data_val:
        return "ok"
    if isinstance(data_val, datetime):
        d = data_val.date()
    elif isinstance(data_val, date):
        d = data_val
    else:
        d = datetime.strptime(str(data_val).strip()[:10], "%Y-%m-%d").date()
    dni = (date.today() - d).days
    if dni > 365:
        return "przeterminowana"
    elif dni > 275:
        return "uwaga"
    else:
        return "ok"

def build_redirect_url(request: Request, extra_params=None):
    params = dict(request.query_params)
    if extra_params:
        params.update(extra_params)
    params = {k: v for k, v in params.items() if v}
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"/magazyn?{query}" if query else "/magazyn"

# Maszyny produkcyjne (spójne przenoszenie zleceń i walidacja URL)
PRODUCTION_MACHINES = ("D6", "D8", "D10")


def migrate_schema(cur):
    """Dodaje brakujące kolumny w starszych plikach database.db (CREATE IF NOT EXISTS ich nie uzupełnia)."""
    if is_postgres():
        from db_compat import migrate_schema_postgres

        migrate_schema_postgres(cur)
        return
    migrations = {
        "operacje": [("farba_id", "INTEGER")],
        "production_reports": [("plan_id", "INTEGER")],
        "print_control_reports": [("plan_id", "INTEGER")],
        "production_plans": [("assortment_prep_status", "TEXT")],
    }
    for table, columns in migrations.items():
        cur.execute("PRAGMA table_info(%s)" % (table,))
        existing = {row[1] for row in cur.fetchall()}
        for col_name, col_type in columns:
            if col_name not in existing:
                cur.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, col_name, col_type))


# ----------------- NOWE: INICJALIZACJA BAZY (dodanie tabel dla polimerów) -----------------
def init_db():
    conn = get_db()
    cur = conn.cursor()
    if is_postgres():
        from db_compat import init_postgres_schema, seed_default_users_postgres

        init_postgres_schema(cur)
        migrate_schema(cur)
        seed_notification_settings_rows(cur)
        seed_default_users_postgres(cur)
        cur.execute("SELECT COUNT(*) FROM shifts")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("dzień", "06:00", "18:00"))
            cur.execute("INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("noc", "18:00", "06:00"))
        conn.commit()
        conn.close()
        return

    # Tabela farb (SQLite)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS farby (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pantone TEXT NOT NULL,
            lub TEXT,
            polka TEXT,
            waga REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'dostepna',  -- dostepna, w_uzyciu, zutylizowana
            data_produkcji DATE NOT NULL
        )
    """)
    # Tabela operacji na farbach
    cur.execute("""
        CREATE TABLE IF NOT EXISTS operacje (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            typ TEXT NOT NULL,  -- przyjecie, wydanie, zwrot, utylizacja
            farba TEXT NOT NULL,
            ilosc TEXT NOT NULL,
            polka TEXT,
            uwagi TEXT,
            farba_id INTEGER
        )
    """)
    # Tabela polimerów (matryc)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS polymers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lub TEXT NOT NULL,                -- numer LUB pracy, do której należy polimer
            kolor TEXT NOT NULL,              -- nazwa koloru lub numer Pantone
            status TEXT NOT NULL DEFAULT 'dostepna',  -- dostepna, w_uzyciu, uszkodzona, zutylizowana
            lokalizacja TEXT,
            data_waznosci DATE,
            uwagi TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Tabela operacji na polimerach
    cur.execute("""
        CREATE TABLE IF NOT EXISTS polymer_operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            typ TEXT NOT NULL,
            polymer_id INTEGER NOT NULL,
            lokalizacja TEXT,
            uwagi TEXT,
            FOREIGN KEY (polymer_id) REFERENCES polymers(id) ON DELETE CASCADE
        )
    """)
    # Tabela planów produkcji
    cur.execute("""
        CREATE TABLE IF NOT EXISTS production_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT NOT NULL,
            order_number TEXT NOT NULL,
            artwork_number TEXT,
            lub_number TEXT,
            order_name TEXT,
            laminate TEXT,
            meters INTEGER,
            pieces INTEGER,
            planned_date DATE,
            status TEXT DEFAULT 'planned',
            assortment_prep_status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Tabela raportów produkcji maszyn
    cur.execute("""
        CREATE TABLE IF NOT EXISTS production_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT NOT NULL,
            date DATE NOT NULL,
            shift TEXT NOT NULL,  -- dzien, noc
            job_number TEXT NOT NULL,
            start_time TIME,
            end_time TIME,
            quantity INTEGER NOT NULL,
            ok_quantity INTEGER NOT NULL,
            nok_quantity INTEGER NOT NULL,
            notes TEXT,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan_id INTEGER
        )
    """)
    # Tabela raportów kontroli druku
    cur.execute("""
        CREATE TABLE IF NOT EXISTS print_control_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            job_number TEXT NOT NULL,
            status TEXT NOT NULL,
            notes TEXT,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan_id INTEGER
        )
    """)
    # Tabela zmian produkcyjnych
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,  -- dzien, noc
            start_time TIME NOT NULL,
            end_time TIME NOT NULL
        )
    """)

    # Tabela powiadomień / komunikator
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT,
            plan_id INTEGER,
            message TEXT NOT NULL,
            target_role TEXT NOT NULL,
            target_user TEXT,
            created_by TEXT NOT NULL,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Tabela dziennika zmian produkcyjnych
    cur.execute("""
        CREATE TABLE IF NOT EXISTS production_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_type TEXT NOT NULL,  -- rozpoczecie, zakonczenie, pauza, wznowienie, etc.
            description TEXT NOT NULL,
            machine TEXT,
            plan_id INTEGER,
            user TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Zdarzenia domenowe (audyt, integracje)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT NOT NULL,
            actor_user TEXT NOT NULL,
            machine TEXT,
            plan_id INTEGER,
            lub_number TEXT,
            payload TEXT
        )
    """)

    # Włączanie/wyłączanie typów powiadomień (klucz = NOTIFICATION_EVENT_LABELS)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notification_settings (
            event_key TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1
        )
    """)

    migrate_schema(cur)
    seed_notification_settings_rows(cur)

    # Wstaw domyślne zmiany jeśli nie istnieją
    cur.execute("SELECT COUNT(*) FROM shifts")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("dzień", "06:00", "18:00"))
        cur.execute("INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("noc", "18:00", "06:00"))
    conn.commit()
    conn.close()

# Wywołanie inicjalizacji
init_db()

# ----------------- LOGOWANIE -----------------
@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request):
    logo_path = "/static/logo_duze.svg"
    custom_logo_path = os.path.join(get_base_path(), "static", "logo_custom.png")
    if os.path.exists(custom_logo_path):
        logo_path = "/static/logo_custom.png"
    return render_template("login.html", {"operators": [], "logo_url": logo_path})

@app.post("/login")
def login(
    request: Request,
    username: str = Form(None),
    password: str = Form(None)
):
    conn = get_db()
    cur = conn.cursor()
    user = None
    if username and password:
        cur.execute("SELECT * FROM users WHERE username=?", (username,))
        db_user = cur.fetchone()
        if db_user and db_user["password"] == password:
            user = db_user
    if user:
        request.session["username"] = user["username"]
        request.session["role"] = user["role"]
        if user["role"] == "drukarz":
            return RedirectResponse("/select-machine", status_code=303)
        return RedirectResponse("/dashboard", status_code=303)
    else:
        cur.execute("SELECT username FROM users WHERE role != 'admin' ORDER BY username")
        operators = cur.fetchall()
        logo_path = "/static/logo_duze.svg"
        custom_logo_path = os.path.join(get_base_path(), "static", "logo_custom.png")
        if os.path.exists(custom_logo_path):
            logo_path = "/static/logo_custom.png"
        return render_template("login.html", {"operators": operators, "error": "Nieprawidłowe dane logowania", "logo_url": logo_path})

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and not request.session.get("machine"):
        return RedirectResponse("/select-machine", status_code=303)
    return render_template("dashboard.html", {
        "user": {"username": user["username"], "role": user["role"]}
    })

# Redirect root to dashboard
@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=303)
    return RedirectResponse("/login", status_code=303)

# ----------------- ADMIN PANEL -----------------
@app.get("/admin", response_class=HTMLResponse)
def admin_panel(request: Request, admin=Depends(require_admin), error: str = Query(None), success: str = Query(None)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role FROM users ORDER BY role DESC, username")
    users = cur.fetchall()
    cur.execute("SELECT name, start_time, end_time FROM shifts ORDER BY start_time")
    shifts = cur.fetchall()
    cur.execute("SELECT event_key, enabled FROM notification_settings")
    settings_map = {r["event_key"]: bool(r["enabled"]) for r in cur.fetchall()}
    notification_prefs = [
        {"key": k, "label": v, "enabled": settings_map.get(k, True)}
        for k, v in NOTIFICATION_EVENT_LABELS.items()
    ]
    context = {
        "users": users,
        "shifts": shifts,
        "notification_prefs": notification_prefs,
        "user": {"username": request.session.get("username"), "role": request.session.get("role")}
    }
    if error:
        context["error"] = error
    if success:
        context["success"] = success
    return render_template("admin_panel.html", context)

@app.post("/admin/add_operator")
def add_operator(request: Request, username: str = Form(...), admin=Depends(require_admin)):
    return RedirectResponse("/admin?error=U%C5%BCyj+formularza+dodawania+u%C5%BCytkownika", status_code=303)

@app.post("/admin/add_user")
def add_user(
    request: Request,
    username: str = Form(...),
    role: str = Form(...),
    password: str = Form(""),
    confirm_password: str = Form(""),
    admin=Depends(require_admin)
):
    if role not in ["drukarz", "operator_mieszalni", "prepress", "manager", "admin"]:
        return RedirectResponse("/admin?error=Nieprawid%C5%82owa+rola", status_code=303)

    if not password or not confirm_password or password != confirm_password:
        return RedirectResponse("/admin?error=Has%C5%82a+niezgodne+lub+puste", status_code=303)

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute("INSERT INTO users (username, role, password) VALUES (?, ?, ?)",
                    (username, role, password))
        conn.commit()
    except INTEGRITY_ERRORS:
        return RedirectResponse("/admin?error=U%C5%BCytkownik+ju%C5%BC+istnieje", status_code=303)

    return RedirectResponse("/admin?success=U%C5%BCytkownik+dodany", status_code=303)

@app.post("/admin/notification-settings")
async def admin_notification_settings(request: Request, admin=Depends(require_admin)):
    form = await request.form()
    conn = get_db()
    cur = conn.cursor()
    for key in NOTIFICATION_EVENT_LABELS:
        enabled = 1 if form.get(key) == "on" else 0
        cur.execute("UPDATE notification_settings SET enabled=? WHERE event_key=?", (enabled, key))
    conn.commit()
    return RedirectResponse("/admin?success=Zapisano+ustawienia+powiadomie%C5%84", status_code=303)


@app.post("/admin/delete_user")
def delete_user(request: Request, user_id: int = Form(...), admin=Depends(require_admin)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT role FROM users WHERE id=?", (user_id,))
    user = cur.fetchone()
    if user and user["role"] != "admin":
        cur.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()
    return RedirectResponse("/admin", status_code=303)

@app.post("/admin/change_password")
def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    admin=Depends(require_admin)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT password FROM users WHERE username=? AND role='admin'", (admin["username"],))
    db_user = cur.fetchone()
    if db_user and db_user["password"] == current_password:
        cur.execute("UPDATE users SET password=? WHERE username=? AND role='admin'", (new_password, admin["username"]))
        conn.commit()
        # Możesz dodać komunikat sukcesu
    return RedirectResponse("/admin", status_code=303)

@app.post("/admin/import_farby")
def import_farby(
    request: Request,
    plik: UploadFile = File(...),
    admin=Depends(require_admin)
):
    try:
        content = plik.file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))
        conn = get_db(); cur = conn.cursor()
        count = 0
        for row in reader:
            pantone = row.get('pantone') or row.get('pantone'.upper()) or row.get('PANTONE')
            lub = row.get('lub', '')
            polka = row.get('polka', '')
            waga = row.get('waga')
            data_produkcji = row.get('data_produkcji') or row.get('data_produkcji'.replace('_', ''))
            if not pantone or not waga or not data_produkcji:
                continue
            try:
                waga_val = float(waga)
            except ValueError:
                continue
            cur.execute("INSERT INTO farby (pantone, lub, polka, waga, status, data_produkcji) VALUES (?, ?, ?, ?, 'dostepna', ?)",
                        (pantone, lub, polka, waga_val, data_produkcji))
            dodaj_operacje(cur, 'przyjęcie', pantone, str(waga_val), polka, "", cur.lastrowid)
            count += 1
        conn.commit(); conn.close()
        return RedirectResponse(f"/admin?success=Zaimportowano+{count}+farb", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/admin?error=Błąd+importu+farb:+{str(e)}", status_code=303)

@app.post("/admin/import_polimery")
def import_polimery(
    request: Request,
    plik: UploadFile = File(...),
    admin=Depends(require_admin)
):
    try:
        content = plik.file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))
        conn = get_db(); cur = conn.cursor()
        count = 0
        for row in reader:
            lub = row.get('lub') or row.get('LUB')
            kolor = row.get('kolor') or row.get('Kolor')
            lokalizacja = row.get('lokalizacja', '')
            data_waznosci = row.get('data_waznosci') or row.get('data_waznosci'.replace('_', ''))
            uwagi = row.get('uwagi', '')
            if not lub or not kolor:
                continue
            cur.execute("INSERT INTO polymers (lub, kolor, status, lokalizacja, data_waznosci, uwagi) VALUES (?, ?, 'dostepna', ?, ?, ?)",
                        (lub, kolor, lokalizacja, data_waznosci, uwagi))
            new_id = cur.lastrowid
            cur.execute("INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('przyjęcie', ?, ?, ?)",
                        (new_id, lokalizacja, f"Import CSV: LUB={lub}, kolor={kolor}"))
            count += 1
        conn.commit(); conn.close()
        return RedirectResponse(f"/admin?success=Zaimportowano+{count}+polimerów", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/admin?error=Błąd+importu+polimerów:+{str(e)}", status_code=303)

@app.post("/admin/import_plany")
def import_plany(
    request: Request,
    plik: UploadFile = File(...),
    admin=Depends(require_admin)
):
    try:
        content = plik.file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))
        conn = get_db(); cur = conn.cursor()
        count = 0
        for row in reader:
            machine = (row.get('machine') or row.get('maszyna') or '').upper()
            order_number = row.get('order_number') or row.get('numer_zlecenia')
            artwork_number = row.get('artwork_number') or row.get('numer_artwork')
            lub_number = row.get('lub_number') or row.get('numer_lub')
            order_name = row.get('order_name') or row.get('nazwa_zlecenia')
            laminate = row.get('laminate') or row.get('laminat')
            meters = row.get('meters') or row.get('ilosc_metrow') or 0
            pieces = row.get('pieces') or row.get('ilosc_sztuk') or 0
            planned_date = row.get('planned_date') or row.get('data_planowana')
            if machine not in PRODUCTION_MACHINES or not order_number:
                continue
            try:
                meters_val = int(meters)
                pieces_val = int(pieces)
            except ValueError:
                continue
            cur.execute(
                """INSERT INTO production_plans (machine, order_number, artwork_number, lub_number, order_name, laminate, meters, pieces, planned_date, status, assortment_prep_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', 'pending')""",
                (machine, order_number, artwork_number, lub_number, order_name, laminate, meters_val, pieces_val, planned_date),
            )
            count += 1
        conn.commit(); conn.close()
        return RedirectResponse(f"/admin?success=Zaimportowano+{count}+planów", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/admin?error=Błąd+importu+planów:+{str(e)}", status_code=303)

# ==================== PANELE MASZYN I KIEROWNIKA ====================
@app.get("/maszyny", response_class=HTMLResponse)
def maszyny(request: Request, user=Depends(require_auth)):
    if user["role"] not in ["admin", "manager", "drukarz", "operator_mieszalni", "prepress"]:
        return RedirectResponse("/dashboard", status_code=303)
    assigned_machine = request.session.get("machine") if user["role"] == "drukarz" else None
    return render_template("maszyny.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "assigned_machine": assigned_machine
    })

@app.get("/plany", response_class=HTMLResponse)
def plany(request: Request, user=Depends(require_auth)):
    # Trasa głównie dla prepress/operator_mieszalni, ale dopuszczamy również drukarza,
    # aby uniknąć routa Not Found w przypadku wejścia z linka.
    if user["role"] not in ["operator_mieszalni", "prepress", "drukarz", "manager", "admin"]:
        return RedirectResponse("/dashboard", status_code=303)
    machines = list(PRODUCTION_MACHINES)
    conn = get_db()
    cur = conn.cursor()
    machine_prep = {}
    for m in machines:
        cur.execute(
            """
            SELECT COUNT(*) AS c,
                   SUM(CASE WHEN COALESCE(assortment_prep_status,'')='ready' THEN 1 ELSE 0 END) AS r
            FROM production_plans WHERE machine=? AND status='planned'
            """,
            (m,),
        )
        row = cur.fetchone()
        tot = row["c"] or 0
        rdy = min(row["r"] or 0, tot)
        machine_prep[m] = {"total": tot, "ready": rdy}
    conn.close()
    return render_template("plany_machines.html", {
        "machines": machines,
        "machine_prep": machine_prep,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/select-machine", response_class=HTMLResponse)
def select_machine_form(request: Request, user=Depends(require_auth)):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    return render_template("select_machine.html", {"user": user})

@app.post("/select-machine")
def select_machine(request: Request, machine: str = Form(...), user=Depends(require_auth)):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    if machine.upper() not in PRODUCTION_MACHINES:
        return RedirectResponse("/select-machine", status_code=303)
    request.session["machine"] = machine.upper()
    return RedirectResponse(f"/maszyna/{machine.lower()}/plany", status_code=303)

@app.get("/notifications", response_class=HTMLResponse)
def notifications_view(request: Request, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    base = """
        SELECT n.id, n.machine, n.plan_id, n.message, n.target_role, n.target_user,
               n.created_by, n.is_read, n.created_at,
               p.order_number AS plan_order_number, p.lub_number AS plan_lub_number
        FROM notifications n
        LEFT JOIN production_plans p ON n.plan_id = p.id
    """
    if user["role"] == "admin":
        cur.execute(base + " ORDER BY n.created_at DESC")
    else:
        cur.execute(
            base + " WHERE n.target_role=? OR n.target_user=? ORDER BY n.created_at DESC",
            (user["role"], user["username"]),
        )
    notifications = cur.fetchall()
    return render_template("notifications.html", {"notifications": notifications})

@app.post("/mark_notification_read/{notification_id}")
def mark_notification_read(notification_id: int, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    if user["role"] == "admin":
        cur.execute("UPDATE notifications SET is_read=1 WHERE id=?", (notification_id,))
    else:
        cur.execute(
            "UPDATE notifications SET is_read=1 WHERE id=? AND (target_role=? OR target_user=?)",
            (notification_id, user["role"], user["username"]),
        )
    conn.commit()
    return JSONResponse({"success": True})

def _notification_select_sql(with_role_filter: bool) -> str:
    q = """
        SELECT n.id, n.machine, n.plan_id, n.message, n.target_role, n.target_user,
               n.created_by, n.is_read, n.created_at,
               p.order_number AS plan_order_number, p.lub_number AS plan_lub_number
        FROM notifications n
        LEFT JOIN production_plans p ON n.plan_id = p.id
        WHERE n.is_read=0
    """
    if with_role_filter:
        q += " AND (n.target_role=? OR n.target_user=?)"
    q += " ORDER BY n.created_at DESC LIMIT 1"
    return q


@app.get("/api/notifications/new")
def get_new_notifications(user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    if user["role"] == "admin":
        cur.execute(_notification_select_sql(False))
    else:
        cur.execute(
            _notification_select_sql(True),
            (user["role"], user["username"]),
        )
    notifications = cur.fetchall()
    return JSONResponse({"notifications": [dict(n) for n in notifications]})

@app.get("/kierownik", response_class=HTMLResponse)
def kierownik(request: Request, user=Depends(require_manager_or_admin)):
    return render_template("kierownik.html", {
        "user": {"username": user["username"], "role": user["role"]}
    })

# ==================== MAGAZYN (WIDOK GŁÓWNY) ====================
@app.get("/magazyn", response_class=HTMLResponse)
def magazyn(
    request: Request,
    search_field: str = Query("lub"),
    search_value: str = Query(""),
    filtr_alert: str = Query(""),
    status: str = Query(""),
    sort: str = Query("status"),
    dir: str = Query("asc"),
    error: str = Query(""),
    success: str = Query(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby")
    dane = cur.fetchall()

    farby = []
    licznik_przeterminowane = 0
    licznik_uwaga = 0
    licznik_zutylizowane = 0

    for f in dane:
        alert = alert_daty(f["data_produkcji"])
        if f["status"] != "zutylizowana":
            if alert == "przeterminowana":
                licznik_przeterminowane += 1
            elif alert == "uwaga":
                licznik_uwaga += 1
        else:
            licznik_zutylizowane += 1

        if filtr_alert and alert != filtr_alert:
            continue
        if status and f["status"] != status:
            continue
        if search_value:
            if search_field == "lub" and search_value.lower() not in (f["lub"] or "").lower():
                continue
            elif search_field == "pantone" and search_value.lower() not in f["pantone"].lower():
                continue
            elif search_field == "polka" and search_value.lower() not in (f["polka"] or "").lower():
                continue

        farby.append({**dict(f), "alert": alert})

    reverse = (dir == "desc")
    if sort == "status":
        order = {"dostepna": 1, "w_uzyciu": 2, "zutylizowana": 3}
        farby.sort(key=lambda x: order.get(x["status"], 9), reverse=reverse)
    else:
        farby.sort(key=lambda x: (x.get(sort) or ""), reverse=reverse)

    return render_template("magazyn.html", {
        "farby": farby,
        "licznik_przeterminowane": licznik_przeterminowane,
        "licznik_uwaga": licznik_uwaga,
        "licznik_zutylizowane": licznik_zutylizowane,
        "search_field": search_field,
        "search_value": search_value,
        "filtr_alert": filtr_alert,
        "status": status,
        "sort": sort,
        "dir": dir,
        "user": {"username": user["username"], "role": user["role"]},
        "error": error,
        "success": success
    })

# ----------------- AKCJE POJEDYNCZE (AJAX) -----------------
@app.post("/dodaj_farba")
def dodaj_farba(
    request: Request,
    pantone: str = Form(...),
    lub: str = Form(""),
    polka: str = Form(...),
    data_produkcji: str = Form(...),
    waga: float = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO farby (pantone, lub, polka, waga, status, data_produkcji)
        VALUES (?, ?, ?, ?, 'dostepna', ?)
    """, (pantone, lub, polka, waga, data_produkcji))
    dodaj_operacje(cur, "przyjęcie", pantone, str(waga), polka, "", cur.lastrowid)
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Dodano farbę"})
    return RedirectResponse(build_redirect_url(request, {"success": "dodano"}), status_code=303)

@app.post("/pobierz")
def pobierz(
    request: Request,
    id: int = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse(build_redirect_url(request, {"error": "notfound"}), status_code=303)
    if f["status"] != "dostepna":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "niedostepna"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "niedostepna"}), status_code=303)
    if alert_daty(f["data_produkcji"]) == "przeterminowana":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "przeterminowana"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "przeterminowana"}), status_code=303)
    cur.execute("UPDATE farby SET status='w_uzyciu' WHERE id=?", (id,))
    dodaj_operacje(cur, "wydanie", f["pantone"], str(f["waga"]), f["polka"], "", f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Pobrano farbę", "new_status": "w_uzyciu"})
    return RedirectResponse(build_redirect_url(request, {"success": "pobrano"}), status_code=303)

@app.post("/zwrot")
def zwrot(
    request: Request,
    id: int = Form(...),
    waga: float = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse(build_redirect_url(request, {"error": "notfound"}), status_code=303)
    if f["status"] != "w_uzyciu":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "nie_w_uzyciu"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "nie_w_uzyciu"}), status_code=303)
    cur.execute("UPDATE farby SET status='dostepna', waga=? WHERE id=?", (waga, id))
    dodaj_operacje(cur, "zwrot", f["pantone"], str(waga), f["polka"], "", f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zwrócono farbę", "new_status": "dostepna", "new_waga": waga})
    return RedirectResponse(build_redirect_url(request, {"success": "zwrocono"}), status_code=303)

@app.post("/utylizacja")
def utylizacja(
    request: Request,
    id: int = Form(...),
    powod: str = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse(build_redirect_url(request, {"error": "notfound"}), status_code=303)
    cur.execute("UPDATE farby SET status='zutylizowana', waga=0 WHERE id=?", (id,))
    dodaj_operacje(cur, "utylizacja", f["pantone"], str(f["waga"]), f["polka"], powod, f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zutylizowano farbę", "new_status": "zutylizowana"})
    return RedirectResponse(build_redirect_url(request, {"success": "utylizowano"}), status_code=303)

@app.post("/przywroc")
def przywroc(
    request: Request,
    id: int = Form(...),
    nowa_data: str = Form(...),
    nowa_waga: float = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f or f["status"] != "zutylizowana":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "blad_przywracania"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "blad_przywracania"}), status_code=303)
    cur.execute("""
        UPDATE farby
        SET status='dostepna', data_produkcji=?, waga=?
        WHERE id=?
    """, (nowa_data, nowa_waga, id))
    dodaj_operacje(cur, "przywrócenie", f["pantone"], str(nowa_waga), f["polka"], f"nowa data: {nowa_data}", f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({
            "success": True,
            "message": "Przywrócono farbę",
            "new_status": "dostepna",
            "new_data": nowa_data,
            "new_waga": nowa_waga
        })
    return RedirectResponse(build_redirect_url(request, {"success": "przywrocono"}), status_code=303)

# ----------------- AKCJE ZBIORCZE (tylko pobierz wszystkie) -----------------
@app.post("/pobierz_wszystkie")
def pobierz_wszystkie(
    request: Request,
    search_field: str = Form(...),
    search_value: str = Form(...),
    filtr_alert: str = Form(""),
    status: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby")
    dane = cur.fetchall()
    liczba = 0
    for f in dane:
        alert = alert_daty(f["data_produkcji"])
        if filtr_alert and alert != filtr_alert:
            continue
        if status and f["status"] != status:
            continue
        if search_value:
            if search_field == "lub" and search_value.lower() not in (f["lub"] or "").lower():
                continue
            elif search_field == "pantone" and search_value.lower() not in f["pantone"].lower():
                continue
            elif search_field == "polka" and search_value.lower() not in (f["polka"] or "").lower():
                continue
        if f["status"] == "dostepna" and alert != "przeterminowana":
            cur.execute("UPDATE farby SET status='w_uzyciu' WHERE id=?", (f["id"],))
            dodaj_operacje(cur, "wydanie", f["pantone"], str(f["waga"]), f["polka"], "", f["id"])
            liczba += 1
    conn.commit()
    return RedirectResponse(build_redirect_url(request, {"success": f"pobrano_{liczba}"}), status_code=303)

# ----------------- POZOSTAŁE ENDPOINTY -----------------
@app.get("/get_row/{id}", response_class=HTMLResponse)
def get_row(
    id: int,
    request: Request,
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        return HTMLResponse(".<td colspan='7'>Błąd: farba nie istnieje</td>", status_code=404)
    alert = alert_daty(f["data_produkcji"])
    f_dict = dict(f)
    f_dict["alert"] = alert
    return render_template("row.html", {"f": f_dict})

@app.get("/historia", response_class=HTMLResponse)
def historia(request: Request, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM operacje ORDER BY id DESC")
    operacje = cur.fetchall()
    return render_template("historia.html", {"operacje": operacje, "user": {"username": user["username"], "role": user["role"]}})

@app.get("/statystyki", response_class=HTMLResponse)
def statystyki(request: Request, user=Depends(require_manager_or_admin)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT farba, COUNT(*) as ile
        FROM operacje
        WHERE typ='wydanie'
        GROUP BY farba
        ORDER BY ile DESC
        LIMIT 10
    """)
    top_farby = cur.fetchall()
    labels = [row["farba"] for row in top_farby]
    values = [row["ile"] for row in top_farby]
    return render_template("statystyki.html", {
        "top_farby": top_farby,
        "labels": labels,
        "values": values,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/raport_utylizacji", response_class=HTMLResponse)
def raport_utylizacji(
    request: Request,
    od: str = Query(""),
    do: str = Query(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    if od and do:
        cur.execute("""
            SELECT farba, ilosc, data, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od, do))
    else:
        do_dom = datetime.now().strftime("%Y-%m-%d")
        od_dom = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT farba, ilosc, data, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od_dom, do_dom))
        od, do = od_dom, do_dom
    utylizacje = cur.fetchall()
    return render_template("raport_utylizacji.html", {
        "utylizacje": utylizacje,
        "od": od,
        "do": do,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/export_raport_utylizacji")
def export_raport_utylizacji(
    od: str = Query(""),
    do: str = Query(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    if od and do:
        cur.execute("""
            SELECT data, farba, ilosc, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od, do))
    else:
        do_dom = datetime.now().strftime("%Y-%m-%d")
        od_dom = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT data, farba, ilosc, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od_dom, do_dom))
    rows = cur.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Data", "Farba", "Ilość (kg)", "Powód"])
    for row in rows:
        writer.writerow([row["data"], row["farba"], row["ilosc"], row["uwagi"]])
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=raport_utylizacji.csv"
    })

@app.get("/export")
def export_csv(user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM operacje")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["data", "typ", "farba", "ilosc", "polka", "uwagi"])
    for row in cur.fetchall():
        writer.writerow([row["data"], row["typ"], row["farba"], row["ilosc"], row["polka"], row["uwagi"]])
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=logi.csv"
    })

# ----------------- NOWE: ENDPOINTY DLA POLIMERÓW (MATRYC) -----------------
@app.get("/polimery", response_class=HTMLResponse)
def polimery(
    request: Request,
    search_field: str = Query("lub"),
    search_value: str = Query(""),
    status: str = Query(""),
    sort: str = Query("status"),
    dir: str = Query("asc"),
    error: str = Query(""),
    success: str = Query(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers")
    dane = cur.fetchall()

    polimery = []
    for p in dane:
        if search_value:
            if search_field == "lub" and search_value.lower() not in (p["lub"] or "").lower():
                continue
            elif search_field == "kolor" and search_value.lower() not in (p["kolor"] or "").lower():
                continue
            elif search_field == "lokalizacja" and search_value.lower() not in (p["lokalizacja"] or "").lower():
                continue
        if status and p["status"] != status:
            continue
        polimery.append(dict(p))

    reverse = (dir == "desc")
    if sort == "status":
        order = {"dostepna": 1, "w_uzyciu": 2, "uszkodzona": 3, "zutylizowana": 4}
        polimery.sort(key=lambda x: order.get(x["status"], 9), reverse=reverse)
    else:
        polimery.sort(key=lambda x: (x.get(sort) or ""), reverse=reverse)

    return render_template("polimery.html", {
        "polimery": polimery,
        "search_field": search_field,
        "search_value": search_value,
        "status": status,
        "sort": sort,
        "dir": dir,
        "user": {"username": user["username"], "role": user["role"]},
        "error": error,
        "success": success
    })

@app.post("/dodaj_polimer")
def dodaj_polimer(
    request: Request,
    lub: str = Form(...),
    kolor: str = Form(...),
    lokalizacja: str = Form(""),
    data_waznosci: str = Form(""),
    uwagi: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO polymers (lub, kolor, status, lokalizacja, data_waznosci, uwagi)
            VALUES (?, ?, 'dostepna', ?, ?, ?)
        """, (lub, kolor, lokalizacja, data_waznosci if data_waznosci else None, uwagi))
        conn.commit()
        new_id = cur.lastrowid
        # Dodaj operację "przyjęcie"
        cur.execute("""
            INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi)
            VALUES ('przyjęcie', ?, ?, ?)
        """, (new_id, lokalizacja, f"Dodano: LUB={lub}, kolor={kolor}"))
        conn.commit()
    except INTEGRITY_ERRORS:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "Błąd bazy danych"}, status_code=400)
        return RedirectResponse("/polimery?error=duplikat", status_code=303)

    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Dodano polimer"})
    return RedirectResponse("/polimery?success=dodano", status_code=303)

@app.post("/pobierz_polimer")
def pobierz_polimer(
    request: Request,
    id: int = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] != "dostepna":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "niedostepna"}, status_code=400)
        return RedirectResponse("/polimery?error=niedostepna", status_code=303)
    cur.execute("UPDATE polymers SET status='w_uzyciu' WHERE id=?", (id,))
    cur.execute("""
        INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi)
        VALUES ('pobranie', ?, ?, ?)
    """, (id, p["lokalizacja"], f"Pobrano: {p['lub']} / {p['kolor']}"))
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Pobrano polimer", "new_status": "w_uzyciu"})
    return RedirectResponse("/polimery?success=pobrano", status_code=303)

@app.post("/zwroc_polimer")
def zwroc_polimer(
    request: Request,
    id: int = Form(...),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] != "w_uzyciu":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "nie_w_uzyciu"}, status_code=400)
        return RedirectResponse("/polimery?error=nie_w_uzyciu", status_code=303)
    cur.execute("UPDATE polymers SET status='dostepna' WHERE id=?", (id,))
    cur.execute("""
        INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi)
        VALUES ('zwrot', ?, ?, ?)
    """, (id, p["lokalizacja"], f"Zwrot: {p['lub']} / {p['kolor']}"))
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zwrócono polimer", "new_status": "dostepna"})
    return RedirectResponse("/polimery?success=zwrocono", status_code=303)

@app.post("/uszkodz_polimer")
def uszkodz_polimer(
    request: Request,
    id: int = Form(...),
    powod: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] not in ["dostepna", "w_uzyciu"]:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "nie_mozna_uszkodzic"}, status_code=400)
        return RedirectResponse("/polimery?error=nie_mozna_uszkodzic", status_code=303)
    cur.execute("UPDATE polymers SET status='uszkodzona' WHERE id=?", (id,))
    cur.execute("""
        INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi)
        VALUES ('uszkodzenie', ?, ?, ?)
    """, (id, p["lokalizacja"], f"Uszkodzenie: {powod}" if powod else "Uszkodzenie"))
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Oznaczono jako uszkodzony", "new_status": "uszkodzona"})
    return RedirectResponse("/polimery?success=uszkodzono", status_code=303)

@app.post("/utylizuj_polimer")
def utylizuj_polimer(
    request: Request,
    id: int = Form(...),
    powod: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] == "zutylizowana":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "juz_zutylizowana"}, status_code=400)
        return RedirectResponse("/polimery?error=juz_zutylizowana", status_code=303)
    cur.execute("UPDATE polymers SET status='zutylizowana' WHERE id=?", (id,))
    cur.execute("""
        INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi)
        VALUES ('utylizacja', ?, ?, ?)
    """, (id, p["lokalizacja"], f"Utylizacja: {powod}" if powod else "Utylizacja"))
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zutylizowano polimer", "new_status": "zutylizowana"})
    return RedirectResponse("/polimery?success=utylizowano", status_code=303)

@app.post("/przywroc_polimer")
def przywroc_polimer(
    request: Request,
    id: int = Form(...),
    nowa_data_waznosci: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p or p["status"] != "uszkodzona":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "blad_przywracania"}, status_code=400)
        return RedirectResponse("/polimery?error=blad_przywracania", status_code=303)
    if nowa_data_waznosci:
        cur.execute("UPDATE polymers SET status='dostepna', data_waznosci=? WHERE id=?", (nowa_data_waznosci, id))
    else:
        cur.execute("UPDATE polymers SET status='dostepna' WHERE id=?", (id,))
    cur.execute("""
        INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi)
        VALUES ('przywrócenie', ?, ?, ?)
    """, (id, p["lokalizacja"], f"Przywrócono z uszkodzenia" + (f", nowa data ważności: {nowa_data_waznosci}" if nowa_data_waznosci else "")))
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Przywrócono polimer", "new_status": "dostepna"})
    return RedirectResponse("/polimery?success=przywrocono", status_code=303)

@app.get("/get_polimer_row/{id}", response_class=HTMLResponse)
def get_polimer_row(
    id: int,
    request: Request,
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        return HTMLResponse("<td colspan='7'>Błąd: polimer nie istnieje</td>", status_code=404)
    return render_template("polimery_row.html", {"p": dict(p)})

# ==================== ENDPOINTY DLA MASZYN ====================
@app.get("/maszyna/{machine}/plany", response_class=HTMLResponse)
def maszyna_plany(
    machine: str,
    request: Request,
    user=Depends(require_auth),
    success: str = Query(""),
    error: str = Query(""),
):
    if user["role"] not in ["admin", "manager", "drukarz", "operator_mieszalni", "prepress"]:
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT * FROM production_plans
        WHERE machine=? AND status='planned'
        ORDER BY id
    """, (machine.upper(),))
    plan_rows = cur.fetchall()
    plans = enrich_plans_with_lub_materials(cur, plan_rows)

    can_move = user["role"] in ("admin", "manager")
    other_machines = [m for m in PRODUCTION_MACHINES if m != machine.upper()]
    prep_ui = user["role"] in ("operator_mieszalni", "prepress")
    show_prep_column = user["role"] in (
        "drukarz", "manager", "admin", "operator_mieszalni", "prepress",
    )

    return render_template("maszyna_plany.html", {
        "machine": machine.upper(),
        "plans": plans,
        "user": {"username": user["username"], "role": user["role"]},
        "can_move": can_move,
        "other_machines": other_machines,
        "success_msg": success,
        "error_msg": error,
        "prep_ui": prep_ui,
        "show_prep_column": show_prep_column,
    })


@app.post("/maszyna/{machine}/plan/{plan_id}/potwierdz-asortyment")
def potwierdz_asortyment(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
):
    if user["role"] not in ("operator_mieszalni", "prepress"):
        return RedirectResponse("/dashboard", status_code=303)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan or plan["status"] != "planned":
        return RedirectResponse(f"/maszyna/{machine.lower()}/plany?error=brak_zlecenia", status_code=303)
    cur.execute(
        "UPDATE production_plans SET assortment_prep_status='ready' WHERE id=? AND machine=?",
        (plan_id, machine.upper()),
    )
    desc = f"Asortyment zatwierdzony dla {plan['order_number']} ({machine.upper()}) przez {user['username']}"
    log_production_operation(cur, "asortyment_zatwierdzony", desc, machine.upper(), plan_id, user["username"])
    log_domain_event(
        cur,
        "ASSORTMENT_CONFIRMED",
        user["username"],
        machine.upper(),
        plan_id,
        plan["lub_number"],
        None,
    )
    insert_notification_if_enabled(
        cur,
        "ASSORTMENT_CONFIRMED",
        machine.upper(),
        plan_id,
        f"Zatwierdzono asortyment (farby/matryce) dla {plan['order_number']} na {machine.upper()}",
        "manager",
        user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine.lower()}/plany?success=asortyment", status_code=303)


@app.post("/kierownik/przenies-zlecenie")
def kierownik_przenies_zlecenie(
    request: Request,
    plan_id: int = Form(...),
    source_machine: str = Form(...),
    target_machine: str = Form(...),
    user=Depends(require_manager_or_admin),
):
    src = source_machine.strip().upper()
    tgt = target_machine.strip().upper()
    if tgt not in PRODUCTION_MACHINES or src not in PRODUCTION_MACHINES:
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=niewlasciwa_maszyna", status_code=303)
    if src == tgt:
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=ta_sama_maszyna", status_code=303)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, src))
    plan = cur.fetchone()
    if not plan:
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=brak_zlecenia", status_code=303)
    if plan["status"] != "planned":
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=tylko_planowane", status_code=303)
    cur.execute(
        "UPDATE production_plans SET machine=?, assortment_prep_status='pending' WHERE id=?",
        (tgt, plan_id),
    )
    log_domain_event(
        cur,
        "PLAN_MOVED",
        user["username"],
        tgt,
        plan_id,
        plan["lub_number"],
        f"z {src} na {tgt}",
    )
    log_production_operation(
        cur,
        "przeniesienie_zlecenia",
        f"Zlecenie {plan['order_number']} przeniesione z {src} na {tgt}",
        tgt,
        plan_id,
        user["username"],
    )
    conn.commit()
    conn.close()
    return RedirectResponse(f"/maszyna/{tgt.lower()}/plany?success=przeniesiono", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}", response_class=HTMLResponse)
def maszyna_job(machine: str, plan_id: int, request: Request, user=Depends(require_auth), status: str = Query(""), message: str = Query("")):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        print(f"[WARN] Drukarz {user['username']} próbował wejść na maszynę {machine}, ale ma przypisaną {request.session.get('machine')}")
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        print(f"[ERROR] Brak planu: id={plan_id}, machine={machine.upper()}")
        return HTMLResponse("Błąd: Zlecenie nie znalezione lub nie pasuje do maszyny!", status_code=404)

    farby = []
    polimery = []
    if user["role"] in ["operator_mieszalni", "prepress"] and plan["lub_number"]:
        cur.execute("SELECT * FROM farby WHERE lub=?", (plan["lub_number"],))
        farby = cur.fetchall()
        cur.execute("SELECT * FROM polymers WHERE lub=?", (plan["lub_number"],))
        polimery = cur.fetchall()

    return render_template("maszyna_job.html", {
        "machine": machine.upper(),
        "plan": plan,
        "farby": farby,
        "polimery": polimery,
        "user": {"username": user["username"], "role": user["role"]},
        "status": status,
        "message": message
    })

@app.get("/maszyna/{machine}/job/{plan_id}/raport-zadruku")
def maszyna_job_raport_zadruku(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "REPORT_PRINT_CALLED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "REPORT_PRINT_CALLED", machine.upper(), plan_id,
        f"Raport zadruku wywołany na {machine.upper()} dla {plan['order_number']}", "manager", user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Raport+zadruku+wysłany", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/raport-produkcji")
def maszyna_job_raport_produkcji(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "REPORT_PRODUCTION_CALLED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "REPORT_PRODUCTION_CALLED", machine.upper(), plan_id,
        f"Raport produkcji wywołany na {machine.upper()} dla {plan['order_number']}", "manager", user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Raport+produkcji+wysłany", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/problem")
def maszyna_job_problem(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    message = f"Problem zgłoszony na {machine.upper()} dla zlecenia {plan['order_number']}"
    log_domain_event(cur, "PROBLEM_REPORT", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(cur, "PROBLEM_REPORT", machine.upper(), plan_id, message, "manager", user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=warning&message=Problem+zgłoszony", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/call-manager")
def maszyna_call_manager(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "CALL_MANAGER", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_MANAGER", machine.upper(), plan_id,
        f"Wezwanie kierownika na {machine.upper()} dla {plan['order_number']}", "manager", user["username"])
    log_production_operation(cur, "wezwanie", f"Wezwanie kierownika na {machine.upper()} dla {plan['order_number']}", machine.upper(), plan_id, user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Kierownik+został+wezwany", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/call-mieszalnia")
def maszyna_call_mieszalnia(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "CALL_MIXING", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_MIXING", machine.upper(), plan_id,
        f"Wezwanie operatora mieszalni na {machine.upper()} dla {plan['order_number']}", "operator_mieszalni", user["username"])
    log_production_operation(cur, "wezwanie", f"Wezwanie operatora mieszalni na {machine.upper()} dla {plan['order_number']}", machine.upper(), plan_id, user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Operator+mieszalni+został+wezany", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/call-prepress")
def maszyna_call_prepress(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "CALL_PREPRESS", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_PREPRESS", machine.upper(), plan_id,
        f"Wezwanie prepress na {machine.upper()} dla {plan['order_number']}", "prepress", user["username"])
    log_production_operation(cur, "wezwanie", f"Wezwanie prepress na {machine.upper()} dla {plan['order_number']}", machine.upper(), plan_id, user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Prepress+został+wezany", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/start")
def maszyna_job_start(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    cur.execute("UPDATE production_plans SET status='in_progress' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_STARTED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_STARTED", machine.upper(), plan_id,
        f"Rozpoczęcie zlecenia na {machine.upper()} dla {plan['order_number']}", "manager", user["username"])
    log_production_operation(cur, "rozpoczecie_zlecenia", f"Zlecenie rozpoczete {plan['order_number']} na {machine.upper()}", machine.upper(), plan_id, user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Zlecenie+rozpoczęte", status_code=303)

@app.get("/maszyna/{machine}/job/{plan_id}/complete")
def maszyna_job_complete(machine: str, plan_id: int, request: Request, user=Depends(require_auth)):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    cur.execute("UPDATE production_plans SET status='completed' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_COMPLETED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_COMPLETED", machine.upper(), plan_id,
        f"Ukończenie zlecenia na {machine.upper()} dla {plan['order_number']}", "manager", user["username"])
    log_production_operation(cur, "zakonczenie_zlecenia", f"Zlecenie zakonczone {plan['order_number']} na {machine.upper()}", machine.upper(), plan_id, user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Zlecenie+zakończone", status_code=303)

@app.post("/maszyna/{machine}/job/{plan_id}/submit-report")
def submit_report(
    machine: str,
    plan_id: int,
    request: Request,
    report_type: str = Form(...),
    report_date: str = Form(...),
    shift: str = Form(...),
    job_number: str = Form(...),
    status: str = Form(...),
    notes: str = Form(''),
    ok_quantity: int = Form(0),
    nok_quantity: int = Form(0),
    quantity: int = Form(0),
    user=Depends(require_auth)
):
    if user["role"] not in ["admin", "manager", "drukarz", "operator_mieszalni", "prepress"]:
        return RedirectResponse("/dashboard", status_code=303)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=?", (plan_id,))
    plan_row = cur.fetchone()
    lub = plan_row["lub_number"] if plan_row else None
    dpart = (report_date.split("T")[0] if "T" in report_date else report_date)[:10]
    tpart = report_date.split("T")[1][:8] if "T" in report_date else datetime.now().strftime("%H:%M:%S")
    shift_norm = normalize_shift_label(shift)

    if report_type == 'print_control':
        cur.execute(
            "INSERT INTO print_control_reports (machine, date, time, job_number, status, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (machine.upper(), dpart, tpart, job_number, status, notes, user["username"], plan_id))
        log_production_operation(cur, 'raport_zadruku', f'Raport kontroli zadruku: {job_number} [{status}]', machine.upper(), plan_id, user["username"])
        log_domain_event(cur, "RAPORT_ZADRUKU_ZAPISANY", user["username"], machine.upper(), plan_id, lub)
        insert_notification_if_enabled(
            cur, "RAPORT_ZADRUKU_ZAPISANY", machine.upper(), plan_id,
            f"Raport kontroli zadruku: {job_number} na {machine.upper()} — {status}", "manager", user["username"])
    else:
        cur.execute(
            "INSERT INTO production_reports (machine, date, shift, job_number, start_time, end_time, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (machine.upper(), dpart, shift_norm, job_number, tpart, tpart,
             quantity, ok_quantity, nok_quantity, notes, user["username"], plan_id))
        log_production_operation(cur, 'raport_produkcji', f'Raport produkcji: {job_number} qty {quantity} OK {ok_quantity} NOK {nok_quantity}', machine.upper(), plan_id, user["username"])
        log_domain_event(cur, "RAPORT_PRODUKCJI_ZAPISANY", user["username"], machine.upper(), plan_id, lub)
        insert_notification_if_enabled(
            cur, "RAPORT_PRODUKCJI_ZAPISANY", machine.upper(), plan_id,
            f"Raport produkcji: {job_number} na {machine.upper()} — szt. {quantity}, OK {ok_quantity}, NOK {nok_quantity}", "manager", user["username"])

    conn.commit()
    if user["role"] in ("manager", "admin"):
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={dpart}", status_code=303)
    return RedirectResponse(
        f"/maszyna/{machine.lower()}/job/{plan_id}?status=success&message=Raport+zapisany.+Kierownik+widzi+go+w+Rejestrze+raportów.",
        status_code=303,
    )

@app.get("/maszyna/{machine}/export-csv")
def export_plany_csv(machine: str, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    
    # Pobierz plany dla tej maszyny
    cur.execute("""
        SELECT order_number, artwork_number, lub_number, order_name, laminate, meters, pieces
        FROM production_plans
        WHERE machine=? AND status='planned'
        ORDER BY id
    """, (machine.upper(),))
    plans = cur.fetchall()
    
    # Stwórz CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['numer_zlecenia', 'numer_artwork', 'numer_lub', 'nazwa_zlecenia', 'laminat', 'ilosc_metrow', 'ilosc_sztuk'])
    
    # Data
    for plan in plans:
        writer.writerow(plan)
    
    # Zwróć jako plik
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=plany_{machine.lower()}.csv"}
    )

@app.get("/maszyna/{machine}/raport-zadruku", response_class=HTMLResponse)
def maszyna_raport_zadruku(machine: str, request: Request, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM print_control_reports WHERE machine=? AND date(created_at)=date('now') ORDER BY created_at DESC LIMIT 20", (machine.upper(),))
    reports = cur.fetchall()
    return render_template("maszyna_raport_zadruku.html", {
        "machine": machine.upper(),
        "reports": reports,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.post("/maszyna/{machine}/raport-zadruku")
def maszyna_dodaj_raport_zadruku(
    machine: str,
    request: Request,
    job_number: str = Form(...),
    status: str = Form(...),
    notes: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    plan_id = resolve_plan_id_for_job(cur, machine, job_number)
    cur.execute("""
        INSERT INTO print_control_reports (machine, date, time, job_number, status, notes, created_by, plan_id)
        VALUES (?, date('now'), time('now'), ?, ?, ?, ?, ?)
    """, (machine.upper(), job_number, status, notes, user["username"], plan_id))
    log_production_operation(cur, "raport_zadruku", f"[Panel maszyny] {job_number} [{status}]", machine.upper(), plan_id, user["username"])
    log_domain_event(cur, "RAPORT_ZADRUKU_ZAPISANY", user["username"], machine.upper(), plan_id, None)
    insert_notification_if_enabled(
        cur, "RAPORT_ZADRUKU_ZAPISANY", machine.upper(), plan_id,
        f"Raport zadruku (panel maszyny {machine.upper()}): {job_number} — {status}", "manager", user["username"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Raport zadruku dodany"})
    if user["role"] in ("manager", "admin"):
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={date.today().strftime('%Y-%m-%d')}", status_code=303)
    return RedirectResponse(f"/maszyna/{machine}/raport-zadruku?success=dodano", status_code=303)

@app.get("/maszyna/{machine}/raport-produkcji", response_class=HTMLResponse)
def maszyna_raport_produkcji(machine: str, request: Request, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_reports WHERE machine=? AND date(created_at)=date('now') ORDER BY created_at DESC LIMIT 20", (machine.upper(),))
    reports = cur.fetchall()
    return render_template("maszyna_raport_produkcji.html", {
        "machine": machine.upper(),
        "reports": reports,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.post("/maszyna/{machine}/raport-produkcji")
def maszyna_dodaj_raport_produkcji(
    machine: str,
    request: Request,
    job_number: str = Form(...),
    quantity: int = Form(...),
    ok_quantity: int = Form(...),
    nok_quantity: int = Form(...),
    notes: str = Form(""),
    user=Depends(require_auth)
):
    conn = get_db()
    cur = conn.cursor()
    plan_id = resolve_plan_id_for_job(cur, machine, job_number)
    cur.execute("""
        INSERT INTO production_reports (machine, date, shift, job_number, start_time, end_time, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id)
        VALUES (?, date('now'), 'dzien', ?, time('now'), time('now'), ?, ?, ?, ?, ?, ?)
    """, (machine.upper(), job_number, quantity, ok_quantity, nok_quantity, notes, user["username"], plan_id))
    log_production_operation(cur, "raport_produkcji", f"[Panel maszyny] {job_number} qty {quantity} OK {ok_quantity} NOK {nok_quantity}", machine.upper(), plan_id, user["username"])
    log_domain_event(cur, "RAPORT_PRODUKCJI_ZAPISANY", user["username"], machine.upper(), plan_id, None)
    insert_notification_if_enabled(
        cur, "RAPORT_PRODUKCJI_ZAPISANY", machine.upper(), plan_id,
        f"Raport produkcji (panel maszyny {machine.upper()}): {job_number} — {quantity} szt., OK {ok_quantity}, NOK {nok_quantity}", "manager", user["username"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Raport produkcji dodany"})
    if user["role"] in ("manager", "admin"):
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={date.today().strftime('%Y-%m-%d')}", status_code=303)
    return RedirectResponse(f"/maszyna/{machine}/raport-produkcji?success=dodano", status_code=303)

# ==================== ENDPOINTY DLA KIEROWNIKA ====================
@app.get("/kierownik/rejestr-raportow", response_class=HTMLResponse)
def kierownik_rejestr_raportow(
    request: Request,
    user=Depends(require_manager_or_admin),
    date_q: str = Query("", alias="date"),
):
    if not date_q:
        date_q = date.today().strftime("%Y-%m-%d")
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM print_control_reports WHERE date=? ORDER BY machine, created_at DESC",
        (date_q,),
    )
    print_reports = cur.fetchall()
    cur.execute(
        "SELECT * FROM production_reports WHERE date=? ORDER BY machine, created_at DESC",
        (date_q,),
    )
    production_reports = cur.fetchall()
    return render_template("kierownik_rejestr_raportow.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "date_q": date_q,
        "print_reports": print_reports,
        "production_reports": production_reports,
    })


@app.get("/kierownik/raport-zmiany", response_class=HTMLResponse)
def kierownik_raport_zmiany(
    request: Request,
    user=Depends(require_manager_or_admin),
    date_q: str = Query("", alias="date"),
    zmiana: str = Query("dzien"),
):
    if not date_q:
        date_q = date.today().strftime("%Y-%m-%d")
    zm = normalize_shift_label(zmiana)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM print_control_reports WHERE date=? ORDER BY machine, created_at", (date_q,))
    print_all = cur.fetchall()
    cur.execute("SELECT * FROM production_reports WHERE date=?", (date_q,))
    prod_all = [r for r in cur.fetchall() if normalize_shift_label(r["shift"]) == zm]
    total_qty = sum(int(r["quantity"] or 0) for r in prod_all)
    total_ok = sum(int(r["ok_quantity"] or 0) for r in prod_all)
    total_nok = sum(int(r["nok_quantity"] or 0) for r in prod_all)
    zadruk_ok = sum(1 for r in print_all if (r["status"] or "").upper() == "OK")
    zadruk_nok = len(print_all) - zadruk_ok
    by_machine = {}
    for r in prod_all:
        m = r["machine"]
        if m not in by_machine:
            by_machine[m] = {"qty": 0, "ok": 0, "nok": 0}
        by_machine[m]["qty"] += int(r["quantity"] or 0)
        by_machine[m]["ok"] += int(r["ok_quantity"] or 0)
        by_machine[m]["nok"] += int(r["nok_quantity"] or 0)
    return render_template("kierownik_raport_zmiany.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "date_q": date_q,
        "zmiana": zm,
        "print_all": print_all,
        "prod_filtered": prod_all,
        "total_qty": total_qty,
        "total_ok": total_ok,
        "total_nok": total_nok,
        "zadruk_ok": zadruk_ok,
        "zadruk_nok": zadruk_nok,
        "by_machine": by_machine,
    })


@app.get("/kierownik/raport-dziennie", response_class=HTMLResponse)
def kierownik_raport_dziennie(request: Request, user=Depends(require_manager_or_admin)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT machine, SUM(quantity) as total_qty, SUM(ok_quantity) as ok_qty, SUM(nok_quantity) as nok_qty
        FROM production_reports
        WHERE date(created_at)=date('now')
        GROUP BY machine
    """)
    production = cur.fetchall()
    return render_template("kierownik_raport_dziennie.html", {
        "production": production,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/kierownik/statystyki-zmian", response_class=HTMLResponse)
def kierownik_statystyki_zmian(request: Request, user=Depends(require_manager_or_admin)):
    return render_template("kierownik_statystyki.html", {
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/kierownik/raport-jakosci", response_class=HTMLResponse)
def kierownik_raport_jakosci(request: Request, user=Depends(require_manager_or_admin)):
    conn = get_db()
    cur = conn.cursor()
    
    # Podsumowanie jakości
    cur.execute("""
        SELECT 
            machine,
            COUNT(*) as total_reports,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) as ok_count,
            SUM(CASE WHEN status='NOT_OK' THEN 1 ELSE 0 END) as not_ok_count,
            ROUND(100.0 * SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_score
        FROM print_control_reports
        WHERE date(created_at)=date('now')
        GROUP BY machine
    """)
    quality = cur.fetchall()
    
    # Indywidualne raporty zadruku
    cur.execute("""
        SELECT * FROM print_control_reports
        WHERE date(created_at)=date('now')
        ORDER BY machine, created_at DESC
    """)
    reports = cur.fetchall()
    
    return render_template("kierownik_raport_jakosci.html", {
        "quality": quality,
        "reports": reports,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/kierownik/dziennik-zmian", response_class=HTMLResponse)
def kierownik_dziennik_zmian(request: Request, user=Depends(require_manager_or_admin)):
    conn = get_db()
    cur = conn.cursor()
    
    # Pobierz wszystkie operacje produkcyjne z ostatnich 7 dni
    cur.execute("""
        SELECT pl.*, p.order_number, p.machine
        FROM production_log pl
        LEFT JOIN production_plans p ON pl.plan_id = p.id
        WHERE pl.created_at >= datetime('now', '-7 days')
        ORDER BY pl.created_at DESC
    """)
    operations = cur.fetchall()
    
    return render_template("kierownik_dziennik_zmian.html", {
        "operations": operations,
        "user": {"username": user["username"], "role": user["role"]}
    })

@app.get("/maszyna/{machine}", response_class=HTMLResponse)
def podglad_maszyna(machine: str, request: Request, user=Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    
    # Pobierz plany produkcji dla tej maszyny
    cur.execute("""
        SELECT id, order_number, artwork_number, lub_number, order_name, laminate, meters, pieces, status
        FROM production_plans
        WHERE machine=? AND status='planned'
        ORDER BY id
    """, (machine.upper(),))
    plans = cur.fetchall()
    
    return render_template("maszyna_podglad.html", {
        "machine": machine.upper(),
        "plans": plans,
        "user": {"username": user["username"], "role": user["role"]}
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)