"""
Punkt wejścia aplikacji PULSE.
Inicjalizuje FastAPI, middleware sesji, montuje statyczne pliki,
rejestruje routery i wywołuje init_db() przy starcie.

W produkcji ustaw SESSION_SECRET jako zmienną środowiskową.
"""
import os
import secrets

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from db_compat import get_db as _get_db_raw, is_postgres
from helpers import get_base_path, seed_notification_settings_rows

# ==================== APLIKACJA ====================
app = FastAPI()

_session_secret = os.environ.get("SESSION_SECRET") or secrets.token_urlsafe(32)
app.add_middleware(SessionMiddleware, secret_key=_session_secret)
app.mount("/static", StaticFiles(directory=os.path.join(get_base_path(), "static")), name="static")

# ==================== ROUTERY ====================
from routers import auth, admin, magazyn, polimery, maszyny, kierownik, notifications  # noqa: E402

app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(magazyn.router)
app.include_router(polimery.router)
app.include_router(maszyny.router)
app.include_router(kierownik.router)
app.include_router(notifications.router)

# ==================== INICJALIZACJA BAZY DANYCH ====================

def execute(cur, query, params=()):
    if is_postgres():
        query = query.replace("?", "%s")
    return cur.execute(query, params)


def migrate_schema(cur):
    """Dodaje brakujące kolumny w starszych plikach database.db."""
    if is_postgres():
        from db_compat import migrate_schema_postgres
        migrate_schema_postgres(cur)
        return
    migrations = {
        "operacje": [("farba_id", "INTEGER")],
        "production_reports": [("plan_id", "INTEGER")],
        "print_control_reports": [("plan_id", "INTEGER")],
        "production_plans": [("assortment_prep_status", "TEXT"), ("farby_prep_status", "TEXT"), ("polimery_prep_status", "TEXT")],
    }
    for table, columns in migrations.items():
        execute(cur, "PRAGMA table_info(%s)" % (table,))
        existing = {row[1] for row in cur.fetchall()}
        for col_name, col_type in columns:
            if col_name not in existing:
                execute(cur, "ALTER TABLE %s ADD COLUMN %s %s" % (table, col_name, col_type))
    # Tworzenie tabeli farba_lub_assignments jeśli nie istnieje (migracja starszych db)
    execute(cur, """
        CREATE TABLE IF NOT EXISTS farba_lub_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farba_id INTEGER NOT NULL,
            lub_number TEXT NOT NULL,
            plan_id INTEGER,
            assigned_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(farba_id, lub_number)
        )
    """)


def init_db():
    conn = _get_db_raw()
    cur = conn.cursor()

    if is_postgres():
        from db_compat import init_postgres_schema, seed_default_users_postgres
        init_postgres_schema(cur)
        migrate_schema(cur)
        seed_notification_settings_rows(cur)
        seed_default_users_postgres(cur)
        execute(cur, "SELECT COUNT(*) FROM shifts")
        if cur.fetchone()[0] == 0:
            execute(cur, "INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("dzień", "06:00", "18:00"))
            execute(cur, "INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("noc", "18:00", "06:00"))
        conn.commit()
        conn.close()
        return

    execute(cur, """
        CREATE TABLE IF NOT EXISTS farby (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pantone TEXT NOT NULL,
            lub TEXT,
            polka TEXT,
            waga REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'dostepna',
            data_produkcji DATE NOT NULL
        )
    """)
    execute(cur, """
        CREATE TABLE IF NOT EXISTS operacje (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            typ TEXT NOT NULL,
            farba TEXT NOT NULL,
            ilosc TEXT NOT NULL,
            polka TEXT,
            uwagi TEXT,
            farba_id INTEGER
        )
    """)
    execute(cur, """
        CREATE TABLE IF NOT EXISTS polymers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lub TEXT NOT NULL,
            kolor TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'dostepna',
            lokalizacja TEXT,
            data_waznosci DATE,
            uwagi TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    execute(cur, """
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
    execute(cur, """
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
            farby_prep_status TEXT DEFAULT 'pending',
            polimery_prep_status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    execute(cur, """
        CREATE TABLE IF NOT EXISTS production_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT NOT NULL,
            date DATE NOT NULL,
            shift TEXT NOT NULL,
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
    execute(cur, """
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
    execute(cur, """
        CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL
        )
    """)
    execute(cur, """
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
    execute(cur, """
        CREATE TABLE IF NOT EXISTS production_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_type TEXT NOT NULL,
            description TEXT NOT NULL,
            machine TEXT,
            plan_id INTEGER,
            user TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    execute(cur, """
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
    execute(cur, """
        CREATE TABLE IF NOT EXISTS notification_settings (
            event_key TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1
        )
    """)
    execute(cur, """
        CREATE TABLE IF NOT EXISTS farba_lub_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farba_id INTEGER NOT NULL,
            lub_number TEXT NOT NULL,
            plan_id INTEGER,
            assigned_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(farba_id, lub_number)
        )
    """)
    execute(cur, """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            role TEXT NOT NULL,
            password TEXT
        )
    """)

    migrate_schema(cur)
    seed_notification_settings_rows(cur)

    execute(cur, "SELECT 1 FROM users WHERE username=?", ("admin",))
    if not cur.fetchone():
        execute(cur, "INSERT INTO users (username, role, password) VALUES (?, ?, ?)", ("admin", "admin", "admin123"))
    execute(cur, "SELECT 1 FROM users WHERE username=?", ("drukarz1",))
    if not cur.fetchone():
        execute(cur, "INSERT INTO users (username, role, password) VALUES (?, ?, ?)", ("drukarz1", "drukarz", "drukarz123"))

    execute(cur, "SELECT COUNT(*) FROM shifts")
    if cur.fetchone()[0] == 0:
        execute(cur, "INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("dzień", "06:00", "18:00"))
        execute(cur, "INSERT INTO shifts (name, start_time, end_time) VALUES (?, ?, ?)", ("noc", "18:00", "06:00"))

    conn.commit()
    conn.close()


init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
