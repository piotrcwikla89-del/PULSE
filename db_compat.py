"""
Warstwa kompatybilności: SQLite (domyślnie / dev) albo PostgreSQL przez DATABASE_URL.
Nie usuwa obsługi SQLite — gdy brak DATABASE_URL, zachowanie jak wcześniej.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from typing import Any, Optional

try:
    import psycopg2
    from psycopg2 import extras as pg_extras
except ImportError:
    psycopg2 = None  # type: ignore
    pg_extras = None  # type: ignore


def database_url() -> Optional[str]:
    u = os.environ.get("DATABASE_URL", "").strip()
    if not u:
        return None
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    return u


def is_postgres() -> bool:
    return database_url() is not None


if is_postgres() and psycopg2 is None:
    raise RuntimeError("DATABASE_URL ustawione, ale brak pakietu psycopg2 — zainstaluj psycopg2-binary")

INTEGRITY_ERRORS: tuple = (sqlite3.IntegrityError,)
if psycopg2 is not None:
    INTEGRITY_ERRORS = (sqlite3.IntegrityError, psycopg2.IntegrityError)


def adapt_sql_postgres(sql: str) -> str:
    """Tłumaczy typowe konstrukcje SQLite → PostgreSQL (placeholdery ? → %s)."""
    s = sql
    s = s.replace("datetime('now', '-7 days')", "(CURRENT_TIMESTAMP - INTERVAL '7 days')")
    s = s.replace("date(created_at)=date('now')", "(created_at::date = CURRENT_DATE)")
    s = s.replace("date(created_at) = date('now')", "(created_at::date = CURRENT_DATE)")
    s = s.replace("date(data) BETWEEN", "(data::timestamp::date) BETWEEN")
    s = s.replace("datetime('now')", "CURRENT_TIMESTAMP")
    s = s.replace("date('now')", "CURRENT_DATE")
    s = s.replace("time('now')", "CURRENT_TIME")
    s = s.replace(
        "INSERT OR IGNORE INTO notification_settings (event_key, enabled) VALUES (?, 1)",
        "INSERT INTO notification_settings (event_key, enabled) VALUES (%s, 1) ON CONFLICT (event_key) DO NOTHING",
    )
    s = s.replace(
        "INSERT INTO production_log (operation_type, description, machine, plan_id, user, created_at)",
        'INSERT INTO production_log (operation_type, description, machine, plan_id, "user", created_at)',
    )
    if "?" in s:
        s = s.replace("?", "%s")
    return s


class _PgCursor:
    """Kursor z emulacją sqlite: lastrowid (psycopg2 go nie ma — używa LASTVAL() po INSERT)."""

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self.lastrowid: int | None = None

    def execute(self, sql: str, params: Any = None):
        sql = adapt_sql_postgres(sql)
        self.lastrowid = None
        if params is None:
            self._raw.execute(sql)
        else:
            self._raw.execute(sql, params)
        # Po INSERT z SERIAL/IDENTITY — jak sqlite3.Cursor.lastrowid
        if sql.lstrip().upper().startswith("INSERT") and (self._raw.rowcount or 0) > 0:
            self._raw.execute("SELECT LASTVAL()")
            row = self._raw.fetchone()
            if row is not None:
                self.lastrowid = int(row[0])
        return self

    def __getattr__(self, name: str):
        return getattr(self._raw, name)


class _PgConnection:
    def __init__(self, raw: Any) -> None:
        self._raw = raw

    def cursor(self, *a: Any, **kw: Any):
        return _PgCursor(self._raw.cursor(*a, **kw))

    def commit(self):
        return self._raw.commit()

    def close(self):
        return self._raw.close()

    def __getattr__(self, name: str):
        return getattr(self._raw, name)


def _sqlite_db_path() -> str:
    if getattr(sys, "frozen", False):
        base = os.path.dirname(sys.executable)
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "database.db")


def get_db():
    """Połączenie z bazą: PostgreSQL (DATABASE_URL) lub lokalny SQLite (jak dotychczas)."""
    url = database_url()
    if url:
        conn = psycopg2.connect(url, cursor_factory=pg_extras.DictCursor)
        return _PgConnection(conn)
    conn = sqlite3.connect(_sqlite_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def migrate_schema_postgres(cur) -> None:
    migrations = {
        "operacje": [("farba_id", "INTEGER")],
        "production_reports": [("plan_id", "INTEGER")],
        "print_control_reports": [("plan_id", "INTEGER")],
        "production_plans": [("assortment_prep_status", "TEXT"), ("farby_prep_status", "TEXT"), ("polimery_prep_status", "TEXT")],
    }
    for table, columns in migrations.items():
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table,),
        )
        existing = {row["column_name"] for row in cur.fetchall()}
        for col_name, col_type in columns:
            if col_name not in existing:
                cur.execute('ALTER TABLE "%s" ADD COLUMN %s %s' % (table, col_name, col_type))
    # Migracja: tabela winding_reports
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='winding_reports'"
    )
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE winding_reports (
                id SERIAL PRIMARY KEY, machine TEXT NOT NULL, plan_id INTEGER,
                date DATE NOT NULL, shift TEXT NOT NULL, order_number TEXT NOT NULL,
                cut_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
                ok_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
                nok_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
                notes TEXT, created_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        """)
    # Migracja: tabela farba_lub_assignments
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='farba_lub_assignments'"
    )
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE farba_lub_assignments (
                id SERIAL PRIMARY KEY,
                farba_id INTEGER NOT NULL,
                lub_number TEXT NOT NULL,
                plan_id INTEGER,
                assigned_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(farba_id, lub_number)
            )
        """)


def init_postgres_schema(cur) -> None:
    """CREATE TABLE dla PostgreSQL (odpowiednik init_db SQLite)."""
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS farby (
            id SERIAL PRIMARY KEY,
            pantone TEXT NOT NULL,
            lub TEXT,
            polka TEXT,
            waga DOUBLE PRECISION NOT NULL,
            status TEXT NOT NULL DEFAULT 'dostepna',
            data_produkcji DATE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS operacje (
            id SERIAL PRIMARY KEY,
            data TEXT NOT NULL,
            typ TEXT NOT NULL,
            farba TEXT NOT NULL,
            ilosc TEXT NOT NULL,
            polka TEXT,
            uwagi TEXT,
            farba_id INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS polymers (
            id SERIAL PRIMARY KEY,
            lub TEXT NOT NULL,
            kolor TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'dostepna',
            lokalizacja TEXT,
            data_waznosci DATE,
            uwagi TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS polymer_operations (
            id SERIAL PRIMARY KEY,
            data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            typ TEXT NOT NULL,
            polymer_id INTEGER NOT NULL,
            lokalizacja TEXT,
            uwagi TEXT,
            FOREIGN KEY (polymer_id) REFERENCES polymers(id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS production_plans (
            id SERIAL PRIMARY KEY,
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
        """,
        """
        CREATE TABLE IF NOT EXISTS production_reports (
            id SERIAL PRIMARY KEY,
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
        """,
        """
        CREATE TABLE IF NOT EXISTS print_control_reports (
            id SERIAL PRIMARY KEY,
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
        """,
        """
        CREATE TABLE IF NOT EXISTS shifts (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            machine TEXT,
            plan_id INTEGER,
            message TEXT NOT NULL,
            target_role TEXT NOT NULL,
            target_user TEXT,
            created_by TEXT NOT NULL,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS production_log (
            id SERIAL PRIMARY KEY,
            operation_type TEXT NOT NULL,
            description TEXT NOT NULL,
            machine TEXT,
            plan_id INTEGER,
            "user" TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT NOT NULL,
            actor_user TEXT NOT NULL,
            machine TEXT,
            plan_id INTEGER,
            lub_number TEXT,
            payload TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS notification_settings (
            event_key TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS farba_lub_assignments (
            id SERIAL PRIMARY KEY,
            farba_id INTEGER NOT NULL,
            lub_number TEXT NOT NULL,
            plan_id INTEGER,
            assigned_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(farba_id, lub_number)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS winding_reports (
            id SERIAL PRIMARY KEY,
            machine TEXT NOT NULL,
            plan_id INTEGER,
            date DATE NOT NULL,
            shift TEXT NOT NULL,
            order_number TEXT NOT NULL,
            cut_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
            ok_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
            nok_meters DOUBLE PRECISION NOT NULL DEFAULT 0,
            notes TEXT,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            role TEXT NOT NULL,
            password TEXT
        )
        """,
    ]
    for stmt in stmts:
        cur.execute(stmt)


def seed_default_users_postgres(cur) -> None:
    cur.execute("SELECT 1 FROM users WHERE username = %s", ("admin",))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users (username, role, password) VALUES (%s, %s, %s)",
            ("admin", "admin", "admin123"),
        )
    cur.execute("SELECT 1 FROM users WHERE username = %s", ("drukarz1",))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users (username, role, password) VALUES (%s, %s, %s)",
            ("drukarz1", "drukarz", "drukarz123"),
        )
