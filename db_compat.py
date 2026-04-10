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
        "INSERT OR IGNORE INTO farba_lub_assignments (farba_id, lub_number, plan_id, assigned_by) VALUES (?, ?, ?, ?)",
        "INSERT INTO farba_lub_assignments (farba_id, lub_number, plan_id, assigned_by) VALUES (%s, %s, %s, %s) ON CONFLICT (farba_id, lub_number) DO NOTHING",
    )
    s = s.replace(
        "INSERT OR REPLACE INTO system_settings (key, value) VALUES ('edit_password', ?)",
        "INSERT INTO system_settings (key, value) VALUES ('edit_password', %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
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
        # Używamy SAVEPOINT, bo dla tabel bez sekwencji (np. TEXT PRIMARY KEY) LASTVAL() rzuca błąd
        # i bez SAVEPOINT PostgreSQL oznacza całą transakcję jako przerwana.
        if sql.lstrip().upper().startswith("INSERT") and (self._raw.rowcount or 0) > 0:
            try:
                self._raw.execute("SAVEPOINT _lastval_sp")
                self._raw.execute("SELECT LASTVAL()")
                row = self._raw.fetchone()
                if row is not None:
                    self.lastrowid = int(row[0])
                self._raw.execute("RELEASE SAVEPOINT _lastval_sp")
            except Exception:
                self._raw.execute("ROLLBACK TO SAVEPOINT _lastval_sp")
                self._raw.execute("RELEASE SAVEPOINT _lastval_sp")
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
        raw_cur = conn.cursor()
        raw_cur.execute("SET TIME ZONE 'UTC'")
        raw_cur.close()
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
    # Migracja: tabela system_settings
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='system_settings'"
    )
    if not cur.fetchone():
        cur.execute("CREATE TABLE system_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    cur.execute("INSERT INTO system_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                ("edit_password", "haslo"))
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
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='problem_categories'"
    )
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE problem_categories (
                id SERIAL PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL,
                target_role TEXT NOT NULL,
                visible_for_manager INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0
            )
        """)
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='production_report_issues'"
    )
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE production_report_issues (
                id SERIAL PRIMARY KEY,
                production_report_id INTEGER NOT NULL,
                problem_category_id INTEGER NOT NULL,
                machine TEXT NOT NULL,
                plan_id INTEGER,
                reported_by TEXT NOT NULL,
                issue_scope TEXT NOT NULL DEFAULT 'job',
                short_note TEXT,
                is_blocking INTEGER NOT NULL DEFAULT 0,
                needs_handover INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'new',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP,
                resolved_by TEXT,
                resolution_note TEXT,
                FOREIGN KEY (production_report_id) REFERENCES production_reports(id) ON DELETE CASCADE,
                FOREIGN KEY (problem_category_id) REFERENCES problem_categories(id),
                FOREIGN KEY (plan_id) REFERENCES production_plans(id)
            )
        """)
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='shift_handovers'"
    )
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE shift_handovers (
                id SERIAL PRIMARY KEY,
                handover_date DATE NOT NULL,
                machine TEXT NOT NULL,
                outgoing_shift_id INTEGER NOT NULL,
                incoming_shift_id INTEGER NOT NULL,
                created_by TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                summary_comment TEXT,
                status TEXT NOT NULL DEFAULT 'waiting_ack',
                acknowledged_by TEXT,
                acknowledged_at TIMESTAMP,
                acknowledgement_note TEXT,
                UNIQUE(handover_date, machine, outgoing_shift_id, incoming_shift_id),
                FOREIGN KEY (outgoing_shift_id) REFERENCES shifts(id),
                FOREIGN KEY (incoming_shift_id) REFERENCES shifts(id)
            )
        """)
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='shift_handover_items'"
    )
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE shift_handover_items (
                id SERIAL PRIMARY KEY,
                handover_id INTEGER NOT NULL,
                item_type TEXT NOT NULL,
                target_role TEXT,
                production_report_issue_id INTEGER,
                plan_id INTEGER,
                job_number TEXT,
                machine TEXT,
                lub_number TEXT,
                title TEXT NOT NULL,
                details TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (handover_id) REFERENCES shift_handovers(id) ON DELETE CASCADE,
                FOREIGN KEY (production_report_issue_id) REFERENCES production_report_issues(id),
                FOREIGN KEY (plan_id) REFERENCES production_plans(id)
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
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
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
        """
        CREATE TABLE IF NOT EXISTS problem_categories (
            id SERIAL PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL,
            target_role TEXT NOT NULL,
            visible_for_manager INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS production_report_issues (
            id SERIAL PRIMARY KEY,
            production_report_id INTEGER NOT NULL,
            problem_category_id INTEGER NOT NULL,
            machine TEXT NOT NULL,
            plan_id INTEGER,
            reported_by TEXT NOT NULL,
            issue_scope TEXT NOT NULL DEFAULT 'job',
            short_note TEXT,
            is_blocking INTEGER NOT NULL DEFAULT 0,
            needs_handover INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'new',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP,
            resolved_by TEXT,
            resolution_note TEXT,
            FOREIGN KEY (production_report_id) REFERENCES production_reports(id) ON DELETE CASCADE,
            FOREIGN KEY (problem_category_id) REFERENCES problem_categories(id),
            FOREIGN KEY (plan_id) REFERENCES production_plans(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS shift_handovers (
            id SERIAL PRIMARY KEY,
            handover_date DATE NOT NULL,
            machine TEXT NOT NULL,
            outgoing_shift_id INTEGER NOT NULL,
            incoming_shift_id INTEGER NOT NULL,
            created_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            summary_comment TEXT,
            status TEXT NOT NULL DEFAULT 'waiting_ack',
            acknowledged_by TEXT,
            acknowledged_at TIMESTAMP,
            acknowledgement_note TEXT,
            UNIQUE(handover_date, machine, outgoing_shift_id, incoming_shift_id),
            FOREIGN KEY (outgoing_shift_id) REFERENCES shifts(id),
            FOREIGN KEY (incoming_shift_id) REFERENCES shifts(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS shift_handover_items (
            id SERIAL PRIMARY KEY,
            handover_id INTEGER NOT NULL,
            item_type TEXT NOT NULL,
            target_role TEXT,
            production_report_issue_id INTEGER,
            plan_id INTEGER,
            job_number TEXT,
            machine TEXT,
            lub_number TEXT,
            title TEXT NOT NULL,
            details TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (handover_id) REFERENCES shift_handovers(id) ON DELETE CASCADE,
            FOREIGN KEY (production_report_issue_id) REFERENCES production_report_issues(id),
            FOREIGN KEY (plan_id) REFERENCES production_plans(id)
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
