"""
Pomocnicze funkcje współdzielone przez wszystkie moduły aplikacji.
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime, date
from urllib.parse import urlencode

from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from starlette.requests import Request

from db_compat import is_postgres

# ==================== ŚCIEŻKI ====================

def get_base_path() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(__file__)


def get_db_path() -> str:
    return os.path.join(get_base_path(), "database.db")


def get_templates_path() -> str:
    return os.path.join(get_base_path(), "templates")


def get_resources_path() -> str:
    """Zwraca ścieżkę do folderu zasobów (CSV, assets itp)."""
    base = get_base_path()
    resources = os.path.join(base, "resources")
    if os.path.isdir(resources):
        return resources
    return base


# ==================== JINJA2 ====================

env: Environment | None = None


def init_jinja_env() -> None:
    global env
    if env is None:
        env = Environment(
            loader=FileSystemLoader(get_templates_path()),
            autoescape=True,
            cache_size=0,
        )


def render_template(name: str, context: dict) -> HTMLResponse:
    init_jinja_env()
    template = env.get_template(name)
    return HTMLResponse(template.render(context))


# ==================== DB HELPERY ====================

def dodaj_operacje(cur, typ, farba, ilosc, polka, uwagi="", farba_id=None):
    cur.execute(
        """
        INSERT INTO operacje (data, typ, farba, ilosc, polka, uwagi, farba_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (datetime.now().strftime("%Y-%m-%d %H:%M"), typ, farba, ilosc, polka, uwagi, farba_id),
    )


def log_production_operation(cur, operation_type, description, machine=None, plan_id=None, user="system"):
    cur.execute(
        """
        INSERT INTO production_log (operation_type, description, machine, plan_id, user, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        (operation_type, description, machine, plan_id, user),
    )


# ==================== DATY / ALERTY ====================

def alert_daty(data_val) -> str:
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
    return "ok"


# ==================== REDIRECT URL ====================

def build_redirect_url(request: Request, extra_params: dict | None = None) -> str:
    params = dict(request.query_params)
    if extra_params:
        params.update(extra_params)
    params = {k: v for k, v in params.items() if v}
    query = urlencode(params)
    return f"/magazyn?{query}" if query else "/magazyn"


# ==================== FILTROWANIE FARB ====================

def filtruj_farby(dane, search_field: str, search_value: str, filtr_alert: str, status: str) -> list:
    """Filtruje rekordy farb i zwraca listę słowników z kluczem 'alert'."""
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

    return farby, licznik_przeterminowane, licznik_uwaga, licznik_zutylizowane


# ==================== POWIADOMIENIA ====================

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


# ==================== MASZYNY / PLANY ====================

PRODUCTION_MACHINES = ("D6", "D8", "D10")


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
