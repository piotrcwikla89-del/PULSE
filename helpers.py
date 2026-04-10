"""
Pomocnicze funkcje współdzielone przez wszystkie moduły aplikacji.
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from urllib.parse import urlencode

from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from starlette.requests import Request

from db_compat import is_postgres
from time_utils import format_local_datetime, local_datetime_str, local_now, local_today, utc_now_db_string

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
        env.filters["localdatetime"] = format_local_datetime


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
        (local_datetime_str(fmt="%Y-%m-%d %H:%M"), typ, farba, ilosc, polka, uwagi, farba_id),
    )


def log_production_operation(cur, operation_type, description, machine=None, plan_id=None, user="system"):
    cur.execute(
        """
        INSERT INTO production_log (operation_type, description, machine, plan_id, user, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (operation_type, description, machine, plan_id, user, utc_now_db_string()),
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
    dni = (local_today() - d).days
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

PROBLEM_CATEGORY_DEFINITIONS = (
    ("farby", "Farby", "operator_mieszalni", 1),
    ("polimery", "Polimery", "prepress", 2),
    ("laminat", "Laminat", "manager", 3),
    ("brak_pomocnika", "Brak pomocnika", "manager", 4),
    ("problemy_techniczne_maszyna", "Problemy techniczne na maszynie", "manager", 5),
)


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
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (event_type, actor_user, machine, plan_id, lub_number, payload, utc_now_db_string()),
    )


def seed_notification_settings_rows(cur):
    for key in NOTIFICATION_EVENT_LABELS:
        cur.execute(
            "INSERT OR IGNORE INTO notification_settings (event_key, enabled) VALUES (?, 1)",
            (key,),
        )


def seed_problem_categories(cur):
    for code, label, target_role, sort_order in PROBLEM_CATEGORY_DEFINITIONS:
        cur.execute(
            """
            INSERT INTO problem_categories (code, label, target_role, visible_for_manager, is_active, sort_order)
            VALUES (?, ?, ?, 1, 1, ?)
            ON CONFLICT(code) DO NOTHING
            """,
            (code, label, target_role, sort_order),
        )


def get_edit_password(cur) -> str:
    """Zwraca aktualne hasło edycji z system_settings (domyślnie 'haslo')."""
    try:
        cur.execute("SELECT value FROM system_settings WHERE key='edit_password'")
        row = cur.fetchone()
        return row["value"] if row else "haslo"
    except Exception:
        return "haslo"


# ==================== MASZYNY / PLANY ====================

PRODUCTION_MACHINES = ("D6", "D8", "D10")
WINDING_MACHINES = ("P1", "P2")


def normalize_shift_label(shift_val: str) -> str:
    if not shift_val:
        return "dzien"
    s = str(shift_val).lower().strip()
    if s in ("1", "dzien", "dzień", "day"):
        return "dzien"
    if s in ("noc", "night", "2"):
        return "noc"
    return s


def _parse_shift_time(value: str) -> tuple[int, int] | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parts = raw.split(":")
        return int(parts[0]), int(parts[1])
    except (IndexError, ValueError):
        return None


def resolve_active_shift(cur, now_local: datetime | None = None) -> tuple[str, str]:
    current_local = now_local or local_now()
    current_minutes = current_local.hour * 60 + current_local.minute
    shift_rows = []
    try:
        cur.execute("SELECT name, start_time, end_time FROM shifts")
        shift_rows = cur.fetchall()
    except Exception:
        shift_rows = []

    for row in shift_rows:
        shift_norm = normalize_shift_label(row["name"])
        start_parts = _parse_shift_time(row["start_time"])
        end_parts = _parse_shift_time(row["end_time"])
        if start_parts is None or end_parts is None:
            continue
        start_minutes = start_parts[0] * 60 + start_parts[1]
        end_minutes = end_parts[0] * 60 + end_parts[1]
        crosses_midnight = start_minutes >= end_minutes
        if not crosses_midnight and start_minutes <= current_minutes < end_minutes:
            return shift_norm, current_local.strftime("%Y-%m-%d")
        if crosses_midnight and (current_minutes >= start_minutes or current_minutes < end_minutes):
            shift_date = current_local.date()
            if current_minutes < end_minutes:
                shift_date = shift_date - timedelta(days=1)
            return shift_norm, shift_date.strftime("%Y-%m-%d")

    fallback_shift = "dzien" if 6 <= current_local.hour < 18 else "noc"
    fallback_date = current_local.date()
    if fallback_shift == "noc" and current_local.hour < 6:
        fallback_date = fallback_date - timedelta(days=1)
    return fallback_shift, fallback_date.strftime("%Y-%m-%d")


def find_pending_machine_handover(cur, machine: str, now_local: datetime | None = None) -> dict | None:
    active_shift, handover_date = resolve_active_shift(cur, now_local=now_local)
    cur.execute(
        """
        SELECT sh.*, incoming.name AS incoming_shift_name, outgoing.name AS outgoing_shift_name
        FROM shift_handovers sh
        JOIN shifts incoming ON incoming.id = sh.incoming_shift_id
        JOIN shifts outgoing ON outgoing.id = sh.outgoing_shift_id
        WHERE sh.machine=? AND sh.handover_date=? AND sh.status='waiting_ack'
        ORDER BY sh.id DESC
        """,
        (machine.upper(), handover_date),
    )
    for row in cur.fetchall():
        handover = dict(row)
        if normalize_shift_label(handover["incoming_shift_name"]) == active_shift:
            handover["active_shift"] = active_shift
            handover["reference_date"] = handover_date
            return handover
    return None


def has_pending_role_handover(cur, role: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM shift_handover_items shi
        JOIN shift_handovers sh ON sh.id = shi.handover_id
        LEFT JOIN production_report_issues pri ON pri.id = shi.production_report_issue_id
        WHERE shi.item_type='issue' AND shi.target_role=?
          AND COALESCE(shi.status, 'open')='open'
          AND COALESCE(pri.status, 'new') != 'resolved'
        LIMIT 1
        """,
        (role,),
    )
    return cur.fetchone() is not None


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
        # 1. Farby z polem lub= (klasyczne przypisanie)
        cur.execute("SELECT * FROM farby WHERE lub IN (%s)" % placeholders, t)
        seen_per_lub: dict = defaultdict(set)
        for f in cur.fetchall():
            fd = dict(f)
            fd["mag_alert"] = alert_daty(f["data_produkcji"]) if f["data_produkcji"] else "ok"
            if f["lub"]:
                farby_by_lub[f["lub"]].append(fd)
                seen_per_lub[f["lub"]].add(f["id"])
        # 2. Farby z tabeli farba_lub_assignments (ręczne / wielokrotne)
        try:
            cur.execute(
                "SELECT f.*, fla.lub_number AS assigned_lub FROM farby f "
                "JOIN farba_lub_assignments fla ON f.id = fla.farba_id "
                "WHERE fla.lub_number IN (%s)" % placeholders,
                t,
            )
            for f in cur.fetchall():
                fd = dict(f)
                assigned_lub = fd.pop("assigned_lub", None)
                if not assigned_lub:
                    continue
                fd["mag_alert"] = alert_daty(fd.get("data_produkcji")) if fd.get("data_produkcji") else "ok"
                if fd["id"] not in seen_per_lub.get(assigned_lub, set()):
                    farby_by_lub[assigned_lub].append(fd)
                    seen_per_lub[assigned_lub].add(fd["id"])
        except Exception:
            pass  # tabela może nie istnieć na starszych bazach
        # 3. Polimery
        cur.execute("SELECT * FROM polymers WHERE lub IN (%s)" % placeholders, t)
        for po in cur.fetchall():
            pol_by_lub[po["lub"]].append(dict(po))
    out = []
    for p in plan_rows:
        d = dict(p)
        lub = d.get("lub_number")
        d["farby"] = farby_by_lub.get(lub, []) if lub else []
        d["polimery"] = pol_by_lub.get(lub, []) if lub else []
        st = d.get("assortment_prep_status")
        if not st:
            d["assortment_prep_status"] = "pending"
        if not d.get("farby_prep_status"):
            d["farby_prep_status"] = "pending"
        if not d.get("polimery_prep_status"):
            d["polimery_prep_status"] = "pending"
        out.append(d)
    return out


def get_lub_farby(cur, lub_number: str) -> list:
    """Zwraca wszystkie farby powiązane z danym numerem LUB (bezpośrednio i przez assignment)."""
    result = []
    seen_ids: set = set()
    cur.execute("SELECT * FROM farby WHERE lub=?", (lub_number,))
    for f in cur.fetchall():
        fd = dict(f)
        fd["mag_alert"] = alert_daty(fd.get("data_produkcji")) if fd.get("data_produkcji") else "ok"
        result.append(fd)
        seen_ids.add(fd["id"])
    try:
        cur.execute(
            "SELECT f.* FROM farby f JOIN farba_lub_assignments fla ON f.id=fla.farba_id WHERE fla.lub_number=?",
            (lub_number,),
        )
        for f in cur.fetchall():
            if f["id"] not in seen_ids:
                fd = dict(f)
                fd["mag_alert"] = alert_daty(fd.get("data_produkcji")) if fd.get("data_produkcji") else "ok"
                result.append(fd)
                seen_ids.add(fd["id"])
    except Exception:
        pass
    return result
