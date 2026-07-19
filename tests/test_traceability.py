import importlib
import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

router = importlib.import_module("routers.traceability")


def test_can_view_traceability_for_admin_and_manager():
    assert router._can_view_traceability({"role": "admin"}) is True
    assert router._can_view_traceability({"role": "manager"}) is True
    assert router._can_view_traceability({"role": "drukarz"}) is False


def test_build_traceability_context_finds_plan_and_events(tmp_path):
    db_path = tmp_path / "traceability.sqlite"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE production_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT,
            order_number TEXT,
            artwork_number TEXT,
            lub_number TEXT,
            order_name TEXT,
            laminate TEXT,
            meters INTEGER,
            pieces INTEGER,
            planned_date DATE,
            status TEXT,
            assortment_prep_status TEXT,
            farby_prep_status TEXT,
            polimery_prep_status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE production_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT,
            date DATE,
            shift TEXT,
            job_number TEXT,
            quantity INTEGER,
            ok_quantity INTEGER,
            nok_quantity INTEGER,
            notes TEXT,
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE print_control_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT,
            date DATE,
            time TIME,
            job_number TEXT,
            status TEXT,
            notes TEXT,
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE winding_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT,
            plan_id INTEGER,
            date DATE,
            shift TEXT,
            order_number TEXT,
            cut_meters REAL,
            ok_meters REAL,
            nok_meters REAL,
            notes TEXT,
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,
            actor_user TEXT,
            machine TEXT,
            plan_id INTEGER,
            lub_number TEXT,
            payload TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE farby (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pantone TEXT,
            lub TEXT,
            polka TEXT,
            waga REAL,
            status TEXT,
            data_produkcji DATE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE farba_lub_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farba_id INTEGER,
            lub_number TEXT,
            plan_id INTEGER,
            assigned_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        "INSERT INTO production_plans (machine, order_number, artwork_number, lub_number, order_name, status, assortment_prep_status, farby_prep_status, polimery_prep_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("D1", "ZL-1001", "ART-77", "LUB-88", "Testowy nadruk", "in_progress", "ready", "ready", "ready"),
    )
    plan_id = cur.lastrowid
    cur.execute(
        "INSERT INTO production_reports (machine, date, shift, job_number, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("D1", "2026-07-19", "dzień", "JOB-001", 1000, 980, 20, "ok", "admin", plan_id),
    )
    cur.execute(
        "INSERT INTO print_control_reports (machine, date, time, job_number, status, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("D1", "2026-07-19", "10:00", "JOB-001", "OK", "ok", "admin", plan_id),
    )
    cur.execute(
        "INSERT INTO winding_reports (machine, plan_id, date, shift, order_number, cut_meters, ok_meters, nok_meters, notes, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("W1", plan_id, "2026-07-19", "dzień", "ZL-1001", 500.0, 480.0, 20.0, "ok", "admin"),
    )
    cur.execute(
        "INSERT INTO events (event_type, actor_user, machine, plan_id, lub_number, payload) VALUES (?, ?, ?, ?, ?, ?)",
        ("JOB_STARTED", "admin", "D1", plan_id, "LUB-88", "Przygotowanie rozpoczęte"),
    )
    cur.execute(
        "INSERT INTO farby (pantone, lub, polka, waga, status, data_produkcji) VALUES (?, ?, ?, ?, ?, ?)",
        ("PANTONE 123", "LUB-88", "A1", 5.0, "dostepna", "2026-07-19"),
    )
    farba_id = cur.lastrowid
    cur.execute(
        "INSERT INTO farba_lub_assignments (farba_id, lub_number, plan_id, assigned_by) VALUES (?, ?, ?, ?)",
        (farba_id, "LUB-88", plan_id, "admin"),
    )
    conn.commit()

    result = router._build_traceability_context(cur, "ZL-1001")

    assert result["plan"]["order_number"] == "ZL-1001"
    assert result["plan"]["lub_number"] == "LUB-88"
    assert result["materials"][0]["pantone"] == "PANTONE 123"
    assert result["events"][0]["event_type"] == "JOB_STARTED"
