"""
Router: maszyny produkcyjne — widoki, plany, zlecenia, raporty, akcje operatorów.
"""
import csv
import io
from collections import defaultdict

from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from starlette.requests import Request

from dependencies import get_db, is_ajax, require_auth, require_manager_or_admin
from helpers import (
    PRODUCTION_MACHINES,
    enrich_plans_with_lub_materials,
    find_pending_machine_handover,
    find_pending_role_shift_handover,
    get_lub_farby,
    has_pending_role_handover,
    insert_notification_if_enabled,
    log_domain_event,
    log_production_operation,
    normalize_shift_label,
    render_template,
    resolve_active_shift,
    resolve_plan_id_for_job,
)
from time_utils import local_date_str, local_day_bounds_utc, local_time_str, local_today

router = APIRouter()


def _resolve_shift_ids(cur, outgoing_shift: str) -> tuple[int | None, int | None, str, str]:
    outgoing_norm = normalize_shift_label(outgoing_shift)
    incoming_norm = "noc" if outgoing_norm == "dzien" else "dzien"
    cur.execute("SELECT id, name FROM shifts")
    shift_ids = {normalize_shift_label(row["name"]): row["id"] for row in cur.fetchall()}
    return shift_ids.get(outgoing_norm), shift_ids.get(incoming_norm), outgoing_norm, incoming_norm


def _load_handover_snapshot(cur, machine: str, report_date: str, shift_norm: str) -> tuple[list[dict], list[dict]]:
    cur.execute(
        """
        SELECT pr.plan_id, pr.job_number, SUM(pr.quantity) AS quantity, SUM(pr.ok_quantity) AS ok_quantity,
               SUM(pr.nok_quantity) AS nok_quantity, MAX(pr.created_at) AS last_report_at,
               pp.order_name, pp.lub_number
        FROM production_reports pr
        LEFT JOIN production_plans pp ON pp.id = pr.plan_id
        WHERE pr.machine=? AND pr.date=? AND pr.shift=? AND COALESCE(pp.status, '')='completed'
        GROUP BY pr.plan_id, pr.job_number, pp.order_name, pp.lub_number
        ORDER BY MAX(pr.created_at) DESC, pr.job_number
        """,
        (machine.upper(), report_date, shift_norm),
    )
    completed_jobs = [dict(row) for row in cur.fetchall()]

    cur.execute(
        """
        SELECT pri.id, pri.plan_id, pri.short_note, pri.status, pri.created_at,
               pc.label, pc.target_role, pr.job_number
        FROM production_report_issues pri
        JOIN production_reports pr ON pr.id = pri.production_report_id
        JOIN problem_categories pc ON pc.id = pri.problem_category_id
        WHERE pr.machine=? AND pr.date=? AND pr.shift=?
          AND COALESCE(pri.needs_handover, 1)=1
          AND COALESCE(pri.status, 'new') != 'resolved'
        ORDER BY pri.created_at DESC, pri.id DESC
        """,
        (machine.upper(), report_date, shift_norm),
    )
    issues = [dict(row) for row in cur.fetchall()]
    return completed_jobs, issues


def _load_role_handover_data(cur, role: str, status_filter: str = "open") -> tuple[list[dict], list[dict], str]:
    if role not in ("operator_mieszalni", "prepress"):
        return [], [], ""

    issue_where = ""
    if status_filter == "open":
        issue_where = " AND COALESCE(pri.status, 'new') != 'resolved'"
    elif status_filter == "resolved":
        issue_where = " AND COALESCE(pri.status, 'new') = 'resolved'"

    cur.execute(
        f"""
        SELECT sh.id AS handover_id,
               COALESCE(sh.handover_date, pr.date) AS handover_date,
               COALESCE(sh.machine, pr.machine, pri.machine) AS machine,
               COALESCE(sh.created_by, pri.reported_by) AS created_by,
               sh.created_at AS handover_created_at,
               sh.summary_comment,
               sh.status AS handover_status,
               ('Problem: ' || pc.label) AS title,
               COALESCE(shi.details, pri.short_note, ('Zlecenie: ' || COALESCE(pr.job_number, 'n/d'))) AS details,
               pr.job_number,
               pp.lub_number,
               pri.id AS production_issue_id, pri.short_note, pri.status AS issue_status,
               pri.resolved_at, pri.resolved_by, pri.resolution_note
        FROM production_report_issues pri
        JOIN problem_categories pc ON pc.id = pri.problem_category_id
        LEFT JOIN production_reports pr ON pr.id = pri.production_report_id
        LEFT JOIN production_plans pp ON pp.id = pri.plan_id
        LEFT JOIN shift_handover_items shi ON shi.production_report_issue_id = pri.id AND shi.item_type='issue'
        LEFT JOIN shift_handovers sh ON sh.id = shi.handover_id
        WHERE pc.target_role=?
          AND COALESCE(pri.needs_handover, 1)=1
        {issue_where}
        ORDER BY COALESCE(sh.handover_date, pr.date) DESC, pri.created_at DESC, pri.id DESC
        """,
        (role,),
    )
    issues = [dict(row) for row in cur.fetchall()]

    prep_column = "farby_prep_status" if role == "operator_mieszalni" else "polimery_prep_status"
    prep_title = "Farby przygotowane pod kolejne zlecenia" if role == "operator_mieszalni" else "Matryce przygotowane pod kolejne zlecenia"
    cur.execute(
        f"""
        SELECT id AS plan_id, machine, order_number, lub_number, order_name, laminate, planned_date,
               {prep_column} AS prep_status
        FROM production_plans
        WHERE status='planned' AND COALESCE({prep_column}, '')='ready'
        ORDER BY machine, planned_date, id
        """
    )
    prepared_items = [dict(row) for row in cur.fetchall()]
    return issues, prepared_items, prep_title


def _load_role_shift_handover(cur, role: str, report_date: str, shift: str) -> tuple[dict | None, str]:
    outgoing_shift_id, incoming_shift_id, shift_norm, incoming_shift = _resolve_shift_ids(cur, shift)
    if not outgoing_shift_id or not incoming_shift_id:
        return None, incoming_shift
    cur.execute(
        """
        SELECT rsh.*, incoming.name AS incoming_shift_name, outgoing.name AS outgoing_shift_name
        FROM role_shift_handovers rsh
        JOIN shifts incoming ON incoming.id = rsh.incoming_shift_id
        JOIN shifts outgoing ON outgoing.id = rsh.outgoing_shift_id
        WHERE rsh.handover_date=? AND rsh.role=? AND rsh.outgoing_shift_id=? AND rsh.incoming_shift_id=?
          AND rsh.status IN ('draft', 'waiting_ack', 'acknowledged')
        ORDER BY rsh.id DESC LIMIT 1
        """,
        (report_date, role, outgoing_shift_id, incoming_shift_id),
    )
    row = cur.fetchone()
    return (dict(row) if row else None), incoming_shift


def _finalize_job_completion(cur, machine: str, plan_row, plan_id: int, username: str) -> None:
    if not plan_row or plan_row["status"] == "completed":
        return
    cur.execute("UPDATE production_plans SET status='completed' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_COMPLETED", username, machine.upper(), plan_id, plan_row["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_COMPLETED", machine.upper(), plan_id,
        f"Ukończenie zlecenia na {machine.upper()} dla {plan_row['order_number']}", "manager", username,
    )
    log_production_operation(
        cur, "zakonczenie_zlecenia",
        f"Zlecenie zakonczone {plan_row['order_number']} na {machine.upper()}",
        machine.upper(), plan_id, username,
    )


@router.get("/maszyny")
def maszyny(request: Request, user=Depends(require_auth)):
    if user["role"] not in ["admin", "manager", "drukarz", "operator_mieszalni", "prepress"]:
        return RedirectResponse("/dashboard", status_code=303)
    assigned_machine = request.session.get("machine") if user["role"] == "drukarz" else None
    return render_template("maszyny.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "assigned_machine": assigned_machine,
    })


@router.get("/plany")
def plany(request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    if user["role"] not in ["operator_mieszalni", "prepress", "drukarz", "manager", "admin"]:
        return RedirectResponse("/dashboard", status_code=303)
    machines = list(PRODUCTION_MACHINES)
    cur = conn.cursor()
    machine_prep = {}
    for m in machines:
        cur.execute(
            """
            SELECT COUNT(*) AS c,
                   SUM(CASE WHEN COALESCE(farby_prep_status,'')='ready' THEN 1 ELSE 0 END) AS fr,
                   SUM(CASE WHEN COALESCE(polimery_prep_status,'')='ready' THEN 1 ELSE 0 END) AS pr
            FROM production_plans WHERE machine=? AND status='planned'
            """,
            (m,),
        )
        row = cur.fetchone()
        tot = row["c"] or 0
        fr = min(row["fr"] or 0, tot)
        pr = min(row["pr"] or 0, tot)
        machine_prep[m] = {"total": tot, "farby_ready": fr, "polimery_ready": pr}
    return render_template("plany_machines.html", {
        "machines": machines,
        "machine_prep": machine_prep,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.get("/przekazanie-zmiany")
def role_przekazanie_zmiany(
    request: Request,
    user=Depends(require_auth),
    success: str = Query(""),
    view: str = Query("open"),
    report_date: str = Query("", alias="date"),
    shift: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_mieszalni", "prepress"):
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    view_mode = view if view in ("open", "resolved", "all") else "open"
    issues, prepared_items, prep_title = _load_role_handover_data(cur, user["role"], status_filter=view_mode)
    active_shift, active_date = resolve_active_shift(cur)
    date_q = report_date or active_date
    shift_q = normalize_shift_label(shift or active_shift)
    existing_handover, incoming_shift = _load_role_shift_handover(cur, user["role"], date_q, shift_q)
    pending_role_handover = find_pending_role_shift_handover(cur, user["role"])
    return render_template("rola_przekazanie_zmiany.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "issues": issues,
        "prepared_items": prepared_items,
        "prep_title": prep_title,
        "success": success,
        "view_mode": view_mode,
        "date_q": date_q,
        "shift": shift_q,
        "incoming_shift": incoming_shift,
        "existing_handover": existing_handover,
        "pending_role_handover": pending_role_handover,
        "has_pending_handover": has_pending_role_handover(cur, user["role"]),
    })


@router.post("/przekazanie-zmiany")
def role_zapisz_przekazanie_zmiany(
    request: Request,
    report_date: str = Form(...),
    shift: str = Form(...),
    summary_comment: str = Form(""),
    action: str = Form("send"),
    view: str = Form("open"),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_mieszalni", "prepress"):
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    outgoing_shift_id, incoming_shift_id, shift_norm, _incoming_shift = _resolve_shift_ids(cur, shift)
    next_view = view if view in ("open", "resolved", "all") else "open"
    if not outgoing_shift_id or not incoming_shift_id:
        return RedirectResponse(
            f"/przekazanie-zmiany?date={report_date}&shift={shift}&view={next_view}",
            status_code=303,
        )

    cur.execute(
        """
        SELECT id FROM role_shift_handovers
        WHERE handover_date=? AND role=? AND outgoing_shift_id=? AND incoming_shift_id=?
        ORDER BY id DESC LIMIT 1
        """,
        (report_date, user["role"], outgoing_shift_id, incoming_shift_id),
    )
    existing = cur.fetchone()
    action_mode = action if action in ("draft", "send") else "send"
    trimmed_comment = summary_comment.strip()

    if action_mode == "draft":
        if existing:
            cur.execute(
                "UPDATE role_shift_handovers SET created_by=?, summary_comment=?, status='draft' WHERE id=?",
                (user["username"], trimmed_comment, existing["id"]),
            )
        else:
            cur.execute(
                """
                INSERT INTO role_shift_handovers (handover_date, role, outgoing_shift_id, incoming_shift_id, created_by, summary_comment, status)
                VALUES (?, ?, ?, ?, ?, ?, 'draft')
                """,
                (report_date, user["role"], outgoing_shift_id, incoming_shift_id, user["username"], trimmed_comment),
            )
        log_production_operation(
            cur,
            "szkic_przekazania_zmiany_roli",
            f"Zapisano szkic przekazania zmiany dla roli {user['role']} ({shift_norm})",
            None,
            None,
            user["username"],
        )
        conn.commit()
        return RedirectResponse(
            f"/przekazanie-zmiany?date={report_date}&shift={shift_norm}&view={next_view}&success=draft",
            status_code=303,
        )

    if existing:
        cur.execute(
            "UPDATE role_shift_handovers SET created_by=?, summary_comment=?, status='waiting_ack', acknowledged_by=NULL, acknowledged_at=NULL, acknowledgement_note=NULL WHERE id=?",
            (user["username"], trimmed_comment, existing["id"]),
        )
        handover_id = existing["id"]
    else:
        cur.execute(
            """
            INSERT INTO role_shift_handovers (handover_date, role, outgoing_shift_id, incoming_shift_id, created_by, summary_comment, status)
            VALUES (?, ?, ?, ?, ?, ?, 'waiting_ack')
            """,
            (report_date, user["role"], outgoing_shift_id, incoming_shift_id, user["username"], trimmed_comment),
        )
        handover_id = cur.lastrowid

    log_production_operation(
        cur,
        "przekazanie_zmiany_roli",
        f"Wysłano przekazanie zmiany dla roli {user['role']} ({shift_norm})",
        None,
        None,
        user["username"],
    )
    log_domain_event(cur, "ROLE_SHIFT_HANDOVER_CREATED", user["username"], None, None, None, f"{user['role']}:{handover_id}")
    conn.commit()
    return RedirectResponse(
        f"/przekazanie-zmiany?date={report_date}&shift={shift_norm}&view={next_view}&success=sent",
        status_code=303,
    )


@router.post("/przekazanie-zmiany/odbior")
def role_potwierdz_odbior_przekazania(
    request: Request,
    handover_id: int = Form(...),
    acknowledgement_note: str = Form(""),
    view: str = Form("open"),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_mieszalni", "prepress"):
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM role_shift_handovers
        WHERE id=? AND role=?
        """,
        (handover_id, user["role"]),
    )
    handover = cur.fetchone()
    next_view = view if view in ("open", "resolved", "all") else "open"
    if not handover or handover["status"] != "waiting_ack":
        return RedirectResponse(f"/przekazanie-zmiany?view={next_view}", status_code=303)
    cur.execute(
        "UPDATE role_shift_handovers SET status='acknowledged', acknowledged_by=?, acknowledged_at=CURRENT_TIMESTAMP, acknowledgement_note=? WHERE id=?",
        (user["username"], acknowledgement_note.strip(), handover_id),
    )
    log_production_operation(
        cur,
        "odbior_przekazania_zmiany_roli",
        f"Przejęto przekazanie zmiany dla roli {user['role']} #{handover_id}",
        None,
        None,
        user["username"],
    )
    log_domain_event(cur, "ROLE_SHIFT_HANDOVER_ACKNOWLEDGED", user["username"], None, None, None, str(handover_id))
    conn.commit()
    return RedirectResponse(f"/przekazanie-zmiany?view={next_view}&success=ack", status_code=303)


@router.post("/przekazanie-zmiany/problem/{issue_id}/status")
def role_problem_status_update(
    issue_id: int,
    request: Request,
    resolved: str = Form(...),
    resolution_note: str = Form(""),
    view: str = Form("open"),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_mieszalni", "prepress"):
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pri.id, pc.target_role, pr.machine, pr.plan_id, pc.label
        FROM production_report_issues pri
        JOIN problem_categories pc ON pc.id = pri.problem_category_id
        LEFT JOIN production_reports pr ON pr.id = pri.production_report_id
        WHERE pri.id=?
        """,
        (issue_id,),
    )
    issue_row = cur.fetchone()
    if not issue_row or issue_row["target_role"] != user["role"]:
        return RedirectResponse("/przekazanie-zmiany", status_code=303)

    if resolved == "yes":
        cur.execute(
            "UPDATE production_report_issues SET status='resolved', resolved_at=CURRENT_TIMESTAMP, resolved_by=?, resolution_note=? WHERE id=?",
            (user["username"], resolution_note.strip(), issue_id),
        )
        cur.execute(
            "UPDATE shift_handover_items SET status='resolved' WHERE production_report_issue_id=?",
            (issue_id,),
        )
        log_production_operation(
            cur,
            "problem_rozwiazany",
            f"Problem rozwiązany: {issue_row['label']}",
            issue_row["machine"],
            issue_row["plan_id"],
            user["username"],
        )
    else:
        cur.execute(
            "UPDATE production_report_issues SET status='new', resolved_at=NULL, resolved_by=NULL, resolution_note=NULL WHERE id=?",
            (issue_id,),
        )
        cur.execute(
            "UPDATE shift_handover_items SET status='open' WHERE production_report_issue_id=?",
            (issue_id,),
        )

    conn.commit()
    next_view = view if view in ("open", "resolved", "all") else "open"
    return RedirectResponse(f"/przekazanie-zmiany?success=status&view={next_view}", status_code=303)


@router.get("/select-machine")
def select_machine_form(request: Request, user=Depends(require_auth)):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    return render_template("select_machine.html", {"user": user})


@router.post("/select-machine")
def select_machine(request: Request, machine: str = Form(...), user=Depends(require_auth)):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    if machine.upper() not in PRODUCTION_MACHINES:
        return RedirectResponse("/select-machine", status_code=303)
    request.session["machine"] = machine.upper()
    return RedirectResponse("/dashboard", status_code=303)


@router.get("/maszyna/{machine}/plany")
def maszyna_plany(
    machine: str,
    request: Request,
    user=Depends(require_auth),
    success: str = Query(""),
    error: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] not in ["admin", "manager", "drukarz", "operator_mieszalni", "prepress"]:
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM production_plans WHERE machine=? AND status='planned' ORDER BY id",
        (machine.upper(),),
    )
    plan_rows = cur.fetchall()
    cur.execute(
        "SELECT * FROM production_plans WHERE machine=? AND status='in_progress' ORDER BY id DESC LIMIT 1",
        (machine.upper(),),
    )
    active_plan = cur.fetchone()
    plans = enrich_plans_with_lub_materials(cur, plan_rows)
    can_move = user["role"] in ("admin", "manager")
    other_machines = [m for m in PRODUCTION_MACHINES if m != machine.upper()]
    prep_ui = user["role"] in ("operator_mieszalni", "prepress")
    show_prep_column = user["role"] in ("drukarz", "manager", "admin", "operator_mieszalni", "prepress")
    return render_template("maszyna_plany.html", {
        "machine": machine.upper(),
        "plans": plans,
        "active_plan": dict(active_plan) if active_plan else None,
        "user": {"username": user["username"], "role": user["role"]},
        "can_move": can_move,
        "other_machines": other_machines,
        "success_msg": success,
        "error_msg": error,
        "prep_ui": prep_ui,
        "show_prep_column": show_prep_column,
    })


@router.get("/maszyna/{machine}/przekazanie-zmiany")
def maszyna_przekazanie_zmiany(
    machine: str,
    request: Request,
    user=Depends(require_auth),
    report_date: str = Query("", alias="date"),
    shift: str = Query("dzien"),
    success: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    date_q = report_date or local_today().strftime("%Y-%m-%d")
    cur = conn.cursor()
    outgoing_shift_id, incoming_shift_id, shift_norm, incoming_shift = _resolve_shift_ids(cur, shift)
    completed_jobs, issues = _load_handover_snapshot(cur, machine, date_q, shift_norm)
    existing_handover = None
    existing_items = []
    if outgoing_shift_id and incoming_shift_id:
        cur.execute(
            """
            SELECT * FROM shift_handovers
            WHERE handover_date=? AND machine=? AND outgoing_shift_id=? AND incoming_shift_id=?
              AND status IN ('draft', 'waiting_ack', 'acknowledged')
            ORDER BY id DESC LIMIT 1
            """,
            (date_q, machine.upper(), outgoing_shift_id, incoming_shift_id),
        )
        row = cur.fetchone()
        if row:
            existing_handover = dict(row)
            cur.execute(
                "SELECT * FROM shift_handover_items WHERE handover_id=? ORDER BY item_type, sort_order, id",
                (existing_handover["id"],),
            )
            existing_items = [dict(item) for item in cur.fetchall()]
    return render_template("drukarz_przekazanie_zmiany.html", {
        "machine": machine.upper(),
        "user": {"username": user["username"], "role": user["role"]},
        "date_q": date_q,
        "shift": shift_norm,
        "incoming_shift": incoming_shift,
        "completed_jobs": completed_jobs,
        "issues": issues,
        "existing_handover": existing_handover,
        "existing_items": existing_items,
        "success": success,
    })


@router.get("/maszyna/{machine}/przekazanie-zmiany/odbior")
def maszyna_odbior_przekazania_zmiany(
    machine: str,
    request: Request,
    handover_id: int = Query(0),
    success: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    handover = None
    if handover_id:
        cur.execute(
            """
            SELECT sh.*, incoming.name AS incoming_shift_name, outgoing.name AS outgoing_shift_name
            FROM shift_handovers sh
            JOIN shifts incoming ON incoming.id = sh.incoming_shift_id
            JOIN shifts outgoing ON outgoing.id = sh.outgoing_shift_id
            WHERE sh.id=? AND sh.machine=?
            """,
            (handover_id, machine.upper()),
        )
        row = cur.fetchone()
        if row:
            handover = dict(row)
    if not handover:
        handover = find_pending_machine_handover(cur, machine)
    if not handover or handover["status"] != "waiting_ack":
        return RedirectResponse(f"/maszyna/{machine.lower()}/plany", status_code=303)
    cur.execute(
        "SELECT * FROM shift_handover_items WHERE handover_id=? ORDER BY item_type, sort_order, id",
        (handover["id"],),
    )
    items = [dict(row) for row in cur.fetchall()]
    completed_items = [item for item in items if item["item_type"] == "completed_job"]
    issue_items = [item for item in items if item["item_type"] == "issue"]
    return render_template("drukarz_odbior_przekazania.html", {
        "machine": machine.upper(),
        "user": {"username": user["username"], "role": user["role"]},
        "handover": handover,
        "completed_items": completed_items,
        "issue_items": issue_items,
        "success": success,
    })


@router.post("/maszyna/{machine}/przekazanie-zmiany/odbior")
def maszyna_potwierdz_odbior_przekazania(
    machine: str,
    request: Request,
    handover_id: int = Form(...),
    acknowledgement_note: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM shift_handovers WHERE id=? AND machine=?", (handover_id, machine.upper()))
    handover = cur.fetchone()
    if not handover or handover["status"] != "waiting_ack":
        return RedirectResponse(f"/maszyna/{machine.lower()}/plany", status_code=303)
    cur.execute(
        "UPDATE shift_handovers SET status='acknowledged', acknowledged_by=?, acknowledged_at=CURRENT_TIMESTAMP, acknowledgement_note=? WHERE id=?",
        (user["username"], acknowledgement_note.strip(), handover_id),
    )
    log_production_operation(
        cur,
        "odbior_przekazania_zmiany",
        f"Przejęcie zmiany na {machine.upper()} dla przekazania #{handover_id}",
        machine.upper(),
        None,
        user["username"],
    )
    log_domain_event(cur, "SHIFT_HANDOVER_ACKNOWLEDGED", user["username"], machine.upper(), None, None, str(handover_id))
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine.lower()}/plany?success=przejecie_zmiany", status_code=303)


@router.post("/maszyna/{machine}/przekazanie-zmiany")
def maszyna_zapisz_przekazanie_zmiany(
    machine: str,
    request: Request,
    report_date: str = Form(...),
    shift: str = Form(...),
    summary_comment: str = Form(""),
    action: str = Form("send"),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] != "drukarz":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    outgoing_shift_id, incoming_shift_id, shift_norm, _incoming_shift = _resolve_shift_ids(cur, shift)
    if not outgoing_shift_id or not incoming_shift_id:
        return RedirectResponse(
            f"/maszyna/{machine.lower()}/przekazanie-zmiany?date={report_date}&shift={shift_norm}",
            status_code=303,
        )
    completed_jobs, issues = _load_handover_snapshot(cur, machine, report_date, shift_norm)
    cur.execute(
        """
        SELECT id FROM shift_handovers
        WHERE handover_date=? AND machine=? AND outgoing_shift_id=? AND incoming_shift_id=?
        ORDER BY id DESC LIMIT 1
        """,
        (report_date, machine.upper(), outgoing_shift_id, incoming_shift_id),
    )
    existing = cur.fetchone()
    action_mode = action if action in ("draft", "send") else "send"

    if action_mode == "draft":
        if existing:
            handover_id = existing["id"]
            cur.execute(
                "UPDATE shift_handovers SET created_by=?, summary_comment=?, status='draft' WHERE id=?",
                (user["username"], summary_comment.strip(), handover_id),
            )
            cur.execute("DELETE FROM shift_handover_items WHERE handover_id=?", (handover_id,))
        else:
            cur.execute(
                """
                INSERT INTO shift_handovers (handover_date, machine, outgoing_shift_id, incoming_shift_id, created_by, summary_comment, status)
                VALUES (?, ?, ?, ?, ?, ?, 'draft')
                """,
                (report_date, machine.upper(), outgoing_shift_id, incoming_shift_id, user["username"], summary_comment.strip()),
            )
        log_production_operation(
            cur,
            "szkic_przekazania_zmiany",
            f"Zapisano szkic przekazania zmiany dla {machine.upper()} ({shift_norm})",
            machine.upper(),
            None,
            user["username"],
        )
        conn.commit()
        return RedirectResponse(
            f"/maszyna/{machine.lower()}/przekazanie-zmiany?date={report_date}&shift={shift_norm}&success=draft",
            status_code=303,
        )

    if existing:
        handover_id = existing["id"]
        cur.execute(
            "UPDATE shift_handovers SET created_by=?, summary_comment=?, status='waiting_ack', acknowledged_by=NULL, acknowledged_at=NULL, acknowledgement_note=NULL WHERE id=?",
            (user["username"], summary_comment.strip(), handover_id),
        )
        cur.execute("DELETE FROM shift_handover_items WHERE handover_id=?", (handover_id,))
    else:
        cur.execute(
            """
            INSERT INTO shift_handovers (handover_date, machine, outgoing_shift_id, incoming_shift_id, created_by, summary_comment)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (report_date, machine.upper(), outgoing_shift_id, incoming_shift_id, user["username"], summary_comment.strip()),
        )
        handover_id = cur.lastrowid

    for index, job in enumerate(completed_jobs, start=1):
        title = f"Zakończone zlecenie: {job['job_number']}"
        details = f"Ilość: {job['quantity'] or 0}, OK: {job['ok_quantity'] or 0}, NOK: {job['nok_quantity'] or 0}"
        cur.execute(
            """
            INSERT INTO shift_handover_items
            (handover_id, item_type, plan_id, job_number, machine, lub_number, title, details, status, sort_order)
            VALUES (?, 'completed_job', ?, ?, ?, ?, ?, ?, 'done', ?)
            """,
            (handover_id, job.get("plan_id"), job.get("job_number"), machine.upper(), job.get("lub_number"), title, details, index),
        )

    for index, issue in enumerate(issues, start=1):
        title = f"Problem: {issue['label']}"
        details = issue.get("short_note") or f"Zlecenie: {issue.get('job_number') or 'n/d'}"
        cur.execute(
            """
            INSERT INTO shift_handover_items
            (handover_id, item_type, target_role, production_report_issue_id, plan_id, job_number, machine, title, details, status, sort_order)
            VALUES (?, 'issue', ?, ?, ?, ?, ?, ?, ?, 'open', ?)
            """,
            (handover_id, issue.get("target_role"), issue.get("id"), issue.get("plan_id"), issue.get("job_number"), machine.upper(), title, details, index),
        )

    log_production_operation(
        cur,
        "przekazanie_zmiany",
        f"Przekazanie zmiany dla {machine.upper()} ({shift_norm}) — zlecenia: {len(completed_jobs)}, problemy: {len(issues)}",
        machine.upper(),
        None,
        user["username"],
    )
    log_domain_event(cur, "SHIFT_HANDOVER_CREATED", user["username"], machine.upper(), None, None, f"{report_date}:{shift_norm}")
    conn.commit()
    return RedirectResponse(
        f"/maszyna/{machine.lower()}/przekazanie-zmiany?date={report_date}&shift={shift_norm}&success=sent",
        status_code=303,
    )


@router.post("/maszyna/{machine}/plan/{plan_id}/potwierdz-asortyment")
def potwierdz_asortyment(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_mieszalni", "prepress"):
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan or plan["status"] != "planned":
        return RedirectResponse(f"/maszyna/{machine.lower()}/plany?error=brak_zlecenia", status_code=303)

    lub = plan["lub_number"]

    if user["role"] == "operator_mieszalni":
        # --- Walidacja farb ---
        if not lub:
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/plany?error=brak_lub&plan_id={plan_id}", status_code=303
            )
        farby = get_lub_farby(cur, lub)
        if not farby:
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/plany?error=brak_farb&plan_id={plan_id}", status_code=303
            )
        bledy = []
        for f in farby:
            if f["status"] == "zutylizowana":
                bledy.append(f"Farba {f['pantone']} jest zutylizowana")
            elif f["mag_alert"] == "przeterminowana":
                bledy.append(f"Farba {f['pantone']} jest przeterminowana")
        if bledy:
            import urllib.parse
            bledy_str = urllib.parse.quote(" | ".join(bledy[:3]))
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/plany?error=asortyment_blad&plan_id={plan_id}&bledy={bledy_str}",
                status_code=303,
            )
        # Auto-pobranie: dostępne farby → w_uzyciu
        from helpers import dodaj_operacje as _dodaj_op, alert_daty as _alert
        for f in farby:
            if f["status"] == "dostepna":
                cur.execute("UPDATE farby SET status='w_uzyciu' WHERE id=?", (f["id"],))
                _dodaj_op(cur, "wydanie_asortyment", f["pantone"], str(f["waga"]), f.get("polka", ""),
                          f"Auto-pobranie dla LUB {lub} zlecenie {plan['order_number']}", f["id"])
        cur.execute(
            "UPDATE production_plans SET farby_prep_status='ready' WHERE id=? AND machine=?",
            (plan_id, machine.upper()),
        )
        success_key = "asortyment_farby"
        desc = f"Farby zatwierdzone dla {plan['order_number']} ({machine.upper()}) przez {user['username']}"
        notif_msg = f"Zatwierdzono przygotowanie farb dla {plan['order_number']} na {machine.upper()}"

    else:  # prepress
        # --- Walidacja polimerów ---
        if not lub:
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/plany?error=brak_lub&plan_id={plan_id}", status_code=303
            )
        cur.execute("SELECT * FROM polymers WHERE lub=?", (lub,))
        polimery = [dict(p) for p in cur.fetchall()]
        if not polimery:
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/plany?error=brak_polimerów&plan_id={plan_id}", status_code=303
            )
        bledy = []
        for p in polimery:
            if p["status"] == "zutylizowana":
                bledy.append(f"Polimer {p['kolor']} jest zutylizowany")
            elif p["status"] == "uszkodzona":
                bledy.append(f"Polimer {p['kolor']} jest uszkodzony")
        if bledy:
            import urllib.parse
            bledy_str = urllib.parse.quote(" | ".join(bledy[:3]))
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/plany?error=asortyment_blad&plan_id={plan_id}&bledy={bledy_str}",
                status_code=303,
            )
        # Auto-pobranie: dostępne polimery → w_uzyciu
        for p in polimery:
            if p["status"] == "dostepna":
                cur.execute("UPDATE polymers SET status='w_uzyciu' WHERE id=?", (p["id"],))
                cur.execute(
                    "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('wydanie_asortyment', ?, ?, ?)",
                    (p["id"], p.get("lokalizacja"), f"Auto-pobranie dla LUB {lub} zlecenie {plan['order_number']}"),
                )
        cur.execute(
            "UPDATE production_plans SET polimery_prep_status='ready' WHERE id=? AND machine=?",
            (plan_id, machine.upper()),
        )
        success_key = "asortyment_polimery"
        desc = f"Matryce zatwierdzone dla {plan['order_number']} ({machine.upper()}) przez {user['username']}"
        notif_msg = f"Zatwierdzono przygotowanie matryc dla {plan['order_number']} na {machine.upper()}"

    log_production_operation(cur, "asortyment_zatwierdzony", desc, machine.upper(), plan_id, user["username"])
    log_domain_event(cur, "ASSORTMENT_CONFIRMED", user["username"], machine.upper(), plan_id, plan["lub_number"], None)
    insert_notification_if_enabled(
        cur, "ASSORTMENT_CONFIRMED", machine.upper(), plan_id,
        notif_msg, "manager", user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine.lower()}/plany?success={success_key}", status_code=303)


@router.post("/kierownik/przenies-zlecenie")
def kierownik_przenies_zlecenie(
    request: Request,
    plan_id: int = Form(...),
    source_machine: str = Form(...),
    target_machine: str = Form(...),
    user=Depends(require_manager_or_admin),
    conn=Depends(get_db),
):
    src = source_machine.strip().upper()
    tgt = target_machine.strip().upper()
    if tgt not in PRODUCTION_MACHINES or src not in PRODUCTION_MACHINES:
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=niewlasciwa_maszyna", status_code=303)
    if src == tgt:
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=ta_sama_maszyna", status_code=303)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, src))
    plan = cur.fetchone()
    if not plan:
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=brak_zlecenia", status_code=303)
    if plan["status"] != "planned":
        return RedirectResponse(f"/maszyna/{src.lower()}/plany?error=tylko_planowane", status_code=303)
    cur.execute(
        "UPDATE production_plans SET machine=?, assortment_prep_status='pending', farby_prep_status='pending', polimery_prep_status='pending' WHERE id=?",
        (tgt, plan_id),
    )
    log_domain_event(cur, "PLAN_MOVED", user["username"], tgt, plan_id, plan["lub_number"], f"z {src} na {tgt}")
    log_production_operation(
        cur, "przeniesienie_zlecenia",
        f"Zlecenie {plan['order_number']} przeniesione z {src} na {tgt}",
        tgt, plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{tgt.lower()}/plany?success=przeniesiono", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}")
def maszyna_job(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    status: str = Query(""),
    message: str = Query(""),
    finalize: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        print(f"[WARN] Drukarz {user['username']} próbował wejść na maszynę {machine}, ale ma przypisaną {request.session.get('machine')}")
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
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
    cur.execute(
        "SELECT code, label FROM problem_categories WHERE is_active=1 ORDER BY sort_order, label"
    )
    problem_categories = cur.fetchall()
    cur.execute(
        "SELECT * FROM print_control_reports WHERE plan_id=? ORDER BY created_at DESC, id DESC LIMIT 12",
        (plan_id,),
    )
    print_reports = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT * FROM production_reports WHERE plan_id=? ORDER BY created_at DESC, id DESC LIMIT 1",
        (plan_id,),
    )
    production_report = cur.fetchone()
    prep_ready = plan["farby_prep_status"] == "ready" and plan["polimery_prep_status"] == "ready"
    active_job = plan["status"] == "in_progress"
    completed_job = plan["status"] == "completed"
    return render_template("maszyna_job.html", {
        "machine": machine.upper(),
        "plan": plan,
        "farby": farby,
        "polimery": polimery,
        "problem_categories": problem_categories,
        "print_reports": print_reports,
        "production_report": dict(production_report) if production_report else None,
        "prep_ready": prep_ready,
        "active_job": active_job,
        "completed_job": completed_job,
        "auto_open_final_report": finalize == "1" and active_job,
        "user": {"username": user["username"], "role": user["role"]},
        "status": status,
        "message": message,
    })


@router.get("/maszyna/{machine}/job/{plan_id}/raport-zadruku")
def maszyna_job_raport_zadruku(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "REPORT_PRINT_CALLED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "REPORT_PRINT_CALLED", machine.upper(), plan_id,
        f"Raport zadruku wywołany na {machine.upper()} dla {plan['order_number']}", "manager", user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Raport+zadruku+wysłany", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/raport-produkcji")
def maszyna_job_raport_produkcji(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "REPORT_PRODUCTION_CALLED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "REPORT_PRODUCTION_CALLED", machine.upper(), plan_id,
        f"Raport produkcji wywołany na {machine.upper()} dla {plan['order_number']}", "manager", user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Raport+produkcji+wysłany", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/problem")
def maszyna_job_problem(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    message = f"Problem zgłoszony na {machine.upper()} dla zlecenia {plan['order_number']}"
    log_domain_event(cur, "PROBLEM_REPORT", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(cur, "PROBLEM_REPORT", machine.upper(), plan_id, message, "manager", user["username"])
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=warning&message=Problem+zgłoszony", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/call-manager")
def maszyna_call_manager(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "CALL_MANAGER", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_MANAGER", machine.upper(), plan_id,
        f"Wezwanie kierownika na {machine.upper()} dla {plan['order_number']}", "manager", user["username"],
    )
    log_production_operation(
        cur, "wezwanie",
        f"Wezwanie kierownika na {machine.upper()} dla {plan['order_number']}",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Kierownik+został+wezwany", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/call-mieszalnia")
def maszyna_call_mieszalnia(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "CALL_MIXING", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_MIXING", machine.upper(), plan_id,
        f"Wezwanie operatora mieszalni na {machine.upper()} dla {plan['order_number']}", "operator_mieszalni", user["username"],
    )
    log_production_operation(
        cur, "wezwanie",
        f"Wezwanie operatora mieszalni na {machine.upper()} dla {plan['order_number']}",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Operator+mieszalni+został+wezany", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/call-prepress")
def maszyna_call_prepress(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    log_domain_event(cur, "CALL_PREPRESS", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_PREPRESS", machine.upper(), plan_id,
        f"Wezwanie prepress na {machine.upper()} dla {plan['order_number']}", "prepress", user["username"],
    )
    log_production_operation(
        cur, "wezwanie",
        f"Wezwanie prepress na {machine.upper()} dla {plan['order_number']}",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Prepress+został+wezany", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/start")
def maszyna_job_start(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    if plan["status"] == "in_progress":
        return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Zlecenie+jest+już+w+trakcie+realizacji", status_code=303)
    if plan["status"] == "completed":
        return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=info&message=Zlecenie+zostało+już+zakończone", status_code=303)
    prep_ready = plan["farby_prep_status"] == "ready" and plan["polimery_prep_status"] == "ready"
    if not prep_ready and request.query_params.get("confirm") != "1":
        return RedirectResponse(
            f"/maszyna/{machine}/job/{plan_id}?status=warning&message=Brak+pełnego+przygotowania+asortymentu.+Potwierdź+rozpoczęcie+zlecenia",
            status_code=303,
        )
    cur.execute("UPDATE production_plans SET status='in_progress' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_STARTED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_STARTED", machine.upper(), plan_id,
        f"Rozpoczęcie zlecenia na {machine.upper()} dla {plan['order_number']}", "manager", user["username"],
    )
    log_production_operation(
        cur, "rozpoczecie_zlecenia",
        f"Zlecenie rozpoczete {plan['order_number']} na {machine.upper()}",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Zlecenie+rozpoczęte.+Możesz+prowadzić+kontrolę+zadruku+i+pracę+na+aktywnym+widoku", status_code=303)


@router.get("/maszyna/{machine}/job/{plan_id}/complete")
def maszyna_job_complete(
    machine: str, plan_id: int, request: Request,
    user=Depends(require_auth), conn=Depends(get_db),
):
    if user["role"] == "drukarz" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej maszyny dla obecnego drukarza", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    if plan["status"] != "in_progress":
        return RedirectResponse(
            f"/maszyna/{machine}/job/{plan_id}?status=warning&message=Najpierw+rozpocznij+zlecenie,+aby+je+finalizować",
            status_code=303,
        )
    return RedirectResponse(
        f"/maszyna/{machine}/job/{plan_id}?finalize=1&status=info&message=Aby+zakończyć+zlecenie,+wypełnij+raport+produkcji",
        status_code=303,
    )


@router.post("/maszyna/{machine}/job/{plan_id}/submit-report")
def submit_report(
    machine: str,
    plan_id: int,
    request: Request,
    report_type: str = Form(...),
    report_date: str = Form(...),
    shift: str = Form(...),
    job_number: str = Form(...),
    status: str = Form(...),
    notes: str = Form(""),
    problem_categories: list[str] = Form([]),
    problem_short_note: str = Form(""),
    ok_quantity: int = Form(0),
    nok_quantity: int = Form(0),
    quantity: int = Form(0),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ["admin", "manager", "drukarz", "operator_mieszalni", "prepress"]:
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=?", (plan_id,))
    plan_row = cur.fetchone()
    if not plan_row:
        return HTMLResponse("Zlecenie nie znalezione", status_code=404)
    lub = plan_row["lub_number"] if plan_row else None
    dpart = (report_date.split("T")[0] if "T" in report_date else report_date)[:10]
    tpart = report_date.split("T")[1][:8] if "T" in report_date else local_time_str()
    shift_norm = normalize_shift_label(shift)
    if report_type == "print_control":
        if plan_row["status"] != "in_progress":
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/job/{plan_id}?status=warning&message=Raport+kontroli+zadruku+jest+dostępny+dopiero+po+rozpoczęciu+zlecenia",
                status_code=303,
            )
        cur.execute(
            "INSERT INTO print_control_reports (machine, date, time, job_number, status, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (machine.upper(), dpart, tpart, job_number, status, notes, user["username"], plan_id),
        )
        log_production_operation(cur, "raport_zadruku", f"Raport kontroli zadruku: {job_number} [{status}]", machine.upper(), plan_id, user["username"])
        log_domain_event(cur, "RAPORT_ZADRUKU_ZAPISANY", user["username"], machine.upper(), plan_id, lub)
        insert_notification_if_enabled(
            cur, "RAPORT_ZADRUKU_ZAPISANY", machine.upper(), plan_id,
            f"Raport kontroli zadruku: {job_number} na {machine.upper()} — {status}", "manager", user["username"],
        )
    else:
        if plan_row["status"] != "in_progress":
            return RedirectResponse(
                f"/maszyna/{machine.lower()}/job/{plan_id}?status=warning&message=Raport+produkcji+możesz+wypełnić+tylko+podczas+finalizacji+aktywnego+zlecenia",
                status_code=303,
            )
        cur.execute(
            "INSERT INTO production_reports (machine, date, shift, job_number, start_time, end_time, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (machine.upper(), dpart, shift_norm, job_number, tpart, tpart, quantity, ok_quantity, nok_quantity, notes, user["username"], plan_id),
        )
        report_id = cur.lastrowid
        selected_problem_codes = [code for code in problem_categories if code]
        if report_id and selected_problem_codes:
            cur.execute(
                "SELECT id, code FROM problem_categories WHERE is_active=1"
            )
            category_map = {row["code"]: row["id"] for row in cur.fetchall()}
            issue_note = problem_short_note.strip()
            issue_rows = []
            for code in selected_problem_codes:
                category_id = category_map.get(code)
                if category_id is None:
                    continue
                issue_rows.append((
                    report_id,
                    category_id,
                    machine.upper(),
                    plan_id,
                    user["username"],
                    issue_note,
                ))
            for row in issue_rows:
                cur.execute(
                    """
                    INSERT INTO production_report_issues
                    (production_report_id, problem_category_id, machine, plan_id, reported_by, short_note)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        log_production_operation(cur, "raport_produkcji", f"Raport produkcji: {job_number} qty {quantity} OK {ok_quantity} NOK {nok_quantity}", machine.upper(), plan_id, user["username"])
        log_domain_event(cur, "RAPORT_PRODUKCJI_ZAPISANY", user["username"], machine.upper(), plan_id, lub)
        insert_notification_if_enabled(
            cur, "RAPORT_PRODUKCJI_ZAPISANY", machine.upper(), plan_id,
            f"Raport produkcji: {job_number} na {machine.upper()} — szt. {quantity}, OK {ok_quantity}, NOK {nok_quantity}", "manager", user["username"],
        )
        _finalize_job_completion(cur, machine, plan_row, plan_id, user["username"])
    conn.commit()
    if user["role"] in ("manager", "admin"):
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={dpart}", status_code=303)
    return RedirectResponse(
        f"/maszyna/{machine.lower()}/job/{plan_id}?status=success&message=Raport+zapisany.+Kierownik+widzi+go+w+Rejestrze+raportów.",
        status_code=303,
    )


@router.get("/maszyna/{machine}/export-csv")
def export_plany_csv(machine: str, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """SELECT order_number, artwork_number, lub_number, order_name, laminate, meters, pieces
           FROM production_plans WHERE machine=? AND status='planned' ORDER BY id""",
        (machine.upper(),),
    )
    plans = cur.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["numer_zlecenia", "numer_artwork", "numer_lub", "nazwa_zlecenia", "laminat", "ilosc_metrow", "ilosc_sztuk"])
    for plan in plans:
        writer.writerow(plan)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=plany_{machine.lower()}.csv"},
    )


@router.get("/maszyna/{machine}/raport-zadruku")
def maszyna_raport_zadruku(machine: str, request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    start_utc, end_utc = local_day_bounds_utc()
    cur.execute(
        "SELECT * FROM print_control_reports WHERE machine=? AND created_at >= ? AND created_at < ? ORDER BY created_at DESC LIMIT 20",
        (machine.upper(), start_utc, end_utc),
    )
    reports = cur.fetchall()
    return render_template("maszyna_raport_zadruku.html", {
        "machine": machine.upper(),
        "reports": reports,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.post("/maszyna/{machine}/raport-zadruku")
def maszyna_dodaj_raport_zadruku(
    machine: str,
    request: Request,
    job_number: str = Form(...),
    status: str = Form(...),
    notes: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    plan_id = resolve_plan_id_for_job(cur, machine, job_number)
    cur.execute(
        "INSERT INTO print_control_reports (machine, date, time, job_number, status, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (machine.upper(), local_date_str(), local_time_str(), job_number, status, notes, user["username"], plan_id),
    )
    log_production_operation(cur, "raport_zadruku", f"[Panel maszyny] {job_number} [{status}]", machine.upper(), plan_id, user["username"])
    log_domain_event(cur, "RAPORT_ZADRUKU_ZAPISANY", user["username"], machine.upper(), plan_id, None)
    insert_notification_if_enabled(
        cur, "RAPORT_ZADRUKU_ZAPISANY", machine.upper(), plan_id,
        f"Raport zadruku (panel maszyny {machine.upper()}): {job_number} — {status}", "manager", user["username"],
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Raport zadruku dodany"})
    if user["role"] in ("manager", "admin"):
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={local_today().strftime('%Y-%m-%d')}", status_code=303)
    return RedirectResponse(f"/maszyna/{machine}/raport-zadruku?success=dodano", status_code=303)


@router.get("/maszyna/{machine}/raport-produkcji")
def maszyna_raport_produkcji(machine: str, request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    start_utc, end_utc = local_day_bounds_utc()
    cur.execute(
        "SELECT * FROM production_reports WHERE machine=? AND created_at >= ? AND created_at < ? ORDER BY created_at DESC LIMIT 20",
        (machine.upper(), start_utc, end_utc),
    )
    reports = cur.fetchall()
    return render_template("maszyna_raport_produkcji.html", {
        "machine": machine.upper(),
        "reports": reports,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.post("/maszyna/{machine}/raport-produkcji")
def maszyna_dodaj_raport_produkcji(
    machine: str,
    request: Request,
    job_number: str = Form(...),
    quantity: int = Form(...),
    ok_quantity: int = Form(...),
    nok_quantity: int = Form(...),
    notes: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    plan_id = resolve_plan_id_for_job(cur, machine, job_number)
    current_time = local_time_str()
    cur.execute(
        "INSERT INTO production_reports (machine, date, shift, job_number, start_time, end_time, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id) VALUES (?, ?, 'dzien', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (machine.upper(), local_date_str(), job_number, current_time, current_time, quantity, ok_quantity, nok_quantity, notes, user["username"], plan_id),
    )
    log_production_operation(cur, "raport_produkcji", f"[Panel maszyny] {job_number} qty {quantity} OK {ok_quantity} NOK {nok_quantity}", machine.upper(), plan_id, user["username"])
    log_domain_event(cur, "RAPORT_PRODUKCJI_ZAPISANY", user["username"], machine.upper(), plan_id, None)
    insert_notification_if_enabled(
        cur, "RAPORT_PRODUKCJI_ZAPISANY", machine.upper(), plan_id,
        f"Raport produkcji (panel maszyny {machine.upper()}): {job_number} — {quantity} szt., OK {ok_quantity}, NOK {nok_quantity}", "manager", user["username"],
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Raport produkcji dodany"})
    if user["role"] in ("manager", "admin"):
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={local_today().strftime('%Y-%m-%d')}", status_code=303)
    return RedirectResponse(f"/maszyna/{machine}/raport-produkcji?success=dodano", status_code=303)


@router.get("/maszyna/{machine}")
def podglad_maszyna(machine: str, request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """SELECT id, order_number, artwork_number, lub_number, order_name, laminate, meters, pieces, status
           FROM production_plans WHERE machine=? AND status='planned' ORDER BY id""",
        (machine.upper(),),
    )
    plans = cur.fetchall()
    return render_template("maszyna_podglad.html", {
        "machine": machine.upper(),
        "plans": plans,
        "user": {"username": user["username"], "role": user["role"]},
    })
