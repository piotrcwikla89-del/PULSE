"""
Router: przewijarki P1 i P2 — widoki, plany, raporty przewijania.
"""
from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.requests import Request

from dependencies import get_db, require_auth
from helpers import (
    WINDING_MACHINES,
    find_pending_machine_handover,
    insert_notification_if_enabled,
    log_domain_event,
    log_production_operation,
    normalize_shift_label,
    render_template,
)
from time_utils import local_today

router = APIRouter()


def _resolve_shift_ids(cur, outgoing_shift: str) -> tuple[int | None, int | None, str, str]:
    outgoing_norm = normalize_shift_label(outgoing_shift)
    incoming_norm = "noc" if outgoing_norm == "dzien" else "dzien"
    cur.execute("SELECT id, name FROM shifts")
    shift_ids = {normalize_shift_label(row["name"]): row["id"] for row in cur.fetchall()}
    return shift_ids.get(outgoing_norm), shift_ids.get(incoming_norm), outgoing_norm, incoming_norm


def _load_winding_handover_snapshot(cur, machine: str, report_date: str, shift_norm: str) -> list[dict]:
    cur.execute(
        """
        SELECT wr.plan_id, wr.order_number AS job_number, SUM(wr.cut_meters) AS cut_meters,
               SUM(wr.ok_meters) AS ok_meters, SUM(wr.nok_meters) AS nok_meters,
               MAX(wr.created_at) AS last_report_at, pp.order_name, pp.lub_number
        FROM winding_reports wr
        LEFT JOIN production_plans pp ON pp.id = wr.plan_id
        WHERE wr.machine=? AND wr.date=? AND wr.shift=? AND COALESCE(pp.status, '')='completed'
        GROUP BY wr.plan_id, wr.order_number, pp.order_name, pp.lub_number
        ORDER BY MAX(wr.created_at) DESC, wr.order_number
        """,
        (machine.upper(), report_date, shift_norm),
    )
    return [dict(row) for row in cur.fetchall()]

# ==================== WYBÓR MASZYNY ====================

@router.get("/select-przewijarka")
def select_przewijarka_form(request: Request, user=Depends(require_auth)):
    if user["role"] != "operator_przewijarki":
        return RedirectResponse("/dashboard", status_code=303)
    return render_template("select_przewijarka.html", {"user": user})


@router.post("/select-przewijarka")
def select_przewijarka(
    request: Request,
    machine: str = Form(...),
    user=Depends(require_auth),
):
    if user["role"] != "operator_przewijarki":
        return RedirectResponse("/dashboard", status_code=303)
    if machine.upper() not in WINDING_MACHINES:
        return RedirectResponse("/select-przewijarka", status_code=303)
    request.session["machine"] = machine.upper()
    return RedirectResponse(f"/przewijarka/{machine.lower()}/plany", status_code=303)


# ==================== PLANY PRZEWIJARKI ====================

@router.get("/przewijarka/{machine}/plany")
def przewijarka_plany(
    machine: str,
    request: Request,
    user=Depends(require_auth),
    success: str = Query(""),
    error: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_przewijarki", "admin", "manager"):
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "operator_przewijarki" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej przewijarki.", status_code=403)
    if machine.upper() not in WINDING_MACHINES:
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    if user["role"] == "operator_przewijarki":
        pending_handover = find_pending_machine_handover(cur, machine)
        if pending_handover:
            return RedirectResponse(
                f"/przewijarka/{machine.lower()}/przekazanie-zmiany/odbior?handover_id={pending_handover['id']}",
                status_code=303,
            )
    cur.execute(
        "SELECT * FROM production_plans WHERE machine=? AND status='planned' ORDER BY id",
        (machine.upper(),),
    )
    plans = [dict(p) for p in cur.fetchall()]
    return render_template("przewijarka_plany.html", {
        "machine": machine.upper(),
        "plans": plans,
        "user": {"username": user["username"], "role": user["role"]},
        "success_msg": success,
        "error_msg": error,
    })


# ==================== WIDOK ZLECENIA ====================

@router.get("/przewijarka/{machine}/job/{plan_id}")
def przewijarka_job(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    status: str = Query(""),
    message: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_przewijarki", "admin", "manager"):
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "operator_przewijarki" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej przewijarki.", status_code=403)
    if machine.upper() not in WINDING_MACHINES:
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM production_plans WHERE id=? AND machine=?",
        (plan_id, machine.upper()),
    )
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione lub nie należy do tej maszyny.", status_code=404)
    # Pobierz raporty tego zlecenia
    cur.execute(
        "SELECT * FROM winding_reports WHERE plan_id=? ORDER BY created_at DESC",
        (plan_id,),
    )
    reports = [dict(r) for r in cur.fetchall()]
    active_job = plan["status"] == "in_progress"
    completed_job = plan["status"] == "completed"
    return render_template("przewijarka_job.html", {
        "machine": machine.upper(),
        "plan": dict(plan),
        "reports": reports,
        "active_job": active_job,
        "completed_job": completed_job,
        "user": {"username": user["username"], "role": user["role"]},
        "status": status,
        "message": message,
        "today": local_today().isoformat(),
    })


# ==================== WEZWANIE KIEROWNIKA ====================

@router.get("/przewijarka/{machine}/job/{plan_id}/call-manager")
def przewijarka_call_manager(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_przewijarki", "admin", "manager"):
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "operator_przewijarki" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu.", status_code=403)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM production_plans WHERE id=? AND machine=?",
        (plan_id, machine.upper()),
    )
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione.", status_code=404)
    log_domain_event(cur, "CALL_MANAGER", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "CALL_MANAGER", machine.upper(), plan_id,
        f"Wezwanie kierownika na {machine.upper()} (przewijarka) dla {plan['order_number']}",
        "manager", user["username"],
    )
    log_production_operation(
        cur, "wezwanie",
        f"Wezwanie kierownika na {machine.upper()} (przewijarka) dla {plan['order_number']}",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(
        f"/przewijarka/{machine.lower()}/job/{plan_id}?status=info&message=Kierownik+zostal+wezwany",
        status_code=303,
    )


# ==================== RAPORT PRZEWIJANIA ====================

@router.post("/przewijarka/{machine}/job/{plan_id}/submit-winding-report")
def submit_winding_report(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
    report_date: str = Form(...),
    shift: str = Form(...),
    cut_meters: float = Form(...),
    ok_meters: float = Form(...),
    nok_meters: float = Form(...),
    notes: str = Form(""),
):
    if user["role"] not in ("operator_przewijarki", "admin", "manager"):
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "operator_przewijarki" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu.", status_code=403)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM production_plans WHERE id=? AND machine=?",
        (plan_id, machine.upper()),
    )
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione.", status_code=404)
    cur.execute(
        """
        INSERT INTO winding_reports
            (machine, plan_id, date, shift, order_number, cut_meters, ok_meters, nok_meters, notes, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            machine.upper(), plan_id, report_date, shift,
            plan["order_number"], cut_meters, ok_meters, nok_meters,
            notes.strip(), user["username"],
        ),
    )
    log_production_operation(
        cur, "raport_przewijania",
        f"Raport przewijania dla {plan['order_number']} na {machine.upper()}: "
        f"{cut_meters}m wycięte, OK={ok_meters}m, NOK={nok_meters}m",
        machine.upper(), plan_id, user["username"],
    )
    log_domain_event(
        cur, "WINDING_REPORT_SUBMITTED", user["username"],
        machine.upper(), plan_id, plan["lub_number"],
        f"cut={cut_meters} ok={ok_meters} nok={nok_meters}",
    )
    insert_notification_if_enabled(
        cur, "WINDING_REPORT_SUBMITTED", machine.upper(), plan_id,
        f"Raport przewijania: {plan['order_number']} na {machine.upper()} — OK {ok_meters}m, NOK {nok_meters}m",
        "manager", user["username"],
    )
    conn.commit()
    return RedirectResponse(
        f"/przewijarka/{machine.lower()}/job/{plan_id}?status=success&message=Raport+zapisany",
        status_code=303,
    )


# ==================== START / ZAKOŃCZENIE ====================

@router.get("/przewijarka/{machine}/job/{plan_id}/start")
def przewijarka_job_start(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_przewijarki", "admin", "manager"):
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "operator_przewijarki" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu.", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione.", status_code=404)
    cur.execute("UPDATE production_plans SET status='in_progress' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_STARTED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_STARTED", machine.upper(), plan_id,
        f"Rozpoczęcie zlecenia na {machine.upper()} (przewijarka) dla {plan['order_number']}",
        "manager", user["username"],
    )
    log_production_operation(
        cur, "rozpoczecie_zlecenia",
        f"Zlecenie {plan['order_number']} rozpoczęte na {machine.upper()} (przewijarka)",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(
        f"/przewijarka/{machine.lower()}/job/{plan_id}?status=success&message=Zlecenie+rozpoczete",
        status_code=303,
    )


@router.get("/przewijarka/{machine}/job/{plan_id}/complete")
def przewijarka_job_complete(
    machine: str,
    plan_id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] not in ("operator_przewijarki", "admin", "manager"):
        return RedirectResponse("/dashboard", status_code=303)
    if user["role"] == "operator_przewijarki" and request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu.", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM production_plans WHERE id=? AND machine=?", (plan_id, machine.upper()))
    plan = cur.fetchone()
    if not plan:
        return HTMLResponse("Zlecenie nie znalezione.", status_code=404)
    cur.execute("UPDATE production_plans SET status='completed' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_COMPLETED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_COMPLETED", machine.upper(), plan_id,
        f"Ukończenie zlecenia na {machine.upper()} (przewijarka) dla {plan['order_number']}",
        "manager", user["username"],
    )
    log_production_operation(
        cur, "zakonczenie_zlecenia",
        f"Zlecenie {plan['order_number']} zakończone na {machine.upper()} (przewijarka)",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(
        f"/przewijarka/{machine.lower()}/job/{plan_id}?status=success&message=Zlecenie+zakonczone",
        status_code=303,
    )


@router.get("/przewijarka/{machine}/przekazanie-zmiany")
def przewijarka_przekazanie_zmiany(
    machine: str,
    request: Request,
    user=Depends(require_auth),
    report_date: str = Query("", alias="date"),
    shift: str = Query("dzien"),
    success: str = Query(""),
    conn=Depends(get_db),
):
    if user["role"] != "operator_przewijarki":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej przewijarki.", status_code=403)
    date_q = report_date or local_today().strftime("%Y-%m-%d")
    cur = conn.cursor()
    outgoing_shift_id, incoming_shift_id, shift_norm, incoming_shift = _resolve_shift_ids(cur, shift)
    completed_jobs = _load_winding_handover_snapshot(cur, machine, date_q, shift_norm)
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
    return render_template("przewijarka_przekazanie_zmiany.html", {
        "machine": machine.upper(),
        "user": {"username": user["username"], "role": user["role"]},
        "date_q": date_q,
        "shift": shift_norm,
        "incoming_shift": incoming_shift,
        "completed_jobs": completed_jobs,
        "existing_handover": existing_handover,
        "existing_items": existing_items,
        "success": success,
    })


@router.post("/przewijarka/{machine}/przekazanie-zmiany")
def przewijarka_zapisz_przekazanie_zmiany(
    machine: str,
    request: Request,
    report_date: str = Form(...),
    shift: str = Form(...),
    summary_comment: str = Form(""),
    action: str = Form("send"),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] != "operator_przewijarki":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej przewijarki.", status_code=403)
    cur = conn.cursor()
    outgoing_shift_id, incoming_shift_id, shift_norm, _incoming_shift = _resolve_shift_ids(cur, shift)
    if not outgoing_shift_id or not incoming_shift_id:
        return RedirectResponse(
            f"/przewijarka/{machine.lower()}/przekazanie-zmiany?date={report_date}&shift={shift_norm}",
            status_code=303,
        )
    completed_jobs = _load_winding_handover_snapshot(cur, machine, report_date, shift_norm)
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
            f"/przewijarka/{machine.lower()}/przekazanie-zmiany?date={report_date}&shift={shift_norm}&success=draft",
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
        details = f"Wycięte: {job['cut_meters'] or 0}m, OK: {job['ok_meters'] or 0}m, NOK: {job['nok_meters'] or 0}m"
        cur.execute(
            """
            INSERT INTO shift_handover_items
            (handover_id, item_type, plan_id, job_number, machine, lub_number, title, details, status, sort_order)
            VALUES (?, 'completed_job', ?, ?, ?, ?, ?, ?, 'done', ?)
            """,
            (handover_id, job.get("plan_id"), job.get("job_number"), machine.upper(), job.get("lub_number"), title, details, index),
        )

    log_production_operation(
        cur,
        "przekazanie_zmiany",
        f"Przekazanie zmiany dla {machine.upper()} ({shift_norm}) — zlecenia: {len(completed_jobs)}",
        machine.upper(),
        None,
        user["username"],
    )
    log_domain_event(cur, "SHIFT_HANDOVER_CREATED", user["username"], machine.upper(), None, None, f"{report_date}:{shift_norm}")
    conn.commit()
    return RedirectResponse(
        f"/przewijarka/{machine.lower()}/przekazanie-zmiany?date={report_date}&shift={shift_norm}&success=sent",
        status_code=303,
    )


@router.get("/przewijarka/{machine}/przekazanie-zmiany/odbior")
def przewijarka_odbior_przekazania_zmiany(
    machine: str,
    request: Request,
    handover_id: int = Query(0),
    success: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] != "operator_przewijarki":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej przewijarki.", status_code=403)
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
        return RedirectResponse(f"/przewijarka/{machine.lower()}/plany", status_code=303)
    cur.execute(
        "SELECT * FROM shift_handover_items WHERE handover_id=? ORDER BY item_type, sort_order, id",
        (handover["id"],),
    )
    completed_items = [dict(row) for row in cur.fetchall() if row["item_type"] == "completed_job"]
    return render_template("przewijarka_odbior_przekazania.html", {
        "machine": machine.upper(),
        "user": {"username": user["username"], "role": user["role"]},
        "handover": handover,
        "completed_items": completed_items,
        "success": success,
    })


@router.post("/przewijarka/{machine}/przekazanie-zmiany/odbior")
def przewijarka_potwierdz_odbior_przekazania(
    machine: str,
    request: Request,
    handover_id: int = Form(...),
    acknowledgement_note: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if user["role"] != "operator_przewijarki":
        return RedirectResponse("/dashboard", status_code=303)
    if request.session.get("machine") != machine.upper():
        return HTMLResponse("Brak dostępu do tej przewijarki.", status_code=403)
    cur = conn.cursor()
    cur.execute("SELECT * FROM shift_handovers WHERE id=? AND machine=?", (handover_id, machine.upper()))
    handover = cur.fetchone()
    if not handover or handover["status"] != "waiting_ack":
        return RedirectResponse(f"/przewijarka/{machine.lower()}/plany", status_code=303)
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
    return RedirectResponse(f"/przewijarka/{machine.lower()}/plany?success=przejecie_zmiany", status_code=303)
