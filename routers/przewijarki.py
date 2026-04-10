"""
Router: przewijarki P1 i P2 — widoki, plany, raporty przewijania.
"""
from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.requests import Request

from dependencies import get_db, require_auth
from helpers import (
    WINDING_MACHINES,
    insert_notification_if_enabled,
    log_domain_event,
    log_production_operation,
    render_template,
)
from time_utils import local_today

router = APIRouter()

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
    return render_template("przewijarka_job.html", {
        "machine": machine.upper(),
        "plan": dict(plan),
        "reports": reports,
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
