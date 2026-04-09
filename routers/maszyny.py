"""
Router: maszyny produkcyjne — widoki, plany, zlecenia, raporty, akcje operatorów.
"""
import csv
import io
from datetime import date

from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from starlette.requests import Request

from dependencies import get_db, is_ajax, require_auth, require_manager_or_admin
from helpers import (
    PRODUCTION_MACHINES,
    enrich_plans_with_lub_materials,
    get_lub_farby,
    insert_notification_if_enabled,
    log_domain_event,
    log_production_operation,
    normalize_shift_label,
    render_template,
    resolve_plan_id_for_job,
)

router = APIRouter()


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
    return RedirectResponse(f"/maszyna/{machine.lower()}/plany", status_code=303)


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
    plans = enrich_plans_with_lub_materials(cur, plan_rows)
    can_move = user["role"] in ("admin", "manager")
    other_machines = [m for m in PRODUCTION_MACHINES if m != machine.upper()]
    prep_ui = user["role"] in ("operator_mieszalni", "prepress")
    show_prep_column = user["role"] in ("drukarz", "manager", "admin", "operator_mieszalni", "prepress")
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
    return render_template("maszyna_job.html", {
        "machine": machine.upper(),
        "plan": plan,
        "farby": farby,
        "polimery": polimery,
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
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Zlecenie+rozpoczęte", status_code=303)


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
    cur.execute("UPDATE production_plans SET status='completed' WHERE id=?", (plan_id,))
    log_domain_event(cur, "JOB_COMPLETED", user["username"], machine.upper(), plan_id, plan["lub_number"])
    insert_notification_if_enabled(
        cur, "JOB_COMPLETED", machine.upper(), plan_id,
        f"Ukończenie zlecenia na {machine.upper()} dla {plan['order_number']}", "manager", user["username"],
    )
    log_production_operation(
        cur, "zakonczenie_zlecenia",
        f"Zlecenie zakonczone {plan['order_number']} na {machine.upper()}",
        machine.upper(), plan_id, user["username"],
    )
    conn.commit()
    return RedirectResponse(f"/maszyna/{machine}/job/{plan_id}?status=success&message=Zlecenie+zakończone", status_code=303)


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
    lub = plan_row["lub_number"] if plan_row else None
    dpart = (report_date.split("T")[0] if "T" in report_date else report_date)[:10]
    tpart = report_date.split("T")[1][:8] if "T" in report_date else __import__("datetime").datetime.now().strftime("%H:%M:%S")
    shift_norm = normalize_shift_label(shift)
    if report_type == "print_control":
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
        cur.execute(
            "INSERT INTO production_reports (machine, date, shift, job_number, start_time, end_time, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (machine.upper(), dpart, shift_norm, job_number, tpart, tpart, quantity, ok_quantity, nok_quantity, notes, user["username"], plan_id),
        )
        log_production_operation(cur, "raport_produkcji", f"Raport produkcji: {job_number} qty {quantity} OK {ok_quantity} NOK {nok_quantity}", machine.upper(), plan_id, user["username"])
        log_domain_event(cur, "RAPORT_PRODUKCJI_ZAPISANY", user["username"], machine.upper(), plan_id, lub)
        insert_notification_if_enabled(
            cur, "RAPORT_PRODUKCJI_ZAPISANY", machine.upper(), plan_id,
            f"Raport produkcji: {job_number} na {machine.upper()} — szt. {quantity}, OK {ok_quantity}, NOK {nok_quantity}", "manager", user["username"],
        )
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
    cur.execute(
        "SELECT * FROM print_control_reports WHERE machine=? AND date(created_at)=date('now') ORDER BY created_at DESC LIMIT 20",
        (machine.upper(),),
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
        "INSERT INTO print_control_reports (machine, date, time, job_number, status, notes, created_by, plan_id) VALUES (?, date('now'), time('now'), ?, ?, ?, ?, ?)",
        (machine.upper(), job_number, status, notes, user["username"], plan_id),
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
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={date.today().strftime('%Y-%m-%d')}", status_code=303)
    return RedirectResponse(f"/maszyna/{machine}/raport-zadruku?success=dodano", status_code=303)


@router.get("/maszyna/{machine}/raport-produkcji")
def maszyna_raport_produkcji(machine: str, request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM production_reports WHERE machine=? AND date(created_at)=date('now') ORDER BY created_at DESC LIMIT 20",
        (machine.upper(),),
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
    cur.execute(
        "INSERT INTO production_reports (machine, date, shift, job_number, start_time, end_time, quantity, ok_quantity, nok_quantity, notes, created_by, plan_id) VALUES (?, date('now'), 'dzien', ?, time('now'), time('now'), ?, ?, ?, ?, ?, ?)",
        (machine.upper(), job_number, quantity, ok_quantity, nok_quantity, notes, user["username"], plan_id),
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
        return RedirectResponse(f"/kierownik/rejestr-raportow?date={date.today().strftime('%Y-%m-%d')}", status_code=303)
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
