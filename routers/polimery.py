"""
Router: polimery (matryce) — widok główny i wszystkie akcje.
"""
from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.requests import Request

from db_compat import INTEGRITY_ERRORS
from dependencies import get_db, is_ajax, require_auth
from helpers import log_production_operation, render_template

router = APIRouter()


@router.get("/polimery")
def polimery(
    request: Request,
    search_field: str = Query("lub"),
    search_value: str = Query(""),
    status: str = Query(""),
    sort: str = Query("status"),
    dir: str = Query("asc"),
    error: str = Query(""),
    success: str = Query(""),
    assign_lub: str = Query(""),
    plan_id: int = Query(0),
    assign_machine: str = Query(""),
    return_to: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers")
    dane = cur.fetchall()

    polimery_list = []
    for p in dane:
        if search_value:
            if search_field == "lub" and search_value.lower() not in (p["lub"] or "").lower():
                continue
            elif search_field == "kolor" and search_value.lower() not in (p["kolor"] or "").lower():
                continue
            elif search_field == "lokalizacja" and search_value.lower() not in (p["lokalizacja"] or "").lower():
                continue
        if status and p["status"] != status:
            continue
        polimery_list.append(dict(p))

    reverse = dir == "desc"
    if sort == "status":
        order = {"dostepna": 1, "w_uzyciu": 2, "uszkodzona": 3, "zutylizowana": 4}
        polimery_list.sort(key=lambda x: order.get(x["status"], 9), reverse=reverse)
    else:
        polimery_list.sort(key=lambda x: (x.get(sort) or ""), reverse=reverse)

    # Pobierz dane zlecenia jeśli jesteśmy w trybie przypisywania
    assign_plan = None
    if assign_lub and plan_id:
        cur.execute("SELECT order_number, order_name FROM production_plans WHERE id=?", (plan_id,))
        row = cur.fetchone()
        if row:
            assign_plan = dict(row)

    return render_template("polimery.html", {
        "polimery": polimery_list,
        "search_field": search_field,
        "search_value": search_value,
        "status": status,
        "sort": sort,
        "dir": dir,
        "user": {"username": user["username"], "role": user["role"]},
        "error": error,
        "success": success,
        "assign_lub": assign_lub,
        "plan_id": plan_id,
        "assign_machine": assign_machine,
        "return_to": return_to,
        "assign_plan": assign_plan,
    })


@router.post("/dodaj_polimer")
def dodaj_polimer(
    request: Request,
    lub: str = Form(...),
    kolor: str = Form(...),
    lokalizacja: str = Form(""),
    data_waznosci: str = Form(""),
    uwagi: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO polymers (lub, kolor, status, lokalizacja, data_waznosci, uwagi) VALUES (?, ?, 'dostepna', ?, ?, ?)",
            (lub, kolor, lokalizacja, data_waznosci if data_waznosci else None, uwagi),
        )
        conn.commit()
        new_id = cur.lastrowid
        cur.execute(
            "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('przyjęcie', ?, ?, ?)",
            (new_id, lokalizacja, f"Dodano: LUB={lub}, kolor={kolor}"),
        )
        conn.commit()
    except INTEGRITY_ERRORS:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "Błąd bazy danych"}, status_code=400)
        return RedirectResponse("/polimery?error=duplikat", status_code=303)
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Dodano polimer"})
    return RedirectResponse("/polimery?success=dodano", status_code=303)


@router.post("/pobierz_polimer")
def pobierz_polimer(
    request: Request,
    id: int = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] != "dostepna":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "niedostepna"}, status_code=400)
        return RedirectResponse("/polimery?error=niedostepna", status_code=303)
    cur.execute("UPDATE polymers SET status='w_uzyciu' WHERE id=?", (id,))
    cur.execute(
        "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('pobranie', ?, ?, ?)",
        (id, p["lokalizacja"], f"Pobrano: {p['lub']} / {p['kolor']}"),
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Pobrano polimer", "new_status": "w_uzyciu"})
    return RedirectResponse("/polimery?success=pobrano", status_code=303)


@router.post("/zwroc_polimer")
def zwroc_polimer(
    request: Request,
    id: int = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] != "w_uzyciu":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "nie_w_uzyciu"}, status_code=400)
        return RedirectResponse("/polimery?error=nie_w_uzyciu", status_code=303)
    cur.execute("UPDATE polymers SET status='dostepna' WHERE id=?", (id,))
    cur.execute(
        "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('zwrot', ?, ?, ?)",
        (id, p["lokalizacja"], f"Zwrot: {p['lub']} / {p['kolor']}"),
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zwrócono polimer", "new_status": "dostepna"})
    return RedirectResponse("/polimery?success=zwrocono", status_code=303)


@router.post("/uszkodz_polimer")
def uszkodz_polimer(
    request: Request,
    id: int = Form(...),
    powod: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] not in ["dostepna", "w_uzyciu"]:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "nie_mozna_uszkodzic"}, status_code=400)
        return RedirectResponse("/polimery?error=nie_mozna_uszkodzic", status_code=303)
    cur.execute("UPDATE polymers SET status='uszkodzona' WHERE id=?", (id,))
    cur.execute(
        "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('uszkodzenie', ?, ?, ?)",
        (id, p["lokalizacja"], f"Uszkodzenie: {powod}" if powod else "Uszkodzenie"),
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Oznaczono jako uszkodzony", "new_status": "uszkodzona"})
    return RedirectResponse("/polimery?success=uszkodzono", status_code=303)


@router.post("/utylizuj_polimer")
def utylizuj_polimer(
    request: Request,
    id: int = Form(...),
    powod: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse("/polimery?error=notfound", status_code=303)
    if p["status"] == "zutylizowana":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "juz_zutylizowana"}, status_code=400)
        return RedirectResponse("/polimery?error=juz_zutylizowana", status_code=303)
    cur.execute("UPDATE polymers SET status='zutylizowana' WHERE id=?", (id,))
    cur.execute(
        "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('utylizacja', ?, ?, ?)",
        (id, p["lokalizacja"], f"Utylizacja: {powod}" if powod else "Utylizacja"),
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zutylizowano polimer", "new_status": "zutylizowana"})
    return RedirectResponse("/polimery?success=utylizowano", status_code=303)


@router.post("/przywroc_polimer")
def przywroc_polimer(
    request: Request,
    id: int = Form(...),
    nowa_data_waznosci: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p or p["status"] != "uszkodzona":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "blad_przywracania"}, status_code=400)
        return RedirectResponse("/polimery?error=blad_przywracania", status_code=303)
    if nowa_data_waznosci:
        cur.execute("UPDATE polymers SET status='dostepna', data_waznosci=? WHERE id=?", (nowa_data_waznosci, id))
    else:
        cur.execute("UPDATE polymers SET status='dostepna' WHERE id=?", (id,))
    cur.execute(
        "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('przywrócenie', ?, ?, ?)",
        (
            id,
            p["lokalizacja"],
            "Przywrócono z uszkodzenia" + (f", nowa data ważności: {nowa_data_waznosci}" if nowa_data_waznosci else ""),
        ),
    )
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Przywrócono polimer", "new_status": "dostepna"})
    return RedirectResponse("/polimery?success=przywrocono", status_code=303)


@router.get("/get_polimer_row/{id}")
def get_polimer_row(
    id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (id,))
    p = cur.fetchone()
    if not p:
        return HTMLResponse("<td colspan='7'>Błąd: polimer nie istnieje</td>", status_code=404)
    return render_template("polimery_row.html", {"p": dict(p)})


@router.post("/polimer/{polimer_id}/przypisz-lub")
def polimer_przypisz_lub(
    polimer_id: int,
    request: Request,
    assign_lub: str = Form(...),
    plan_id: int = Form(0),
    assign_machine: str = Form(""),
    return_to: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    """Ręczne przypisanie polimeru do numeru LUB i zlecenia."""
    # Walidacja return_to przed open-redirect
    safe_return = return_to if return_to.startswith("/maszyna/") or return_to.startswith("/plany") else ""
    cur = conn.cursor()
    cur.execute("SELECT * FROM polymers WHERE id=?", (polimer_id,))
    p = cur.fetchone()
    if not p:
        redirect = safe_return or "/polimery"
        return RedirectResponse(f"{redirect}?error=notfound", status_code=303)
    cur.execute("UPDATE polymers SET lub=? WHERE id=?", (assign_lub, polimer_id))
    cur.execute(
        "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('przypisanie_lub', ?, ?, ?)",
        (polimer_id, p["lokalizacja"],
         f"Przypisano do LUB {assign_lub} (zlecenie ID {plan_id}) przez {user['username']}"),
    )
    log_production_operation(
        cur, "przypisanie_polimeru_do_lub",
        f"Polimer {p['kolor']} (ID {polimer_id}) przypisany do LUB {assign_lub} przez {user['username']}",
        assign_machine or None, plan_id or None, user["username"],
    )
    conn.commit()
    redirect = safe_return or "/polimery"
    sep = "&" if "?" in redirect else "?"
    return RedirectResponse(f"{redirect}{sep}success=przypisano_polimer", status_code=303)
