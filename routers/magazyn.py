"""
Router: magazyn farb — widok główny, akcje na farbach, historia, statystyki, eksport.
"""
import csv
import io
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from starlette.requests import Request

from dependencies import get_db, is_ajax, require_auth, require_manager_or_admin
from helpers import (
    alert_daty,
    build_redirect_url,
    dodaj_operacje,
    filtruj_farby,
    render_template,
)

router = APIRouter()


@router.get("/magazyn")
def magazyn(
    request: Request,
    search_field: str = Query("lub"),
    search_value: str = Query(""),
    filtr_alert: str = Query(""),
    status: str = Query(""),
    sort: str = Query("status"),
    dir: str = Query("asc"),
    error: str = Query(""),
    success: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby")
    dane = cur.fetchall()

    farby, licznik_przeterminowane, licznik_uwaga, licznik_zutylizowane = filtruj_farby(
        dane, search_field, search_value, filtr_alert, status
    )

    reverse = dir == "desc"
    if sort == "status":
        order = {"dostepna": 1, "w_uzyciu": 2, "zutylizowana": 3}
        farby.sort(key=lambda x: order.get(x["status"], 9), reverse=reverse)
    else:
        farby.sort(key=lambda x: (x.get(sort) or ""), reverse=reverse)

    return render_template("magazyn.html", {
        "farby": farby,
        "licznik_przeterminowane": licznik_przeterminowane,
        "licznik_uwaga": licznik_uwaga,
        "licznik_zutylizowane": licznik_zutylizowane,
        "search_field": search_field,
        "search_value": search_value,
        "filtr_alert": filtr_alert,
        "status": status,
        "sort": sort,
        "dir": dir,
        "user": {"username": user["username"], "role": user["role"]},
        "error": error,
        "success": success,
    })


@router.post("/dodaj_farba")
def dodaj_farba(
    request: Request,
    pantone: str = Form(...),
    lub: str = Form(""),
    polka: str = Form(...),
    data_produkcji: str = Form(...),
    waga: float = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO farby (pantone, lub, polka, waga, status, data_produkcji) VALUES (?, ?, ?, ?, 'dostepna', ?)",
        (pantone, lub, polka, waga, data_produkcji),
    )
    dodaj_operacje(cur, "przyjęcie", pantone, str(waga), polka, "", cur.lastrowid)
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Dodano farbę"})
    return RedirectResponse(build_redirect_url(request, {"success": "dodano"}), status_code=303)


@router.post("/pobierz")
def pobierz(
    request: Request,
    id: int = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse(build_redirect_url(request, {"error": "notfound"}), status_code=303)
    if f["status"] != "dostepna":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "niedostepna"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "niedostepna"}), status_code=303)
    if alert_daty(f["data_produkcji"]) == "przeterminowana":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "przeterminowana"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "przeterminowana"}), status_code=303)
    cur.execute("UPDATE farby SET status='w_uzyciu' WHERE id=?", (id,))
    dodaj_operacje(cur, "wydanie", f["pantone"], str(f["waga"]), f["polka"], "", f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Pobrano farbę", "new_status": "w_uzyciu"})
    return RedirectResponse(build_redirect_url(request, {"success": "pobrano"}), status_code=303)


@router.post("/zwrot")
def zwrot(
    request: Request,
    id: int = Form(...),
    waga: float = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse(build_redirect_url(request, {"error": "notfound"}), status_code=303)
    if f["status"] != "w_uzyciu":
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "nie_w_uzyciu"}, status_code=400)
        return RedirectResponse(build_redirect_url(request, {"error": "nie_w_uzyciu"}), status_code=303)
    cur.execute("UPDATE farby SET status='dostepna', waga=? WHERE id=?", (waga, id))
    dodaj_operacje(cur, "zwrot", f["pantone"], str(waga), f["polka"], "", f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zwrócono farbę", "new_status": "dostepna", "new_waga": waga})
    return RedirectResponse(build_redirect_url(request, {"success": "zwrocono"}), status_code=303)


@router.post("/utylizacja")
def utylizacja(
    request: Request,
    id: int = Form(...),
    powod: str = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "notfound"}, status_code=404)
        return RedirectResponse(build_redirect_url(request, {"error": "notfound"}), status_code=303)
    cur.execute("UPDATE farby SET status='zutylizowana', waga=0 WHERE id=?", (id,))
    dodaj_operacje(cur, "utylizacja", f["pantone"], str(f["waga"]), f["polka"], powod, f["id"])
    conn.commit()
    if is_ajax(request):
        return JSONResponse({"success": True, "message": "Zutylizowano farbę", "new_status": "zutylizowana"})
    return RedirectResponse(build_redirect_url(request, {"success": "utylizowano"}), status_code=303)


@router.post("/przywroc")
def przywroc(
    request: Request,
    id: int = Form(...),
    nowa_data: str = Form(...),
    nowa_waga: float = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM farby WHERE id=?", (id,))
        f = cur.fetchone()
        if not f or f["status"] != "zutylizowana":
            if is_ajax(request):
                return JSONResponse({"success": False, "error": "blad_przywracania"}, status_code=400)
            return RedirectResponse(build_redirect_url(request, {"error": "blad_przywracania"}), status_code=303)
        cur.execute(
            "UPDATE farby SET status='dostepna', data_produkcji=?, waga=? WHERE id=?",
            (nowa_data, nowa_waga, id),
        )
        dodaj_operacje(cur, "przywrócenie", f["pantone"], str(nowa_waga), f["polka"], f"nowa data: {nowa_data}", f["id"])
        conn.commit()
    except Exception:
        conn.rollback()
        if is_ajax(request):
            return JSONResponse({"success": False, "error": "blad_przywracania"}, status_code=500)
        return RedirectResponse(build_redirect_url(request, {"error": "blad_przywracania"}), status_code=303)
    if is_ajax(request):
        return JSONResponse({
            "success": True,
            "message": "Przywrócono farbę",
            "new_status": "dostepna",
            "new_data": nowa_data,
            "new_waga": nowa_waga,
        })
    return RedirectResponse(build_redirect_url(request, {"success": "przywrocono"}), status_code=303)


@router.post("/pobierz_wszystkie")
def pobierz_wszystkie(
    request: Request,
    search_field: str = Form(...),
    search_value: str = Form(...),
    filtr_alert: str = Form(""),
    status: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby")
    dane = cur.fetchall()
    liczba = 0
    for f in dane:
        alert = alert_daty(f["data_produkcji"])
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
        if f["status"] == "dostepna" and alert != "przeterminowana":
            cur.execute("UPDATE farby SET status='w_uzyciu' WHERE id=?", (f["id"],))
            dodaj_operacje(cur, "wydanie", f["pantone"], str(f["waga"]), f["polka"], "", f["id"])
            liczba += 1
    conn.commit()
    return RedirectResponse(build_redirect_url(request, {"success": f"pobrano_{liczba}"}), status_code=303)


@router.get("/get_row/{id}")
def get_row(
    id: int,
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT * FROM farby WHERE id=?", (id,))
    f = cur.fetchone()
    if not f:
        from fastapi.responses import HTMLResponse
        return HTMLResponse(".<td colspan='7'>Błąd: farba nie istnieje</td>", status_code=404)
    alert = alert_daty(f["data_produkcji"])
    f_dict = dict(f)
    f_dict["alert"] = alert
    return render_template("row.html", {"f": f_dict})


@router.get("/historia")
def historia(request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute("SELECT * FROM operacje ORDER BY id DESC")
    operacje = cur.fetchall()
    return render_template("historia.html", {
        "operacje": operacje,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.get("/statystyki")
def statystyki(request: Request, user=Depends(require_manager_or_admin), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute("""
        SELECT farba, COUNT(*) as ile
        FROM operacje
        WHERE typ='wydanie'
        GROUP BY farba
        ORDER BY ile DESC
        LIMIT 10
    """)
    top_farby = cur.fetchall()
    labels = [row["farba"] for row in top_farby]
    values = [row["ile"] for row in top_farby]
    return render_template("statystyki.html", {
        "top_farby": top_farby,
        "labels": labels,
        "values": values,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.get("/raport_utylizacji")
def raport_utylizacji(
    request: Request,
    od: str = Query(""),
    do: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    if od and do:
        cur.execute("""
            SELECT farba, ilosc, data, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od, do))
    else:
        do_dom = datetime.now().strftime("%Y-%m-%d")
        od_dom = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT farba, ilosc, data, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od_dom, do_dom))
        od, do = od_dom, do_dom
    utylizacje = cur.fetchall()
    return render_template("raport_utylizacji.html", {
        "utylizacje": utylizacje,
        "od": od,
        "do": do,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.get("/export_raport_utylizacji")
def export_raport_utylizacji(
    od: str = Query(""),
    do: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    if od and do:
        cur.execute("""
            SELECT data, farba, ilosc, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od, do))
    else:
        do_dom = datetime.now().strftime("%Y-%m-%d")
        od_dom = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT data, farba, ilosc, uwagi
            FROM operacje
            WHERE typ='utylizacja' AND date(data) BETWEEN ? AND ?
            ORDER BY data DESC
        """, (od_dom, do_dom))
    rows = cur.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Data", "Farba", "Ilość (kg)", "Powód"])
    for row in rows:
        writer.writerow([row["data"], row["farba"], row["ilosc"], row["uwagi"]])
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=raport_utylizacji.csv"
    })


@router.get("/export")
def export_csv(user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute("SELECT * FROM operacje")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["data", "typ", "farba", "ilosc", "polka", "uwagi"])
    for row in cur.fetchall():
        writer.writerow([row["data"], row["typ"], row["farba"], row["ilosc"], row["polka"], row["uwagi"]])
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=logi.csv"
    })
