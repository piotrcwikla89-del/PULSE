"""
Router: panel admina — zarządzanie użytkownikami, import danych, ustawienia powiadomień.
"""
import csv
import io

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile
from fastapi.responses import RedirectResponse
from starlette.requests import Request

from db_compat import INTEGRITY_ERRORS
from dependencies import get_db, require_admin
from helpers import (
    NOTIFICATION_EVENT_LABELS,
    PRODUCTION_MACHINES,
    dodaj_operacje,
    render_template,
)

router = APIRouter(prefix="/admin")


@router.get("")
def admin_panel(
    request: Request,
    admin=Depends(require_admin),
    error: str = Query(None),
    success: str = Query(None),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT id, username, role FROM users ORDER BY role DESC, username")
    users = cur.fetchall()
    cur.execute("SELECT name, start_time, end_time FROM shifts ORDER BY start_time")
    shifts = cur.fetchall()
    cur.execute("SELECT event_key, enabled FROM notification_settings")
    settings_map = {r["event_key"]: bool(r["enabled"]) for r in cur.fetchall()}
    notification_prefs = [
        {"key": k, "label": v, "enabled": settings_map.get(k, True)}
        for k, v in NOTIFICATION_EVENT_LABELS.items()
    ]
    context = {
        "users": users,
        "shifts": shifts,
        "notification_prefs": notification_prefs,
        "user": {"username": request.session.get("username"), "role": request.session.get("role")},
    }
    if error:
        context["error"] = error
    if success:
        context["success"] = success
    return render_template("admin_panel.html", context)


@router.post("/add_operator")
def add_operator(request: Request, username: str = Form(...), admin=Depends(require_admin)):
    return RedirectResponse("/admin?error=U%C5%BCyj+formularza+dodawania+u%C5%BCytkownika", status_code=303)


@router.post("/add_user")
def add_user(
    request: Request,
    username: str = Form(...),
    role: str = Form(...),
    password: str = Form(""),
    confirm_password: str = Form(""),
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    if role not in ["drukarz", "operator_mieszalni", "prepress", "manager", "admin"]:
        return RedirectResponse("/admin?error=Nieprawid%C5%82owa+rola", status_code=303)
    if not password or not confirm_password or password != confirm_password:
        return RedirectResponse("/admin?error=Has%C5%82a+niezgodne+lub+puste", status_code=303)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (username, role, password) VALUES (?, ?, ?)",
            (username, role, password),
        )
        conn.commit()
    except INTEGRITY_ERRORS:
        return RedirectResponse("/admin?error=U%C5%BCytkownik+ju%C5%BC+istnieje", status_code=303)
    return RedirectResponse("/admin?success=U%C5%BCytkownik+dodany", status_code=303)


@router.post("/notification-settings")
async def admin_notification_settings(
    request: Request,
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    form = await request.form()
    cur = conn.cursor()
    for key in NOTIFICATION_EVENT_LABELS:
        enabled = 1 if form.get(key) == "on" else 0
        cur.execute("UPDATE notification_settings SET enabled=? WHERE event_key=?", (enabled, key))
    conn.commit()
    return RedirectResponse("/admin?success=Zapisano+ustawienia+powiadomie%C5%84", status_code=303)


@router.post("/delete_user")
def delete_user(
    request: Request,
    user_id: int = Form(...),
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT role FROM users WHERE id=?", (user_id,))
    user = cur.fetchone()
    if user and user["role"] != "admin":
        cur.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()
    return RedirectResponse("/admin", status_code=303)


@router.post("/change_password")
def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    cur.execute("SELECT password FROM users WHERE username=? AND role='admin'", (admin["username"],))
    db_user = cur.fetchone()
    if db_user and db_user["password"] == current_password:
        cur.execute(
            "UPDATE users SET password=? WHERE username=? AND role='admin'",
            (new_password, admin["username"]),
        )
        conn.commit()
    return RedirectResponse("/admin", status_code=303)


@router.post("/import_farby")
def import_farby(
    request: Request,
    plik: UploadFile = File(...),
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    try:
        content = plik.file.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        cur = conn.cursor()
        count = 0
        for row in reader:
            pantone = row.get("pantone") or row.get("pantone".upper()) or row.get("PANTONE")
            lub = row.get("lub", "")
            polka = row.get("polka", "")
            waga = row.get("waga")
            data_produkcji = row.get("data_produkcji") or row.get("data_produkcji".replace("_", ""))
            if not pantone or not waga or not data_produkcji:
                continue
            try:
                waga_val = float(waga)
            except ValueError:
                continue
            cur.execute(
                "INSERT INTO farby (pantone, lub, polka, waga, status, data_produkcji) VALUES (?, ?, ?, ?, 'dostepna', ?)",
                (pantone, lub, polka, waga_val, data_produkcji),
            )
            dodaj_operacje(cur, "przyjęcie", pantone, str(waga_val), polka, "", cur.lastrowid)
            count += 1
        conn.commit()
        return RedirectResponse(f"/admin?success=Zaimportowano+{count}+farb", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/admin?error=Błąd+importu+farb:+{str(e)}", status_code=303)


@router.post("/import_polimery")
def import_polimery(
    request: Request,
    plik: UploadFile = File(...),
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    try:
        content = plik.file.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        cur = conn.cursor()
        count = 0
        for row in reader:
            lub = row.get("lub") or row.get("LUB")
            kolor = row.get("kolor") or row.get("Kolor")
            lokalizacja = row.get("lokalizacja", "")
            data_waznosci = row.get("data_waznosci") or row.get("data_waznosci".replace("_", ""))
            uwagi = row.get("uwagi", "")
            if not lub or not kolor:
                continue
            cur.execute(
                "INSERT INTO polymers (lub, kolor, status, lokalizacja, data_waznosci, uwagi) VALUES (?, ?, 'dostepna', ?, ?, ?)",
                (lub, kolor, lokalizacja, data_waznosci, uwagi),
            )
            new_id = cur.lastrowid
            cur.execute(
                "INSERT INTO polymer_operations (typ, polymer_id, lokalizacja, uwagi) VALUES ('przyjęcie', ?, ?, ?)",
                (new_id, lokalizacja, f"Import CSV: LUB={lub}, kolor={kolor}"),
            )
            count += 1
        conn.commit()
        return RedirectResponse(f"/admin?success=Zaimportowano+{count}+polimerów", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/admin?error=Błąd+importu+polimerów:+{str(e)}", status_code=303)


@router.post("/import_plany")
def import_plany(
    request: Request,
    plik: UploadFile = File(...),
    admin=Depends(require_admin),
    conn=Depends(get_db),
):
    try:
        content = plik.file.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        cur = conn.cursor()
        count = 0
        for row in reader:
            machine = (row.get("machine") or row.get("maszyna") or "").upper()
            order_number = row.get("order_number") or row.get("numer_zlecenia")
            artwork_number = row.get("artwork_number") or row.get("numer_artwork")
            lub_number = row.get("lub_number") or row.get("numer_lub")
            order_name = row.get("order_name") or row.get("nazwa_zlecenia")
            laminate = row.get("laminate") or row.get("laminat")
            meters = row.get("meters") or row.get("ilosc_metrow") or 0
            pieces = row.get("pieces") or row.get("ilosc_sztuk") or 0
            planned_date = row.get("planned_date") or row.get("data_planowana")
            if machine not in PRODUCTION_MACHINES or not order_number:
                continue
            try:
                meters_val = int(meters)
                pieces_val = int(pieces)
            except ValueError:
                continue
            cur.execute(
                """INSERT INTO production_plans (machine, order_number, artwork_number, lub_number, order_name, laminate, meters, pieces, planned_date, status, assortment_prep_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', 'pending')""",
                (machine, order_number, artwork_number, lub_number, order_name, laminate, meters_val, pieces_val, planned_date),
            )
            count += 1
        conn.commit()
        return RedirectResponse(f"/admin?success=Zaimportowano+{count}+planów", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/admin?error=Błąd+importu+planów:+{str(e)}", status_code=303)
