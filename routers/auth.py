"""
Router: logowanie, wylogowanie, dashboard, root redirect.
"""
import os

from fastapi import APIRouter, Depends, Form
from fastapi.responses import RedirectResponse
from starlette.requests import Request

from dependencies import get_current_user, require_auth, get_db
from helpers import find_pending_machine_handover, find_pending_role_shift_handover, get_base_path, has_pending_role_handover, render_template

router = APIRouter()


def _resolve_post_login_redirect(request: Request, user, cur) -> str:
    role = user["role"]
    if role == "drukarz":
        machine = request.session.get("machine")
        if not machine:
            return "/select-machine"
        pending_handover = find_pending_machine_handover(cur, machine)
        if pending_handover:
            return f"/maszyna/{machine.lower()}/przekazanie-zmiany/odbior?handover_id={pending_handover['id']}"
        return f"/maszyna/{machine.lower()}/plany"
    if role == "operator_przewijarki":
        machine = request.session.get("machine")
        if not machine:
            return "/select-przewijarka"
        pending_handover = find_pending_machine_handover(cur, machine)
        if pending_handover:
            return f"/przewijarka/{machine.lower()}/przekazanie-zmiany/odbior?handover_id={pending_handover['id']}"
        return f"/przewijarka/{machine.lower()}/plany"
    if role in ("operator_mieszalni", "prepress"):
        pending_role_handover = find_pending_role_shift_handover(cur, role)
        if pending_role_handover or has_pending_role_handover(cur, role):
            return "/przekazanie-zmiany"
    return "/dashboard"


@router.get("/login")
def login_form(request: Request):
    logo_path = "/static/logo_duze.svg"
    custom_logo_path = os.path.join(get_base_path(), "static", "logo_custom.png")
    if os.path.exists(custom_logo_path):
        logo_path = "/static/logo_custom.png"
    return render_template("login.html", {"operators": [], "logo_url": logo_path})


@router.post("/login")
def login(
    request: Request,
    username: str = Form(None),
    password: str = Form(None),
    conn=Depends(get_db),
):
    cur = conn.cursor()
    user = None
    if username and password:
        cur.execute("SELECT * FROM users WHERE username=?", (username,))
        db_user = cur.fetchone()
        if db_user and db_user["password"] == password:
            user = db_user
    if user:
        request.session["username"] = user["username"]
        request.session["role"] = user["role"]
        return RedirectResponse(_resolve_post_login_redirect(request, user, cur), status_code=303)
    else:
        cur.execute("SELECT username FROM users WHERE role != 'admin' ORDER BY username")
        operators = cur.fetchall()
        logo_path = "/static/logo_duze.svg"
        custom_logo_path = os.path.join(get_base_path(), "static", "logo_custom.png")
        if os.path.exists(custom_logo_path):
            logo_path = "/static/logo_custom.png"
        return render_template("login.html", {"operators": operators, "error": "Nieprawidłowe dane logowania", "logo_url": logo_path})


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")


@router.get("/dashboard")
def dashboard(request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    redirect_path = _resolve_post_login_redirect(request, user, cur)
    if redirect_path != "/dashboard":
        return RedirectResponse(redirect_path, status_code=303)
    return render_template("dashboard.html", {
        "user": {"username": user["username"], "role": user["role"]}
    })


@router.get("/")
def root(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=303)
    return RedirectResponse("/login", status_code=303)
