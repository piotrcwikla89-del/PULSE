"""
Zależności FastAPI współdzielone przez wszystkie routery.
W produkcji ustaw SESSION_SECRET jako zmienną środowiskową.
"""
from __future__ import annotations

import sqlite3
from typing import Generator

from fastapi import Depends, HTTPException
from starlette.requests import Request

from db_compat import get_db as _get_db_raw, is_postgres


def get_db() -> Generator:
    """
    Dependency z yield — połączenie jest zawsze zamykane po zakończeniu requestu.
    W produkcji ustaw SESSION_SECRET jako zmienną środowiskową.
    """
    conn = _get_db_raw()
    try:
        yield conn
    finally:
        conn.close()


def get_current_user(request: Request) -> dict | None:
    username = request.session.get("username")
    role = request.session.get("role")
    if username and role:
        return {"username": username, "role": role}
    return None


def require_auth(user: dict | None = Depends(get_current_user)) -> dict:
    if not user:
        raise HTTPException(status_code=303, detail="Zaloguj się", headers={"Location": "/login"})
    return user


def require_admin(user: dict | None = Depends(get_current_user)) -> dict:
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Brak dostępu")
    return user


def require_manager_or_admin(user: dict | None = Depends(get_current_user)) -> dict:
    if not user or user.get("role") not in ["admin", "manager"]:
        raise HTTPException(status_code=403, detail="Brak dostępu")
    return user


def is_ajax(request: Request) -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"
