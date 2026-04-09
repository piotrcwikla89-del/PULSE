"""
Router: powiadomienia — widok, oznaczanie jako przeczytane, API polling.
"""
from datetime import date, datetime, time as dt_time

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from starlette.requests import Request

from dependencies import get_db, require_auth
from helpers import render_template


def _row_to_dict(row) -> dict:
    """Konwertuje wiersz bazy (sqlite3.Row lub psycopg2.DictRow) do JSON-bezpiecznego dict."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, (datetime, date, dt_time)):
            d[k] = v.isoformat()
    return d

router = APIRouter()


def _notification_select_sql(with_role_filter: bool) -> str:
    q = """
        SELECT n.id, n.machine, n.plan_id, n.message, n.target_role, n.target_user,
               n.created_by, n.is_read, n.created_at,
               p.order_number AS plan_order_number, p.lub_number AS plan_lub_number
        FROM notifications n
        LEFT JOIN production_plans p ON n.plan_id = p.id
        WHERE n.is_read=0
    """
    if with_role_filter:
        q += " AND (n.target_role=? OR n.target_user=?)"
    q += " ORDER BY n.created_at DESC LIMIT 1"
    return q


@router.get("/notifications")
def notifications_view(request: Request, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    base = """
        SELECT n.id, n.machine, n.plan_id, n.message, n.target_role, n.target_user,
               n.created_by, n.is_read, n.created_at,
               p.order_number AS plan_order_number, p.lub_number AS plan_lub_number
        FROM notifications n
        LEFT JOIN production_plans p ON n.plan_id = p.id
    """
    if user["role"] == "admin":
        cur.execute(base + " ORDER BY n.created_at DESC")
    else:
        cur.execute(
            base + " WHERE n.target_role=? OR n.target_user=? ORDER BY n.created_at DESC",
            (user["role"], user["username"]),
        )
    notifications = cur.fetchall()
    return render_template("notifications.html", {"notifications": notifications})


@router.post("/mark_notification_read/{notification_id}")
def mark_notification_read(notification_id: int, user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    if user["role"] == "admin":
        cur.execute("UPDATE notifications SET is_read=1 WHERE id=?", (notification_id,))
    else:
        cur.execute(
            "UPDATE notifications SET is_read=1 WHERE id=? AND (target_role=? OR target_user=?)",
            (notification_id, user["role"], user["username"]),
        )
    conn.commit()
    return JSONResponse({"success": True})


@router.get("/api/notifications/new")
def get_new_notifications(user=Depends(require_auth), conn=Depends(get_db)):
    cur = conn.cursor()
    if user["role"] == "admin":
        cur.execute(_notification_select_sql(False))
    else:
        cur.execute(
            _notification_select_sql(True),
            (user["role"], user["username"]),
        )
    notifications = cur.fetchall()
    return JSONResponse({"notifications": [_row_to_dict(n) for n in notifications]})
