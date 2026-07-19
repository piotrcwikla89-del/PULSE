from fastapi import APIRouter, Depends, Query
from fastapi.responses import RedirectResponse
from starlette.requests import Request

from dependencies import get_db, require_auth
from helpers import render_template

router = APIRouter()


def _can_view_traceability(user: dict) -> bool:
    return user.get("role") in ("admin", "manager", "kierownik")


def _build_traceability_context(cur, query: str) -> dict:
    query_norm = (query or "").strip()
    plan = None
    if query_norm:
        cur.execute(
            """
            SELECT * FROM production_plans
            WHERE order_number = ? OR artwork_number = ? OR lub_number = ?
            ORDER BY id DESC LIMIT 1
            """,
            (query_norm, query_norm, query_norm),
        )
        plan_row = cur.fetchone()
        if plan_row:
            if hasattr(plan_row, "keys"):
                plan = dict(plan_row)
            else:
                columns = [desc[0] for desc in cur.description]
                plan = dict(zip(columns, plan_row))

    materials = []
    reports = []
    print_reports = []
    winding_reports = []
    events = []

    if plan:
        if hasattr(plan, "keys"):
            plan_id = plan["id"]
        else:
            plan_id = plan[0]
        cur.execute(
            "SELECT * FROM production_reports WHERE plan_id=? ORDER BY created_at DESC",
            (plan_id,),
        )
        reports = [dict(row) if hasattr(row, "keys") else dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]

        cur.execute(
            "SELECT * FROM print_control_reports WHERE plan_id=? ORDER BY created_at DESC",
            (plan_id,),
        )
        print_reports = [dict(row) if hasattr(row, "keys") else dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]

        cur.execute(
            "SELECT * FROM winding_reports WHERE plan_id=? ORDER BY created_at DESC",
            (plan_id,),
        )
        winding_reports = [dict(row) if hasattr(row, "keys") else dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]

        if hasattr(plan, "keys"):
            lub_number = plan.get("lub_number")
        else:
            lub_number = plan[3] if len(plan) > 3 else None
        if lub_number:
            cur.execute(
                "SELECT * FROM farby WHERE lub=? ORDER BY id DESC",
                (lub_number,),
            )
            materials = [dict(row) if hasattr(row, "keys") else dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]

        cur.execute(
            "SELECT * FROM events WHERE plan_id=? ORDER BY created_at DESC",
            (plan_id,),
        )
        events = [dict(row) if hasattr(row, "keys") else dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]

    return {
        "query": query_norm,
        "plan": plan,
        "materials": materials,
        "reports": reports,
        "print_reports": print_reports,
        "winding_reports": winding_reports,
        "events": events,
    }


@router.get("/traceability")
def traceability(
    request: Request,
    query: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_view_traceability(user):
        return RedirectResponse("/dashboard", status_code=303)

    cur = conn.cursor()
    context = _build_traceability_context(cur, query)

    return render_template("traceability.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "context": context,
    })
