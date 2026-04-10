"""
Router: panel kierownika — raporty, statystyki, dziennik zmian.
"""
from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from starlette.requests import Request

from dependencies import get_db, require_manager_or_admin
from helpers import normalize_shift_label, render_template
from time_utils import local_day_bounds_utc, local_today, utc_threshold_db_string

router = APIRouter(prefix="/kierownik")


@router.get("")
def kierownik(request: Request, user=Depends(require_manager_or_admin)):
    return render_template("kierownik.html", {
        "user": {"username": user["username"], "role": user["role"]}
    })


@router.get("/rejestr-raportow")
def kierownik_rejestr_raportow(
    request: Request,
    user=Depends(require_manager_or_admin),
    date_q: str = Query("", alias="date"),
    conn=Depends(get_db),
):
    if not date_q:
        date_q = local_today().strftime("%Y-%m-%d")
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM print_control_reports WHERE date=? ORDER BY machine, created_at DESC",
        (date_q,),
    )
    print_reports = cur.fetchall()
    cur.execute(
        "SELECT * FROM production_reports WHERE date=? ORDER BY machine, created_at DESC",
        (date_q,),
    )
    production_reports = [dict(row) for row in cur.fetchall()]
    issues_by_report = defaultdict(list)
    if production_reports:
        placeholders = ", ".join(["?"] * len(production_reports))
        params = [report["id"] for report in production_reports]
        cur.execute(
            f"""
            SELECT pri.production_report_id, pc.label, pc.target_role, pri.short_note, pri.status,
                   pri.resolved_by, pri.resolved_at, pri.resolution_note
            FROM production_report_issues pri
            JOIN problem_categories pc ON pc.id = pri.problem_category_id
            WHERE pri.production_report_id IN ({placeholders})
            ORDER BY pri.id
            """,
            params,
        )
        for row in cur.fetchall():
            issues_by_report[row["production_report_id"]].append({
                "label": row["label"],
                "target_role": row["target_role"],
                "short_note": row["short_note"],
                "status": row["status"],
                "resolved_by": row["resolved_by"],
                "resolved_at": row["resolved_at"],
                "resolution_note": row["resolution_note"],
            })
    for report in production_reports:
        report["issues"] = issues_by_report.get(report["id"], [])
    return render_template("kierownik_rejestr_raportow.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "date_q": date_q,
        "print_reports": print_reports,
        "production_reports": production_reports,
    })


@router.get("/raport-zmiany")
def kierownik_raport_zmiany(
    request: Request,
    user=Depends(require_manager_or_admin),
    date_q: str = Query("", alias="date"),
    zmiana: str = Query("dzien"),
    conn=Depends(get_db),
):
    if not date_q:
        date_q = local_today().strftime("%Y-%m-%d")
    zm = normalize_shift_label(zmiana)
    cur = conn.cursor()
    cur.execute("SELECT * FROM print_control_reports WHERE date=? ORDER BY machine, created_at", (date_q,))
    print_all = cur.fetchall()
    cur.execute("SELECT * FROM production_reports WHERE date=?", (date_q,))
    prod_all = [r for r in cur.fetchall() if normalize_shift_label(r["shift"]) == zm]
    total_qty = sum(int(r["quantity"] or 0) for r in prod_all)
    total_ok = sum(int(r["ok_quantity"] or 0) for r in prod_all)
    total_nok = sum(int(r["nok_quantity"] or 0) for r in prod_all)
    zadruk_ok = sum(1 for r in print_all if (r["status"] or "").upper() == "OK")
    zadruk_nok = len(print_all) - zadruk_ok
    by_machine = {}
    for r in prod_all:
        m = r["machine"]
        if m not in by_machine:
            by_machine[m] = {"qty": 0, "ok": 0, "nok": 0}
        by_machine[m]["qty"] += int(r["quantity"] or 0)
        by_machine[m]["ok"] += int(r["ok_quantity"] or 0)
        by_machine[m]["nok"] += int(r["nok_quantity"] or 0)
    return render_template("kierownik_raport_zmiany.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "date_q": date_q,
        "zmiana": zm,
        "print_all": print_all,
        "prod_filtered": prod_all,
        "total_qty": total_qty,
        "total_ok": total_ok,
        "total_nok": total_nok,
        "zadruk_ok": zadruk_ok,
        "zadruk_nok": zadruk_nok,
        "by_machine": by_machine,
    })


@router.get("/raport-dziennie")
def kierownik_raport_dziennie(request: Request, user=Depends(require_manager_or_admin), conn=Depends(get_db)):
    cur = conn.cursor()
    start_utc, end_utc = local_day_bounds_utc()
    cur.execute("""
        SELECT machine, SUM(quantity) as total_qty, SUM(ok_quantity) as ok_qty, SUM(nok_quantity) as nok_qty
        FROM production_reports
        WHERE created_at >= ? AND created_at < ?
        GROUP BY machine
    """, (start_utc, end_utc))
    production = cur.fetchall()
    return render_template("kierownik_raport_dziennie.html", {
        "production": production,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.get("/statystyki-zmian")
def kierownik_statystyki_zmian(request: Request, user=Depends(require_manager_or_admin)):
    return render_template("kierownik_statystyki.html", {
        "user": {"username": user["username"], "role": user["role"]}
    })


@router.get("/raport-jakosci")
def kierownik_raport_jakosci(request: Request, user=Depends(require_manager_or_admin), conn=Depends(get_db)):
    cur = conn.cursor()
    start_utc, end_utc = local_day_bounds_utc()
    cur.execute("""
        SELECT
            machine,
            COUNT(*) as total_reports,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) as ok_count,
            SUM(CASE WHEN status='NOT_OK' THEN 1 ELSE 0 END) as not_ok_count,
            ROUND(100.0 * SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_score
        FROM print_control_reports
        WHERE created_at >= ? AND created_at < ?
        GROUP BY machine
    """, (start_utc, end_utc))
    quality = cur.fetchall()
    cur.execute("""
        SELECT * FROM print_control_reports
        WHERE created_at >= ? AND created_at < ?
        ORDER BY machine, created_at DESC
    """, (start_utc, end_utc))
    reports = cur.fetchall()
    return render_template("kierownik_raport_jakosci.html", {
        "quality": quality,
        "reports": reports,
        "user": {"username": user["username"], "role": user["role"]},
    })


@router.get("/dziennik-zmian")
def kierownik_dziennik_zmian(request: Request, user=Depends(require_manager_or_admin), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute("""
        SELECT pl.*, p.order_number, p.machine
        FROM production_log pl
        LEFT JOIN production_plans p ON pl.plan_id = p.id
        WHERE pl.created_at >= ?
        ORDER BY pl.created_at DESC
    """, (utc_threshold_db_string(days=7),))
    operations = cur.fetchall()
    return render_template("kierownik_dziennik_zmian.html", {
        "operations": operations,
        "user": {"username": user["username"], "role": user["role"]},
    })
