"""
Router: panel kierownika — raporty, statystyki, dziennik zmian.
"""
from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from starlette.requests import Request

from dependencies import get_db, require_manager_or_admin
from helpers import PRODUCTION_MACHINES, WINDING_MACHINES, normalize_shift_label, render_template
from time_utils import local_day_bounds_utc, local_today, utc_threshold_db_string

router = APIRouter(prefix="/kierownik")


@router.get("")
def kierownik(request: Request, user=Depends(require_manager_or_admin), conn=Depends(get_db)):
    cur = conn.cursor()
    start_utc, end_utc = local_day_bounds_utc()
    machine_rows = []
    active_count = 0

    for machine in list(PRODUCTION_MACHINES) + list(WINDING_MACHINES):
        cur.execute(
            "SELECT id, order_number, order_name, status FROM production_plans WHERE machine=? AND status IN ('planned', 'in_progress') ORDER BY CASE WHEN status='in_progress' THEN 0 ELSE 1 END, id DESC LIMIT 1",
            (machine,),
        )
        active_plan = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS pending_count FROM production_plans WHERE machine=? AND status='planned'", (machine,))
        pending_count = (cur.fetchone()["pending_count"] or 0)
        if machine in WINDING_MACHINES:
            cur.execute(
                "SELECT created_at, order_number, ok_meters, nok_meters FROM winding_reports WHERE machine=? AND created_at >= ? AND created_at < ? ORDER BY created_at DESC LIMIT 1",
                (machine, start_utc, end_utc),
            )
            latest_report = cur.fetchone()
            cur.execute(
                "SELECT COALESCE(SUM(ok_meters), 0) AS today_output FROM winding_reports WHERE machine=? AND created_at >= ? AND created_at < ?",
                (machine, start_utc, end_utc),
            )
            today_output = cur.fetchone()["today_output"] or 0
            machine_type = "przewijarka"
            output_label = "m OK"
        else:
            cur.execute(
                "SELECT created_at, job_number, quantity, ok_quantity FROM production_reports WHERE machine=? AND created_at >= ? AND created_at < ? ORDER BY created_at DESC LIMIT 1",
                (machine, start_utc, end_utc),
            )
            latest_report = cur.fetchone()
            cur.execute(
                "SELECT COALESCE(SUM(quantity), 0) AS today_output FROM production_reports WHERE machine=? AND created_at >= ? AND created_at < ?",
                (machine, start_utc, end_utc),
            )
            today_output = cur.fetchone()["today_output"] or 0
            machine_type = "druk"
            output_label = "szt."

        active_status = "W toku" if active_plan and active_plan["status"] == "in_progress" else "Oczekuje" if pending_count else "Brak planu"
        if active_plan and active_plan["status"] == "in_progress":
            active_count += 1

        machine_rows.append({
            "machine": machine,
            "machine_type": machine_type,
            "active_status": active_status,
            "active_plan": dict(active_plan) if active_plan else None,
            "pending_count": pending_count,
            "today_output": today_output,
            "output_label": output_label,
            "latest_report": dict(latest_report) if latest_report else None,
        })

    cur.execute(
        "SELECT COALESCE(SUM(quantity), 0) AS total_qty, COALESCE(SUM(ok_quantity), 0) AS total_ok, COALESCE(SUM(nok_quantity), 0) AS total_nok FROM production_reports WHERE created_at >= ? AND created_at < ?",
        (start_utc, end_utc),
    )
    production_stats = cur.fetchone()
    cur.execute(
        "SELECT COALESCE(SUM(ok_meters), 0) AS ok_meters, COALESCE(SUM(nok_meters), 0) AS nok_meters FROM winding_reports WHERE created_at >= ? AND created_at < ?",
        (start_utc, end_utc),
    )
    winding_stats = cur.fetchone()
    cur.execute(
        "SELECT COUNT(*) AS total_reports, COALESCE(SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END), 0) AS ok_reports FROM print_control_reports WHERE created_at >= ? AND created_at < ?",
        (start_utc, end_utc),
    )
    quality_stats = cur.fetchone()
    total_reports = quality_stats["total_reports"] or 0
    quality_score = round((quality_stats["ok_reports"] or 0) * 100 / total_reports, 1) if total_reports else 0

    return render_template("kierownik.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "machine_rows": machine_rows,
        "active_count": active_count,
        "machine_total": len(machine_rows),
        "production_total": production_stats["total_qty"] or 0,
        "production_ok": production_stats["total_ok"] or 0,
        "production_nok": production_stats["total_nok"] or 0,
        "winding_ok_meters": winding_stats["ok_meters"] or 0,
        "winding_nok_meters": winding_stats["nok_meters"] or 0,
        "quality_score": quality_score,
        "print_reports_total": total_reports,
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
    cur.execute(
        "SELECT * FROM winding_reports WHERE date=? ORDER BY machine, created_at DESC",
        (date_q,),
    )
    winding_reports = [dict(row) for row in cur.fetchall()]
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
        "winding_reports": winding_reports,
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
