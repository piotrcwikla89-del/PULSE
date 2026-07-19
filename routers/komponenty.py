"""
Router: magazyn komponentów dla operatora mieszalni.
"""
from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import RedirectResponse
from starlette.requests import Request

from dependencies import get_db, require_auth
from helpers import render_template
from db_compat import is_postgres

router = APIRouter()


def _can_manage_components(user: dict) -> bool:
    return user.get("role") in ("operator_mieszalni", "manager", "admin")


def _can_edit_components(user: dict) -> bool:
    return user.get("role") in ("manager", "admin")


COMPONENT_CATEGORIES = ["FARBY", "LAKIERY", "DODATKI", "CHEMIA"]


def _normalize_component_category(raw: str | None) -> str:
    if not raw:
        return "FARBY"
    value = str(raw).strip().upper()
    aliases = {
        "FARBY": "FARBY",
        "FARBA": "FARBY",
        "LAKIERY": "LAKIERY",
        "LAKIER": "LAKIERY",
        "LAKIEROWE": "LAKIERY",
        "VARNISH": "LAKIERY",
        "DODATKI": "DODATKI",
        "ADDITIVE": "DODATKI",
        "ADDITIVES": "DODATKI",
        "ADHESIVE": "DODATKI",
        "ADHESION": "DODATKI",
        "PROMOTOR": "DODATKI",
        "CHEMIA": "CHEMIA",
        "CHEMICZNE": "CHEMIA",
        "CHEMICZNY": "CHEMIA",
        "CLEANER": "CHEMIA",
        "SOLVENT": "CHEMIA",
    }
    if value in aliases:
        return aliases[value]
    lowered = value.lower()
    if any(token in lowered for token in ("lak", "varnish", "matt", "gloss", "satin")):
        return "LAKIERY"
    if any(token in lowered for token in ("adhes", "promotor", "add", "foil")):
        return "DODATKI"
    if any(token in lowered for token in ("clean", "aceton", "octan", "solvent", "printer")):
        return "CHEMIA"
    return "FARBY"


def _ensure_table(cur):
    if is_postgres():
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS komponenty (
                id SERIAL PRIMARY KEY,
                kod TEXT NOT NULL UNIQUE,
                nazwa TEXT NOT NULL,
                kategoria TEXT,
                ilosc DOUBLE PRECISION NOT NULL DEFAULT 0,
                jednostka TEXT NOT NULL DEFAULT 'szt.',
                lokalizacja TEXT,
                uwagi TEXT,
                status TEXT NOT NULL DEFAULT 'dostepny',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS komponenty (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kod TEXT NOT NULL UNIQUE,
                nazwa TEXT NOT NULL,
                kategoria TEXT,
                ilosc REAL NOT NULL DEFAULT 0,
                jednostka TEXT NOT NULL DEFAULT 'szt.',
                lokalizacja TEXT,
                uwagi TEXT,
                status TEXT NOT NULL DEFAULT 'dostepny',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def _seed_components(cur):
    defaults = [
        ("1052994", "FB YFA90095408N MIXING OPAQUE WHITE", "FARBY", 0, "kg", "", ""),
        ("1062547", "FB FLEXO SILVER SMET022: ULTRABRIGHT", "FARBY", 0, "kg", "", ""),
        ("1067241", "FB YFA90092418N BT OPAQUE WHITE", "FARBY", 0, "kg", "", ""),
        ("1080346", "FB YFA30007428N HR WARM RED", "FARBY", 0, "kg", "", ""),
        ("1080347", "FB YFA20025428N HR ORANGE", "FARBY", 0, "kg", "", ""),
        ("1080348", "FARBA YFA1-0035-428N HR YELLOW", "FARBY", 0, "kg", "", ""),
        ("1175729", "YFA3-0318-428N Rubine Red", "FARBY", 0, "kg", "", ""),
        ("881065", "FB YFA00061428N TRANSPARENT WHITE", "FARBY", 0, "kg", "", ""),
        ("881068", "FB YFA10031428N YELLOW", "FARBY", 0, "kg", "", ""),
        ("881069", "FB YFA10080428N PROCESS YELLOW", "FARBY", 0, "kg", "", ""),
        ("881070", "ENC ORANGE 021 YFA20033", "FARBY", 0, "kg", "", ""),
        ("881072", "FB YFA30009428N HR RHODAMINE RED", "FARBY", 0, "kg", "", ""),
        ("881073", "FB YFA30014428N HR RED 032", "FARBY", 0, "kg", "", ""),
        ("881076", "FB YFA30080428N PROCESS MAGENTA", "FARBY", 0, "kg", "", ""),
        ("881077", "YFA3-0284-428N RHODAMINE RED", "FARBY", 0, "kg", "", ""),
        ("881078", "FB YFA40010428N HR VIOLET", "FARBY", 0, "kg", "", ""),
        ("881079", "FB YFA40012428N HR PURPLE", "FARBY", 0, "kg", "", ""),
        ("881080", "FB YFA50021428N REFLEX BLUE", "FARBY", 0, "kg", "", ""),
        ("881081", "FB YFA50022428N PROCESS BLUE", "FARBY", 0, "kg", "", ""),
        ("881082", "FB YFA50072428N BLUE 072", "FARBY", 0, "kg", "", ""),
        ("881083", "FB YFA50080428N PROCESS CYAN", "FARBY", 0, "kg", "", ""),
        ("881084", "FB YFA60051428N GREEN", "FARBY", 0, "kg", "", ""),
        ("881085", "FB YFA80071428N BLACK", "FARBY", 0, "kg", "", ""),
        ("881086", "FB YFA80080428N PROCESS BLACK", "FARBY", 0, "kg", "", ""),
        ("881063", "FB YFM00004408N RICH GOLD", "FARBY", 0, "kg", "", ""),
        ("881067", "FB YFM00087408N SILVER", "FARBY", 0, "kg", "", ""),
        ("865528", "SHOCK DIVA RED 81-813339-9.2730", "FARBY", 0, "kg", "", ""),
        ("1288807", "Farba sitowa USW9-009-408N - Combi White", "FARBY", 0, "kg", "", ""),
        ("1341850", "FB SRSN50:SUNMATCH BLACK:DK02 91378608", "FARBY", 0, "kg", "", ""),
        ("1341854", "FB SRSE50:SUNMATCH BASE:DK02 91378720", "FARBY", 0, "kg", "", ""),
        ("1341855", "FB UVOSCREEN ELITE YELLOW USE1-0031-408N", "FARBY", 0, "kg", "", ""),
        ("865437", "FB FLEXO SRSF 54-BIEL KRYJACA SUNCHEMI91", "FARBY", 0, "kg", "", ""),
        ("951741", "FARBA FLEXO SRSB50 SUNMATCHBLUE 91378450", "FARBY", 0, "kg", "", ""),
        ("951742", "FARBA FLEXO SRSV50 SUNMATVIOLET 91378210", "FARBY", 0, "kg", "", ""),
        ("955654", "FARBA FLEX SRSR54 91378029 SUNMA MID RED", "FARBY", 0, "kg", "", ""),
        ("1061527", "GLOSS VARNISH LM YVX0-0121-107N", "LAKIERY", 0, "kg", "", ""),
        ("1119596", "YVX0-0124 MATT VARNISH", "LAKIERY", 0, "kg", "", ""),
        ("1278612", "Varnish Weilb Gloss 82N 1000 (ex360881)", "LAKIERY", 0, "kg", "", ""),
        ("1484525", "Varnish Weilb Matt 22F 1000 (ex360047)", "LAKIERY", 0, "kg", "", ""),
        ("552177", "LAK Siegw 85-601164-8 (Sche 54205) MATT", "LAKIERY", 0, "kg", "", ""),
        ("625445", "LAK Siegw 85-601168-9 (4.9722) MATT", "LAKIERY", 0, "kg", "", ""),
        ("677035", "VARNISH GL 85-601223-2.2360 sieg", "LAKIERY", 0, "kg", "", ""),
        ("865442", "LAKIER 60UC9203 MAT HUBER", "LAKIERY", 0, "kg", "", ""),
        ("866141", "LAKIER 60UC9230 GLOSS HUBER 60IVHEG03", "LAKIERY", 0, "kg", "", ""),
        ("865491", "VARNISH MAT 85-601224-0-1670 siegwerk", "LAKIERY", 0, "kg", "", ""),
        ("881054", "G006072 SATIN VARNISH LVHO11290", "LAKIERY", 0, "kg", "", ""),
        ("1160982", "FB UAA00117410N ADHESION PROMOTOR", "DODATKI", 0, "kg", "", ""),
        ("1487566", "YAA0-0102-409N FCM UV ADD ,FLUORES CON B", "DODATKI", 0, "kg", "", ""),
        ("866168", "FB UVH00007408N GREEN TINT ADHESIVE", "DODATKI", 0, "kg", "", ""),
        ("928526", "FB YVH00001405N COLD FOIL ADHESIVE", "DODATKI", 0, "kg", "", ""),
        ("1127657", "FW UV SOLVENT CLEANER 4", "CHEMIA", 0, "l", "", ""),
        ("1202360", "PLATE WASH do mycia polimerów", "CHEMIA", 0, "l", "", ""),
        ("865456", "OCTAN ETYLU", "CHEMIA", 0, "l", "", ""),
        ("865562", "ACETON TECHNICZNY", "CHEMIA", 0, "l", "", ""),
        ("865574", "PRINTER CLEAN", "CHEMIA", 0, "l", "", ""),
    ]
    for kod, nazwa, kategoria, ilosc, jednostka, lokalizacja, uwagi in defaults:
        cur.execute(
            "SELECT 1 FROM komponenty WHERE kod=? LIMIT 1",
            (kod,),
        )
        if cur.fetchone() is None:
            cur.execute(
                """
                INSERT INTO komponenty (kod, nazwa, kategoria, ilosc, jednostka, lokalizacja, uwagi, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'dostepny')
                """,
                (kod, nazwa, kategoria, ilosc, jednostka, lokalizacja, uwagi),
            )


@router.get("/komponenty")
def komponenty(
    request: Request,
    search: str = Query(""),
    status: str = Query(""),
    category: str = Query(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_manage_components(user):
        return RedirectResponse("/dashboard", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    _seed_components(cur)
    conn.commit()

    cur.execute(
        """
        SELECT * FROM komponenty
        WHERE (? = '' OR kod LIKE ? OR nazwa LIKE ?)
          AND (? = '' OR status = ?)
          AND (? = '' OR UPPER(COALESCE(kategoria, '')) = UPPER(?))
        ORDER BY nazwa ASC
        """,
        (search, f"%{search}%", f"%{search}%", status, status, category, category),
    )
    components = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT DISTINCT kategoria FROM komponenty WHERE kategoria IS NOT NULL AND kategoria <> '' ORDER BY kategoria")
    categories = [row[0] for row in cur.fetchall()]
    edit_component = None
    if request.query_params.get("edit_id"):
        try:
            edit_id = int(request.query_params.get("edit_id"))
        except ValueError:
            edit_id = None
        if edit_id is not None and _can_edit_components(user):
            cur.execute("SELECT * FROM komponenty WHERE id=?", (edit_id,))
            edit_row = cur.fetchone()
            if edit_row:
                edit_component = dict(edit_row)
    return render_template("komponenty.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "components": components,
        "search": search,
        "status": status,
        "category": category,
        "categories": categories,
        "component_categories": COMPONENT_CATEGORIES,
        "can_edit_catalog": _can_edit_components(user),
        "edit_component": edit_component,
        "success": request.query_params.get("success", ""),
        "error": request.query_params.get("error", ""),
    })


@router.post("/komponenty/dodaj")
def komponenty_dodaj(
    request: Request,
    kod: str = Form(...),
    nazwa: str = Form(...),
    kategoria: str = Form(""),
    ilosc: float = Form(0),
    jednostka: str = Form("szt."),
    lokalizacja: str = Form(""),
    uwagi: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_edit_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute(
        """
        INSERT INTO komponenty (kod, nazwa, kategoria, ilosc, jednostka, lokalizacja, uwagi, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'dostepny')
        """,
        (kod.strip(), nazwa.strip(), _normalize_component_category(kategoria), ilosc, jednostka.strip() or "szt.", lokalizacja.strip(), uwagi.strip()),
    )
    conn.commit()
    return RedirectResponse("/komponenty?success=dodano", status_code=303)


@router.post("/komponenty/{component_id}/edytuj")
def komponenty_edytuj(
    component_id: int,
    request: Request,
    kod: str = Form(...),
    nazwa: str = Form(...),
    kategoria: str = Form(""),
    ilosc: float = Form(0),
    jednostka: str = Form("szt."),
    lokalizacja: str = Form(""),
    uwagi: str = Form(""),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_edit_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute(
        """
        UPDATE komponenty
        SET kod=?, nazwa=?, kategoria=?, ilosc=?, jednostka=?, lokalizacja=?, uwagi=?, updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (kod.strip(), nazwa.strip(), _normalize_component_category(kategoria), ilosc, jednostka.strip() or "szt.", lokalizacja.strip(), uwagi.strip(), component_id),
    )
    conn.commit()
    return RedirectResponse("/komponenty?success=zaktualizowano", status_code=303)


@router.post("/komponenty/{component_id}/pobierz")
def komponenty_pobierz(
    component_id: int,
    request: Request,
    ilosc: float = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_manage_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute("SELECT ilosc, status FROM komponenty WHERE id=?", (component_id,))
    row = cur.fetchone()
    if not row:
        return RedirectResponse("/komponenty?error=notfound", status_code=303)
    if row["ilosc"] < ilosc:
        return RedirectResponse("/komponenty?error=za_malo", status_code=303)
    new_qty = row["ilosc"] - ilosc
    cur.execute(
        "UPDATE komponenty SET ilosc=?, status=? , updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (new_qty, "dostepny" if new_qty > 0 else "wyczerpany", component_id),
    )
    conn.commit()
    return RedirectResponse("/komponenty?success=pobrano", status_code=303)


@router.post("/komponenty/{component_id}/zwrot")
def komponenty_zwrot(
    component_id: int,
    request: Request,
    ilosc: float = Form(...),
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_manage_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute("SELECT ilosc FROM komponenty WHERE id=?", (component_id,))
    row = cur.fetchone()
    if not row:
        return RedirectResponse("/komponenty?error=notfound", status_code=303)
    new_qty = row["ilosc"] + ilosc
    cur.execute(
        "UPDATE komponenty SET ilosc=?, status='dostepny', updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (new_qty, component_id),
    )
    conn.commit()
    return RedirectResponse("/komponenty?success=zwrocono", status_code=303)


@router.post("/komponenty/migracja")
def komponenty_migracja(
    request: Request,
    user=Depends(require_auth),
    conn=Depends(get_db),
):
    if not _can_edit_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute("SELECT id, nazwa, kategoria FROM komponenty")
    migrated = 0
    for row in cur.fetchall():
        new_category = _normalize_component_category(row["kategoria"])
        if row["kategoria"] != new_category:
            cur.execute(
                "UPDATE komponenty SET kategoria=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (new_category, row["id"]),
            )
            migrated += 1
    conn.commit()
    return RedirectResponse(f"/komponenty?success=migracja_{migrated}", status_code=303)
