"""
Router: magazyn komponentów dla operatora mieszalni.
"""
from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import RedirectResponse
from starlette.requests import Request

from dependencies import get_db, require_auth
from helpers import render_template

router = APIRouter()


def _can_manage_components(user: dict) -> bool:
    return user.get("role") in ("operator_mieszalni", "manager", "admin")


def _ensure_table(cur):
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
        ("1052994", "FB YFA90095408N MIXING OPAQUE WHITE", "farba", 0, "kg", "", ""),
        ("1062547", "FB FLEXO SILVER SMET022: ULTRABRIGHT", "farba", 0, "kg", "", ""),
        ("1067241", "FB YFA90092418N BT OPAQUE WHITE", "farba", 0, "kg", "", ""),
        ("1080346", "FB YFA30007428N HR WARM RED", "farba", 0, "kg", "", ""),
        ("1080347", "FB YFA20025428N HR ORANGE", "farba", 0, "kg", "", ""),
        ("1080348", "FARBA YFA1-0035-428N HR YELLOW", "farba", 0, "kg", "", ""),
        ("1175729", "YFA3-0318-428N Rubine Red", "farba", 0, "kg", "", ""),
        ("881065", "FB YFA00061428N TRANSPARENT WHITE", "farba", 0, "kg", "", ""),
        ("881068", "FB YFA10031428N YELLOW", "farba", 0, "kg", "", ""),
        ("881069", "FB YFA10080428N PROCESS YELLOW", "farba", 0, "kg", "", ""),
        ("881070", "ENC ORANGE 021 YFA20033", "farba", 0, "kg", "", ""),
        ("881072", "FB YFA30009428N HR RHODAMINE RED", "farba", 0, "kg", "", ""),
        ("881073", "FB YFA30014428N HR RED 032", "farba", 0, "kg", "", ""),
        ("881076", "FB YFA30080428N PROCESS MAGENTA", "farba", 0, "kg", "", ""),
        ("881077", "YFA3-0284-428N RHODAMINE RED", "farba", 0, "kg", "", ""),
        ("881078", "FB YFA40010428N HR VIOLET", "farba", 0, "kg", "", ""),
        ("881079", "FB YFA40012428N HR PURPLE", "farba", 0, "kg", "", ""),
        ("881080", "FB YFA50021428N REFLEX BLUE", "farba", 0, "kg", "", ""),
        ("881081", "FB YFA50022428N PROCESS BLUE", "farba", 0, "kg", "", ""),
        ("881082", "FB YFA50072428N BLUE 072", "farba", 0, "kg", "", ""),
        ("881083", "FB YFA50080428N PROCESS CYAN", "farba", 0, "kg", "", ""),
        ("881084", "FB YFA60051428N GREEN", "farba", 0, "kg", "", ""),
        ("881085", "FB YFA80071428N BLACK", "farba", 0, "kg", "", ""),
        ("881086", "FB YFA80080428N PROCESS BLACK", "farba", 0, "kg", "", ""),
        ("881063", "FB YFM00004408N RICH GOLD", "farba", 0, "kg", "", ""),
        ("881067", "FB YFM00087408N SILVER", "farba", 0, "kg", "", ""),
        ("865528", "SHOCK DIVA RED 81-813339-9.2730", "farba", 0, "kg", "", ""),
        ("1288807", "Farba sitowa USW9-009-408N - Combi White", "farba", 0, "kg", "", ""),
        ("1341850", "FB SRSN50:SUNMATCH BLACK:DK02 91378608", "farba", 0, "kg", "", ""),
        ("1341854", "FB SRSE50:SUNMATCH BASE:DK02 91378720", "farba", 0, "kg", "", ""),
        ("1341855", "FB UVOSCREEN ELITE YELLOW USE1-0031-408N", "farba", 0, "kg", "", ""),
        ("865437", "FB FLEXO SRSF 54-BIEL KRYJACA SUNCHEMI91", "farba", 0, "kg", "", ""),
        ("951741", "FARBA FLEXO SRSB50 SUNMATCHBLUE 91378450", "farba", 0, "kg", "", ""),
        ("951742", "FARBA FLEXO SRSV50 SUNMATVIOLET 91378210", "farba", 0, "kg", "", ""),
        ("955654", "FARBA FLEX SRSR54 91378029 SUNMA MID RED", "farba", 0, "kg", "", ""),
        ("1061527", "GLOSS VARNISH LM YVX0-0121-107N", "lakier", 0, "kg", "", ""),
        ("1119596", "YVX0-0124 MATT VARNISH", "lakier", 0, "kg", "", ""),
        ("1278612", "Varnish Weilb Gloss 82N 1000 (ex360881)", "lakier", 0, "kg", "", ""),
        ("1484525", "Varnish Weilb Matt 22F 1000 (ex360047)", "lakier", 0, "kg", "", ""),
        ("552177", "LAK Siegw 85-601164-8 (Sche 54205) MATT", "lakier", 0, "kg", "", ""),
        ("625445", "LAK Siegw 85-601168-9 (4.9722) MATT", "lakier", 0, "kg", "", ""),
        ("677035", "VARNISH GL 85-601223-2.2360 sieg", "lakier", 0, "kg", "", ""),
        ("865442", "LAKIER 60UC9203 MAT HUBER", "lakier", 0, "kg", "", ""),
        ("866141", "LAKIER 60UC9230 GLOSS HUBER 60IVHEG03", "lakier", 0, "kg", "", ""),
        ("865491", "VARNISH MAT 85-601224-0-1670 siegwerk", "lakier", 0, "kg", "", ""),
        ("881054", "G006072 SATIN VARNISH LVHO11290", "lakier", 0, "kg", "", ""),
        ("1160982", "FB UAA00117410N ADHESION PROMOTOR", "adhesive", 0, "kg", "", ""),
        ("1487566", "YAA0-0102-409N FCM UV ADD ,FLUORES CON B", "adhesive", 0, "kg", "", ""),
        ("866168", "FB UVH00007408N GREEN TINT ADHESIVE", "adhesive", 0, "kg", "", ""),
        ("928526", "FB YVH00001405N COLD FOIL ADHESIVE", "adhesive", 0, "kg", "", ""),
        ("1127657", "FW UV SOLVENT CLEANER 4", "chemia", 0, "l", "", ""),
        ("1202360", "PLATE WASH do mycia polimerów", "chemia", 0, "l", "", ""),
        ("865456", "OCTAN ETYLU", "chemia", 0, "l", "", ""),
        ("865562", "ACETON TECHNICZNY", "chemia", 0, "l", "", ""),
        ("865574", "PRINTER CLEAN", "chemia", 0, "l", "", ""),
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
          AND (? = '' OR kategoria = ?)
        ORDER BY nazwa ASC
        """,
        (search, f"%{search}%", f"%{search}%", status, status, category, category),
    )
    components = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT DISTINCT kategoria FROM komponenty WHERE kategoria IS NOT NULL AND kategoria <> '' ORDER BY kategoria")
    categories = [row[0] for row in cur.fetchall()]
    return render_template("komponenty.html", {
        "user": {"username": user["username"], "role": user["role"]},
        "components": components,
        "search": search,
        "status": status,
        "category": category,
        "categories": categories,
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
    if not _can_manage_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute(
        """
        INSERT INTO komponenty (kod, nazwa, kategoria, ilosc, jednostka, lokalizacja, uwagi, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'dostepny')
        """,
        (kod.strip(), nazwa.strip(), kategoria.strip(), ilosc, jednostka.strip() or "szt.", lokalizacja.strip(), uwagi.strip()),
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
    if not _can_manage_components(user):
        return RedirectResponse("/komponenty?error=brak_dostepu", status_code=303)
    cur = conn.cursor()
    _ensure_table(cur)
    cur.execute(
        """
        UPDATE komponenty
        SET kod=?, nazwa=?, kategoria=?, ilosc=?, jednostka=?, lokalizacja=?, uwagi=?, updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (kod.strip(), nazwa.strip(), kategoria.strip(), ilosc, jednostka.strip() or "szt.", lokalizacja.strip(), uwagi.strip(), component_id),
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
