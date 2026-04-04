#!/usr/bin/env python3
"""
Kopiuje dane z lokalnego SQLite (źródłowa aplikacja) do PostgreSQL (np. Render).

Wymaga:
  - pliku database.db ze źródła (ścieżka --sqlite lub zmienna SQLITE_SOURCE)
  - DATABASE_URL wskazującego na Postgres (np. z panelu Render — External URL)

Uruchomienie (z katalogu deploy_render lub z root projektu):
  set DATABASE_URL=postgresql://...
  python migrate_sqlite_to_postgres.py --sqlite "..\\database.db"

Opcje:
  --dry-run     tylko liczby wierszy, bez zapisu
  --no-truncate nie czyści Postgresa (możliwe duplikaty PK — używaj świadomie)
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("Zainstaluj: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def normalize_database_url(url: str) -> str:
    u = url.strip()
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    return u


def default_sqlite_candidates() -> list[Path]:
    here = Path(__file__).resolve().parent
    return [
        here.parent / "database.db",
        here / "database.db",
        Path.cwd() / "database.db",
    ]


def resolve_sqlite_path(arg: str | None) -> Path:
    if arg:
        p = Path(arg).expanduser().resolve()
        if not p.is_file():
            raise SystemExit(f"Brak pliku SQLite: {p}")
        return p
    env = os.environ.get("SQLITE_SOURCE", "").strip()
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_file():
            raise SystemExit(f"SQLITE_SOURCE — brak pliku: {p}")
        return p
    for p in default_sqlite_candidates():
        if p.is_file():
            return p.resolve()
    raise SystemExit(
        "Nie znaleziono database.db. Podaj --sqlite ŚCIEŻKA lub ustaw SQLITE_SOURCE.\n"
        f"Szukane: {', '.join(str(p) for p in default_sqlite_candidates())}"
    )


# Kolejność wstawiania (klucze obce → rodzice przed dziećmi)
TABLES_ORDER = [
    "users",
    "shifts",
    "notification_settings",
    "farby",
    "operacje",
    "polymers",
    "polymer_operations",
    "production_plans",
    "notifications",
    "production_reports",
    "print_control_reports",
    "production_log",
    "events",
]


def sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return [row[1] for row in cur.fetchall()]


def pg_quote_ident(name: str) -> str:
    if name == "user":
        return '"user"'
    return '"' + name.replace('"', '""') + '"'


def table_exists_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def truncate_postgres(cur, dry_run: bool) -> None:
    stmt = (
        "TRUNCATE "
        + ", ".join(TABLES_ORDER)
        + " RESTART IDENTITY CASCADE"
    )
    if dry_run:
        print(f"[dry-run] {stmt}")
        return
    cur.execute(stmt)


def ensure_postgres_schema(pg_conn) -> None:
    """Tworzy tabele jak w aplikacji (idempotentnie)."""
    from db_compat import init_postgres_schema, migrate_schema_postgres

    cur = pg_conn.cursor()
    init_postgres_schema(cur)
    migrate_schema_postgres(cur)
    pg_conn.commit()
    cur.close()


def copy_table(
    sl_conn: sqlite3.Connection,
    pg_cur,
    table: str,
    columns: list[str],
    dry_run: bool,
) -> int:
    if not columns:
        return 0
    sl_cur = sl_conn.execute(f'SELECT * FROM "{table}"')
    rows = sl_cur.fetchall()
    if not rows:
        return 0
    col_sql = ", ".join(pg_quote_ident(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {pg_quote_ident(table)} ({col_sql}) VALUES ({placeholders})"
    n = 0
    for row in rows:
        vals = tuple(row[c] for c in columns)
        if not dry_run:
            pg_cur.execute(insert_sql, vals)
        n += 1
    return n


def sync_serial_sequences(pg_cur) -> None:
    """Ustawia sekwencje SERIAL po ręcznym INSERT z id."""
    for table in TABLES_ORDER:
        pg_cur.execute(
            "SELECT pg_get_serial_sequence(%s, 'id')",
            (table,),
        )
        row = pg_cur.fetchone()
        if not row or row[0] is None:
            continue
        seq = row[0]
        pg_cur.execute(sql.SQL("SELECT MAX(id) FROM {}").format(sql.Identifier(table)))
        m = pg_cur.fetchone()[0]
        if m is None:
            pg_cur.execute("SELECT setval(%s, 1, false)", (seq,))
        else:
            pg_cur.execute("SELECT setval(%s, %s, true)", (seq, m))


def main() -> None:
    parser = argparse.ArgumentParser(description="Migracja SQLite → PostgreSQL")
    parser.add_argument(
        "--sqlite",
        help="Ścieżka do database.db (domyślnie: katalog nadrzędny deploy_render, deploy_render lub CWD)",
    )
    parser.add_argument(
        "--database-url",
        help="Nadpisuje DATABASE_URL z środowiska",
    )
    parser.add_argument("--dry-run", action="store_true", help="Tylko podsumowanie, bez zapisu (wymaga działającego Postgres)")
    parser.add_argument(
        "--counts-only",
        action="store_true",
        help="Tylko liczby wierszy w pliku SQLite — bez DATABASE_URL i bez Postgres",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Nie czyść tabel w Postgresie przed kopią (ryzyko konfliktów PK)",
    )
    parser.add_argument(
        "--no-schema",
        action="store_true",
        help="Nie twórz/aktualizuj schematu (zakładamy gotowe tabele)",
    )
    args = parser.parse_args()

    sqlite_path = resolve_sqlite_path(args.sqlite)
    print(f"SQLite (źródło): {sqlite_path}")

    os.chdir(Path(__file__).resolve().parent)
    sys.path.insert(0, str(Path(__file__).resolve().parent))

    if args.counts_only:
        sl = sqlite3.connect(str(sqlite_path))
        sl.row_factory = sqlite3.Row
        print("\n--- Liczby wierszy (tylko SQLite) ---")
        total = 0
        for table in TABLES_ORDER:
            if not table_exists_sqlite(sl, table):
                print(f"  {table}: (brak tabeli)")
                continue
            n = sl.execute(f'SELECT COUNT(*) AS c FROM "{table}"').fetchone()["c"]
            print(f"  {table}: {n}")
            total += n
        print(f"  SUMA: {total}")
        sl.close()
        return

    url = normalize_database_url(args.database_url or os.environ.get("DATABASE_URL", ""))
    if not url:
        sys.exit("Ustaw zmienną DATABASE_URL lub użyj --database-url")
    os.environ["DATABASE_URL"] = url

    print(f"PostgreSQL:      {url.split('@')[-1] if '@' in url else url}")

    sl = sqlite3.connect(str(sqlite_path))
    sl.row_factory = sqlite3.Row
    pg = psycopg2.connect(url)
    try:
        if not args.no_schema and not args.dry_run:
            ensure_postgres_schema(pg)

        pg_cur = pg.cursor()

        if not args.no_truncate and not args.dry_run:
            truncate_postgres(pg_cur, dry_run=False)
            pg.commit()
        elif not args.no_truncate and args.dry_run:
            truncate_postgres(pg_cur, dry_run=True)

        total = 0
        summary: list[tuple[str, int]] = []

        for table in TABLES_ORDER:
            if not table_exists_sqlite(sl, table):
                summary.append((table, -1))
                continue
            cols = sqlite_columns(sl, table)
            if not cols:
                summary.append((table, 0))
                continue
            try:
                n = copy_table(sl, pg_cur, table, cols, dry_run=args.dry_run)
            except Exception as e:
                pg.rollback()
                raise RuntimeError(f"Błąd przy kopiowaniu tabeli {table}: {e}") from e
            summary.append((table, n))
            total += n

        if not args.dry_run:
            sync_serial_sequences(pg_cur)
            pg.commit()
        else:
            pg.rollback()

        print("\n--- Podsumowanie ---")
        for t, n in summary:
            if n < 0:
                print(f"  {t}: (brak tabeli w SQLite)")
            else:
                print(f"  {t}: {n} wierszy")
        print(f"  SUMA: {total}")
        if args.dry_run:
            print("\n(dry-run — nic nie zapisano w Postgresie)")
        else:
            print("\nGotowe.")
    finally:
        sl.close()
        pg.close()


if __name__ == "__main__":
    main()
