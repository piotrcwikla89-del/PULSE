"""
Wspólne utility czasu dla aplikacji.

- techniczne znaczniki czasu zapisujemy w UTC,
- wartości prezentowane użytkownikowi liczymy w lokalnej strefie.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
LOCAL_TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "Europe/Warsaw"))
DB_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def utc_now() -> datetime:
    return datetime.now(UTC)


def local_now() -> datetime:
    return utc_now().astimezone(LOCAL_TZ)


def local_today() -> date:
    return local_now().date()


def local_date_str(dt: datetime | None = None) -> str:
    value = dt.astimezone(LOCAL_TZ) if dt else local_now()
    return value.strftime("%Y-%m-%d")


def local_time_str(dt: datetime | None = None) -> str:
    value = dt.astimezone(LOCAL_TZ) if dt else local_now()
    return value.strftime("%H:%M:%S")


def local_datetime_str(dt: datetime | None = None, fmt: str = "%Y-%m-%d %H:%M") -> str:
    value = dt.astimezone(LOCAL_TZ) if dt else local_now()
    return value.strftime(fmt)


def utc_now_db_string() -> str:
    return utc_now().strftime(DB_DATETIME_FORMAT)


def utc_threshold_db_string(days: int = 0) -> str:
    return (utc_now() - timedelta(days=days)).strftime(DB_DATETIME_FORMAT)


def local_day_bounds_utc(day: date | str | None = None) -> tuple[str, str]:
    if day is None:
        local_day = local_today()
    elif isinstance(day, str):
        local_day = datetime.strptime(day, "%Y-%m-%d").date()
    else:
        local_day = day
    start_local = datetime.combine(local_day, datetime.min.time(), tzinfo=LOCAL_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(UTC)
    end_utc = end_local.astimezone(UTC)
    return start_utc.strftime(DB_DATETIME_FORMAT), end_utc.strftime(DB_DATETIME_FORMAT)


def parse_datetime_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            try:
                parsed = datetime.strptime(raw[:19].replace("T", " "), DB_DATETIME_FORMAT)
            except ValueError:
                return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def format_local_datetime(value, fmt: str = DB_DATETIME_FORMAT) -> str:
    parsed = parse_datetime_value(value)
    if parsed is None:
        return "" if value is None else str(value)
    return parsed.astimezone(LOCAL_TZ).strftime(fmt)