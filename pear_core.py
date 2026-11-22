"""Shared reliability primitives for Pear CM.

This module deliberately depends only on the Python standard library so its
parsers, persistent activity spool, and schema checks can be tested offline.
"""
from __future__ import annotations

import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

_SPOOL_LOCK = threading.Lock()
_SPOOL_PATH = Path(os.getenv("PEAR_ACTIVITY_SPOOL_PATH", "pear_activity_spool.sqlite3"))

def _spool_connect() -> sqlite3.Connection:
    _SPOOL_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_SPOOL_PATH), timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS activity_spool (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(chat_id, message_id)
        )
    """)
    return conn

@contextmanager
def _open_spool():
    """Commit/rollback and always close each short-lived SQLite connection."""
    conn = _spool_connect()
    try:
        with conn:
            yield conn
    finally:
        conn.close()

def spool_activity_batch(items: Iterable[tuple[int, int, str | None, str, str, int]]) -> bool:
    """Durably spool a batch of message events in one SQLite transaction."""
    rows = []
    for item in items:
        chat_id, user_id, username, first_name, created_at, message_id = item
        rows.append((
            int(chat_id), int(message_id), int(user_id), username,
            first_name or "Пользователь", str(created_at),
        ))
    if not rows:
        return True
    try:
        with _SPOOL_LOCK, _open_spool() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO activity_spool "
                "(chat_id,message_id,user_id,username,first_name,created_at) VALUES (?,?,?,?,?,?)",
                rows,
            )
        return True
    except Exception:
        return False

def spool_activity(item: tuple[int, int, str | None, str, str, int]) -> bool:
    """Compatibility wrapper for persisting a single event."""
    return spool_activity_batch([item])

def acknowledge_activity(items: Iterable[tuple[int, int, str | None, str, str, int]]) -> None:
    keys = [(int(item[0]), int(item[5])) for item in items]
    if not keys:
        return
    try:
        with _SPOOL_LOCK, _spool_connect() as conn:
            conn.executemany("DELETE FROM activity_spool WHERE chat_id=? AND message_id=?", keys)
    except Exception:
        # Leaving rows in the spool is safe: the Postgres RPC is idempotent.
        pass

def pending_activity(limit: int = 1000) -> list[tuple[int, int, str | None, str, str, int]]:
    try:
        with _SPOOL_LOCK, _spool_connect() as conn:
            rows = conn.execute(
                "SELECT chat_id,user_id,username,first_name,created_at,message_id "
                "FROM activity_spool ORDER BY rowid LIMIT ?", (max(1, int(limit)),)
            ).fetchall()
        return [(int(c), int(u), un, str(fn or "Пользователь"), str(ts), int(mid))
                for c, u, un, fn, ts, mid in rows]
    except Exception:
        return []

def activity_spool_size() -> int:
    try:
        with _SPOOL_LOCK, _spool_connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM activity_spool").fetchone()[0])
    except Exception:
        return -1

_NUMBER_WORDS = {
    "один": 1, "одна": 1, "одно": 1, "одну": 1, "одного": 1, "одной": 1, "одном": 1,
    "раз": 1, "два": 2, "две": 2, "двух": 2, "двум": 2, "три": 3, "трех": 3, "трёх": 3,
    "четыре": 4, "четырех": 4, "четырёх": 4, "пять": 5, "пяти": 5, "шесть": 6, "шести": 6,
    "семь": 7, "семи": 7, "восемь": 8, "восьми": 8, "девять": 9, "девяти": 9,
    "десять": 10, "десяти": 10, "одиннадцать": 11, "двенадцать": 12, "тринадцать": 13,
    "четырнадцать": 14, "пятнадцать": 15, "шестнадцать": 16, "семнадцать": 17,
    "восемнадцать": 18, "девятнадцать": 19, "двадцать": 20,
}
_UNIT_SECONDS = {
    "second": 1, "minute": 60, "hour": 3600, "day": 86400,
    "week": 604800, "month": 2592000, "year": 31536000,
}
def unit_seconds(unit: str) -> int | None:
    u = (unit or "").casefold().strip(".,;:!?")
    compact = {"с":1,"сек":1,"секунда":1,"секунды":1,"секунд":1,"секунду":1,"секундами":1,"секундах":1,
      "мс":0,"м":60,"мин":60,"минута":60,"минуты":60,"минут":60,"минуту":60,"минутами":60,"минутах":60,
      "ч":3600,"ч.":3600,"час":3600,"часа":3600,"часов":3600,"часу":3600,"часе":3600,"часами":3600,"часах":3600,
      "д":86400,"дн":86400,"день":86400,"дня":86400,"дней":86400,"дню":86400,"днем":86400,"днём":86400,
      "сутки":86400,"суток":86400,"сутки":86400,"сутками":86400,"н":604800,"нед":604800,"неделя":604800,
      "недели":604800,"неделю":604800,"недель":604800,"неделями":604800,"неделях":604800,
      "мес":2592000,"месяц":2592000,"месяца":2592000,"месяцев":2592000,"месяцу":2592000,"месяцами":2592000,"месяцах":2592000,
      "г":31536000,"год":31536000,"года":31536000,"лет":31536000,"году":31536000,"годом":31536000,"годами":31536000,"годах":31536000,
      "s":1,"sec":1,"secs":1,"second":1,"seconds":1,"m":60,"min":60,"mins":60,"minute":60,"minutes":60,
      "h":3600,"hr":3600,"hrs":3600,"hour":3600,"hours":3600,"d":86400,"day":86400,"days":86400,
      "w":604800,"week":604800,"weeks":604800,"month":2592000,"months":2592000,"y":31536000,"year":31536000,"years":31536000}
    if u in compact:
        return compact[u]
    # Match inflected Russian cases by longest unit stem first.
    stems = [
        ("секунд",1),("секундоч",1),("минут",60),("минуточ",60),("час",3600),("часик",3600),
        ("дн",86400),("день",86400),("дня",86400),("сут",86400),("недел",604800),
        ("месяц",2592000),("месяц",2592000),("год",31536000),("лет",31536000),
        ("second",1),("minute",60),("hour",3600),("day",86400),("week",604800),("month",2592000),("year",31536000),
    ]
    for stem, seconds in stems:
        if len(stem) >= 3 and u.startswith(stem):
            return seconds
    return None

def parse_duration_prefix(tokens: list[str]) -> tuple[timedelta, int, str] | None:
    """Parse compact, numeric, and inflected Russian durations; return duration, token count, label."""
    total = 0
    consumed = 0
    labels: list[str] = []
    i = 0
    words = list(tokens)
    while i < len(words):
        token = words[i].strip(".,;:!?").casefold()
        # Accept compact combined forms such as «1ч30м» or «2д3ч» as one token.
        combined_parts = re.findall(r"(\d{1,7})(сек|секунд|мин|минут|час|ч|дн|д|нед|н|мес|месяц|г|год|лет|с|м|h|m|d|w|y)", token, re.I)
        if combined_parts and "".join(amount + unit for amount, unit in combined_parts).casefold() == token:
            combined_seconds = 0
            combined_labels = []
            valid_combined = True
            for amount_text, unit_text in combined_parts:
                seconds_part = unit_seconds(unit_text)
                if seconds_part is None:
                    valid_combined = False
                    break
                combined_seconds += int(amount_text) * seconds_part
                combined_labels.append(f"{amount_text} {unit_text}")
            if valid_combined and combined_seconds > 0:
                total += combined_seconds
                labels.extend(combined_labels)
                consumed += 1
                i += 1
                continue
        if total == 0 and token in {"на", "сроком"}:
            consumed += 1; i += 1
            continue
        if total == 0 and token == "в" and i + 1 < len(words) and words[i+1].casefold().strip(".,;:") == "течение":
            consumed += 2; i += 2
            continue
        if token == "и" and total:
            consumed += 1; i += 1
            continue
        if token in {"полчаса", "полчасика"}:
            amount, seconds, used, label = 0.5, 3600, 1, "30 минут"
        elif token in {"полдня", "полдень"}:
            amount, seconds, used, label = 0.5, 86400, 1, "12 часов"
        elif token in {"полминуты", "полминутки"}:
            amount, seconds, used, label = 0.5, 60, 1, "30 секунд"
        else:
            m = re.fullmatch(r"(\d{1,7})([a-zа-яё.]+)?", token, re.I)
            amount_word = _NUMBER_WORDS.get(token)
            bare_unit_seconds = unit_seconds(token)
            if m:
                amount = int(m.group(1)); unit = m.group(2); used = 1
            elif amount_word is not None:
                amount = amount_word; unit = None; used = 1
            elif bare_unit_seconds is not None:
                amount = 1; seconds = bare_unit_seconds; used = 1
                label = f"1 {token}"
                unit = None
            else:
                break
            if unit is None and (amount_word is not None or (m is not None and m.group(2) is None and bare_unit_seconds is None)):
                if i + used >= len(words):
                    break
                unit = words[i+used].strip(".,;:!?").casefold()
                seconds = unit_seconds(unit)
                if seconds is None:
                    break
                used += 1
            elif unit is not None:
                seconds = unit_seconds(unit)
                if seconds is None:
                    break
            if not (bare_unit_seconds is not None and amount_word is None and m is None):
                label = f"{amount} {unit}"
        if amount < 1 and not (token.startswith("пол") and amount == 0.5):
            return None
        total += int(amount * seconds)
        labels.append(label)
        consumed += used
        i += used
        if i < len(words):
            nxt = words[i].strip(".,;:!?").casefold()
            if nxt != "и" and not re.match(r"^\d", nxt) and nxt not in _NUMBER_WORDS and unit_seconds(nxt) is None and nxt not in {"полчаса","полчасика","полдня","полминуты","полминутки"}:
                break
    if consumed == 0 or total <= 0 or total > 365 * 86400:
        return None
    return timedelta(seconds=total), consumed, " ".join(labels)

def format_panel(title: str, body: str, footer: str = "GDE.WTF · Модерация") -> str:
    """Render every bot panel with the same compact, modern visual hierarchy."""
    title = title.strip()
    body = body.strip()
    footer = footer.strip()
    # Avoid ASCII frames and oversized separators: Telegram typography does the work.
    return f"✦ <b>{title}</b>\n\n{body}\n\n<blockquote>↳ {footer}</blockquote>"

def check_schema(client: Any) -> list[str]:
    """Check core PostgREST tables/columns without mutating the database."""
    required = {
        "bot_users": "user_id,chat_id,username,first_name,warns,rank,messages,last_seen",
        "bot_chat_settings": "chat_id,settings,updated_at",
        "bot_audit": "chat_id,actor_id,action,target_id,details,created_at",
        "bot_activity_events": "chat_id,user_id,message_id,username,first_name,created_at",
        "bot_activity_daily": "chat_id,user_id,day,username,first_name,messages,updated_at",
        "bot_member_profiles": "chat_id,user_id,display_name,interests,about,updated_at",
        "bot_marriages": "chat_id,user1_id,user1_name,user2_id,user2_name,status,created_at,accepted_at",
        "bot_global_bans": "user_id,active,reason,actor_id,created_at,expires_at",
        "bot_operators": "user_id,active,created_by,created_at",
        "bot_reports": "id,chat_id,user_id,reporter_id,target_id,message_id,body,details,status,closed_by,closed_reason,created_at,closed_at",
        "bot_appeals": "id,chat_id,user_id,body,status,closed_by,closed_reason,created_at,closed_at",
        "bot_notes": "id,user_id,content,created_at,chat_id,actor_id",
        "bot_trusted_users": "chat_id,user_id,active,actor_id,created_at",
        "bot_economy": "chat_id,user_id,balance,last_bonus,title,updated_at",
        "bot_clans": "id,chat_id,name,leader_id,created_at",
        "bot_clan_members": "chat_id,user_id,clan_name,role,joined_at",
        "bot_awards": "id,chat_id,user_id,award,actor_id,created_at",
        "bot_relationships": "chat_id,user1_id,user2_id,points,updated_at",
    }
    problems: list[str] = []
    for table, columns in required.items():
        try:
            client.table(table).select(columns).limit(0).execute()
        except Exception as exc:
            problems.append(f"{table}: {type(exc).__name__}: {str(exc)[:180]}")

    # These probes intentionally pass null IDs: the functions return a harmless
    # invalid_request result before making any writes. This catches a missing RPC
    # migration at startup rather than letting moderation/clan commands fail later.
    required_rpcs = {
        "pear_issue_warning": {
            "p_chat_id": None, "p_actor_id": None,
            "p_target_id": None, "p_reason": "schema_probe",
        },
        "pear_clan_action": {
            "p_action": "schema_probe", "p_chat_id": None,
            "p_user_id": None, "p_target_id": None, "p_name": "",
        },
    }
    for function, params in required_rpcs.items():
        try:
            client.rpc(function, params).execute()
        except Exception as exc:
            problems.append(f"RPC {function}: {type(exc).__name__}: {str(exc)[:180]}")
    return problems
