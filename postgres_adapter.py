"""Small synchronous PostgreSQL adapter exposing the subset of the old query API used by iris.py.
Uses DATABASE_URL only; compatible with Relax Dev's internal PostgreSQL (TLS disabled).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:->>?[A-Za-z_][A-Za-z0-9_]*)?$")

def _ident(value: str) -> str:
    if not _IDENT.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return '"' + value + '"'

def _field(value: str) -> str:
    value = value.strip()
    if not _FIELD.fullmatch(value):
        raise ValueError(f"Invalid SQL field: {value!r}")
    if "->>" in value:
        left, right = value.split("->>", 1)
        return f"{_ident(left)} ->> %s", right
    if "->" in value:
        left, right = value.split("->", 1)
        return f"{_ident(left)} -> %s", right
    return _ident(value), None

def _adapt(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Jsonb(value)
    return value

@dataclass
class Result:
    data: Any

class PostgresQuery:
    def __init__(self, client: "PostgresClient", table: str):
        self.client, self.table = client, table
        _ident(table)
        self.action = "select"
        self.columns = "*"
        self.payload: dict[str, Any] | list[dict[str, Any]] | None = None
        self.conflict: list[str] = []
        self.filters: list[tuple[str, str, Any]] = []
        self.orders: list[tuple[str, bool]] = []
        self.max_rows: int | None = None
        self.offset = 0
        self.return_rows = False

    def select(self, columns: str = "*"):
        self.columns = columns
        if self.action in ("insert", "upsert", "update", "delete"):
            self.return_rows = True
        else:
            self.action = "select"
        return self

    def insert(self, payload):
        self.action, self.payload = "insert", payload
        return self

    def upsert(self, payload, on_conflict: str | None = None):
        self.action, self.payload = "upsert", payload
        self.conflict = [part.strip() for part in (on_conflict or "").split(",") if part.strip()]
        return self

    def update(self, payload):
        self.action, self.payload = "update", payload
        return self

    def delete(self):
        self.action = "delete"
        return self

    def eq(self, column, value): return self._where(column, "=", value)
    def neq(self, column, value): return self._where(column, "<>", value)
    def gt(self, column, value): return self._where(column, ">", value)
    def gte(self, column, value): return self._where(column, ">=", value)
    def lt(self, column, value): return self._where(column, "<", value)
    def lte(self, column, value): return self._where(column, "<=", value)
    def ilike(self, column, value): return self._where(column, "ILIKE", value)
    def like(self, column, value): return self._where(column, "LIKE", value)

    def in_(self, column, values):
        self.filters.append((column, "IN", list(values)))
        return self

    def filter(self, column, operator, value):
        op = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "ilike": "ILIKE", "like": "LIKE"}.get(str(operator).lower())
        if op is None:
            raise ValueError(f"Unsupported filter operator: {operator}")
        return self._where(column, op, value)

    def _where(self, column, operator, value):
        self.filters.append((column, operator, value))
        return self

    def order(self, column, desc: bool = False):
        self.orders.append((column, bool(desc)))
        return self

    def limit(self, count: int):
        self.max_rows = max(0, int(count))
        return self

    def range(self, start: int, end: int):
        self.offset = max(0, int(start))
        self.max_rows = max(0, int(end) - int(start) + 1)
        return self

    def _where_sql(self):
        clauses, params = [], []
        for column, op, value in self.filters:
            expression, extra = _field(column)
            if extra is not None:
                params.append(extra)
            if op == "IN":
                values = list(value)
                if not values:
                    clauses.append("FALSE")
                else:
                    clauses.append(f"{expression} = ANY(%s)")
                    params.append(values)
            elif value is None and op in ("=", "<>"):
                clauses.append(f"{expression} IS {'NOT ' if op == '<>' else ''}NULL")
            else:
                clauses.append(f"{expression} {op} %s")
                params.append(_adapt(value))
        return (" WHERE " + " AND ".join(clauses) if clauses else ""), params

    def execute(self):
        table = "public." + _ident(self.table)
        where, params = self._where_sql()
        sql, values = "", list(params)
        if self.action == "select":
            columns = "*" if self.columns.strip() == "*" else ", ".join(_ident(c.strip()) for c in self.columns.split(","))
            sql = f"SELECT {columns} FROM {table}{where}"
            if self.orders:
                sql += " ORDER BY " + ", ".join(f"{_ident(c)} {'DESC' if d else 'ASC'}" for c, d in self.orders)
            if self.max_rows is not None:
                sql += " LIMIT %s"
                values.append(self.max_rows)
            if self.offset:
                sql += " OFFSET %s"
                values.append(self.offset)
        elif self.action in ("insert", "upsert"):
            rows = self.payload if isinstance(self.payload, list) else [self.payload]
            if not rows:
                return Result([])
            if not all(isinstance(row, dict) and row for row in rows):
                raise ValueError("insert/upsert payload must be a non-empty mapping or list of mappings")
            cols = list(dict.fromkeys(key for row in rows for key in row))
            col_sql = ", ".join(_ident(c) for c in cols)
            placeholders, insert_values = [], []
            for row in rows:
                placeholders.append("(" + ", ".join("%s" for _ in cols) + ")")
                insert_values.extend(_adapt(row.get(c)) for c in cols)
            sql = f"INSERT INTO {table} ({col_sql}) VALUES " + ", ".join(placeholders)
            values = insert_values
            if self.action == "upsert":
                if not self.conflict:
                    raise ValueError("upsert requires on_conflict columns")
                conflict_sql = ", ".join(_ident(c) for c in self.conflict)
                updates = [c for c in cols if c not in self.conflict]
                if updates:
                    sql += f" ON CONFLICT ({conflict_sql}) DO UPDATE SET " + ", ".join(f"{_ident(c)} = EXCLUDED.{_ident(c)}" for c in updates)
                else:
                    sql += f" ON CONFLICT ({conflict_sql}) DO NOTHING"
            sql += " RETURNING *"
        elif self.action == "update":
            if not isinstance(self.payload, dict) or not self.payload:
                raise ValueError("update payload must be a non-empty mapping")
            sql = f"UPDATE {table} SET " + ", ".join(f"{_ident(c)} = %s" for c in self.payload) + where + " RETURNING *"
            values = [_adapt(v) for v in self.payload.values()] + params
        elif self.action == "delete":
            sql = f"DELETE FROM {table}{where} RETURNING *"
        else:
            raise ValueError(f"Unsupported query action {self.action}")
        with self.client.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                data = cur.fetchall() if cur.description else []
        return Result(data)

class PostgresRPC:
    def __init__(self, client: "PostgresClient", name: str, params: dict[str, Any]):
        self.client, self.name, self.params = client, name, params

    def execute(self):
        _ident(self.name)
        names = list(self.params)
        for name in names:
            _ident(name)
        args = ", ".join(f"{_ident(name)} => %s" for name in names)
        sql = f"SELECT public.{_ident(self.name)}({args}) AS result"
        values = [_adapt(self.params[name]) for name in names]
        with self.client.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                row = cur.fetchone()
        value = row["result"] if row else None
        return Result(value)

class PostgresClient:
    def __init__(self, dsn: str):
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set; configure PostgreSQL in Relax Dev.")
        self.dsn = dsn

    def connect(self):
        # Relax Dev's DATABASE_URL points to its internal PgBouncer endpoint; TLS is disabled.
        dsn = self.dsn
        if "sslmode=" in dsn:
            dsn = re.sub(r"([?&])sslmode=[^&]*&?", r"\1", dsn).rstrip("?&")
        return psycopg.connect(dsn, connect_timeout=4, sslmode="disable", row_factory=dict_row)

    def table(self, name: str):
        return PostgresQuery(self, name)

    def rpc(self, name: str, params: dict[str, Any]):
        return PostgresRPC(self, name, params)

def create_client_from_env() -> PostgresClient:
    return PostgresClient(os.getenv("DATABASE_URL", "").strip())
