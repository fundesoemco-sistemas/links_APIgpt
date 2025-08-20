import os
import psycopg
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
from psycopg.rows import dict_row  # row_factory=dict_row

DATABASE_URL = os.getenv("DATABASE_URL")

def now_utc():
    return datetime.now(timezone.utc)

def _connect(**kw):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurada")
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        autocommit=True,
        connect_timeout=10,
        **kw
    )

class PgStorage:
    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL no configurada")

        # Crear extensión y tabla si no existen (conexión efímera)
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("create extension if not exists pgcrypto;")
            cur.execute(
                """
                create table if not exists links(
                  id uuid primary key default gen_random_uuid(),
                  url text not null,
                  title text,
                  tags text[],
                  notes text,
                  created_at timestamptz default now(),
                  updated_at timestamptz default now()
                );
                """
            )

    def list_links(
        self,
        limit: int,
        offset: int,
        tag: Optional[str],
        q: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], int]:
        where = []
        params: List[Any] = []

        if tag:
            where.append("%s = any(tags)")
            params.append(tag)
        if q:
            where.append(
                "(coalesce(title,'') ilike %s or url ilike %s or coalesce(notes,'') ilike %s)"
            )
            like = f"%{q}%"
            params.extend([like, like, like])

        wh = (" where " + " and ".join(where)) if where else ""

        with _connect() as conn, conn.cursor() as cur:
            cur.execute(f"select count(*) as c from links{wh}", params)
            total = cur.fetchone()["c"]

            cur.execute(
                f"""
                select id::text as id, url, title, tags, notes, created_at, updated_at
                from links{wh}
                order by updated_at desc
                limit %s offset %s
                """,
                [*params, limit, offset],
            )
            rows = cur.fetchall()

        return rows, total

    def create_link(self, item: Dict[str, Any]) -> Dict[str, Any]:
        now = now_utc()
        tags = item.get("tags") or []
        url = str(item["url"])  # asegurar string
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into links(url, title, tags, notes, created_at, updated_at)
                values (%s, %s, %s, %s, %s, %s)
                returning id::text as id, url, title, tags, notes, created_at, updated_at
                """,
                [url, item.get("title"), tags, item.get("notes"), now, now],
            )
            return cur.fetchone()

    def create_links_bulk(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        now = now_utc()
        out: List[Dict[str, Any]] = []
        # transacción explícita para bulk
        with _connect(autocommit=False) as conn, conn.cursor() as cur:
            try:
                for it in items:
                    url = str(it["url"])
                    cur.execute(
                        """
                        insert into links(url, title, tags, notes, created_at, updated_at)
                        values (%s, %s, %s, %s, %s, %s)
                        returning id::text as id, url, title, tags, notes, created_at, updated_at
                        """,
                        [url, it.get("title"), it.get("tags") or [], it.get("notes"), now, now],
                    )
                    out.append(cur.fetchone())
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return out

    def get_link(self, link_id: str) -> Optional[Dict[str, Any]]:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select id::text as id, url, title, tags, notes, created_at, updated_at from links where id = %s",
                [link_id],
            )
            return cur.fetchone()

    def update_link(self, link_id: str, patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        fields: List[str] = []
        params: List[Any] = []

        for k in ("url", "title", "tags", "notes"):
            if patch.get(k) is not None:
                v = str(patch[k]) if k == "url" else patch[k]
                fields.append(f"{k}=%s")
                params.append(v)

        if not fields:
            return self.get_link(link_id)

        fields_sql = ", ".join(fields + ["updated_at=%s"])
        params.append(now_utc())
        params.append(link_id)

        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                update links
                set {fields_sql}
                where id=%s
                returning id::text as id, url, title, tags, notes, created_at, updated_at
                """,
                params,
            )
            return cur.fetchone()

    def delete_link(self, link_id: str) -> bool:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("delete from links where id=%s", [link_id])
            return cur.rowcount > 0

    def export_all(self) -> List[Dict[str, Any]]:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select id::text as id, url, title, tags, notes, created_at, updated_at from links order by updated_at desc"
            )
            return cur.fetchall()
