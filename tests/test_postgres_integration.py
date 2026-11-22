"""Optional PostgreSQL integration checks.

Set TEST_DATABASE_URL to a disposable PostgreSQL database and run:
python -m unittest discover -s tests -p 'test_postgres_integration.py' -v
"""
import os
import unittest
import uuid

try:
    import psycopg
except ImportError:
    psycopg = None


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL") and psycopg, "requires disposable TEST_DATABASE_URL")
class PostgreSQLReliabilityIntegrationTests(unittest.TestCase):
    def test_update_id_primary_key_rejects_duplicate_delivery(self):
        dsn = os.environ["TEST_DATABASE_URL"]
        update_id = int(uuid.uuid4().int % 9_000_000_000_000_000_000)
        with psycopg.connect(dsn) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.bot_telegram_updates (
                    update_id bigint primary key,
                    status text not null check (status in ('processing','done','uncertain')),
                    last_error text,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
            """)
            conn.execute("INSERT INTO public.bot_telegram_updates(update_id,status) VALUES (%s,'processing')", (update_id,))
            with self.assertRaises(psycopg.errors.UniqueViolation):
                with conn.transaction():
                    conn.execute("INSERT INTO public.bot_telegram_updates(update_id,status) VALUES (%s,'processing')", (update_id,))
            conn.execute("DELETE FROM public.bot_telegram_updates WHERE update_id=%s", (update_id,))
