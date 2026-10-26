import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

import pear_core


class DurationParserTests(unittest.TestCase):
    def check(self, tokens, expected, consumed):
        result = pear_core.parse_duration_prefix(tokens)
        self.assertIsNotNone(result, tokens)
        self.assertEqual(result[0], expected)
        self.assertEqual(result[1], consumed)

    def test_compact_and_inflected_russian_units(self):
        cases = [
            (["30м"], timedelta(minutes=30), 1),
            (["30", "минут"], timedelta(minutes=30), 2),
            (["1ч"], timedelta(hours=1), 1),
            (["1ч30м"], timedelta(hours=1, minutes=30), 1),
            (["2д3ч"], timedelta(days=2, hours=3), 1),
            (["1", "часа"], timedelta(hours=1), 2),
            (["1", "час", "30", "минут", "флуд"], timedelta(hours=1, minutes=30), 4),
            (["два", "часа"], timedelta(hours=2), 2),
            (["на", "одну", "неделю", "спам"], timedelta(days=7), 3),
            (["полчаса"], timedelta(minutes=30), 1),
            (["полдня"], timedelta(hours=12), 1),
        ]
        for tokens, duration, used in cases:
            with self.subTest(tokens=tokens):
                self.check(tokens, duration, used)

    def test_invalid_or_excessive_durations_rejected(self):
        self.assertIsNone(pear_core.parse_duration_prefix(["0", "минут"]))
        self.assertIsNone(pear_core.parse_duration_prefix(["2", "года"]))
        self.assertIsNone(pear_core.parse_duration_prefix(["просто", "причина"]))

    def test_reason_is_not_consumed_as_duration(self):
        result = pear_core.parse_duration_prefix(["1ч", "за", "флуд"])
        self.assertEqual(result[0], timedelta(hours=1))
        self.assertEqual(result[1], 1)


class DurableActivitySpoolTests(unittest.TestCase):
    def test_activity_survives_until_acknowledged(self):
        with tempfile.TemporaryDirectory() as folder:
            original = pear_core._SPOOL_PATH
            pear_core._SPOOL_PATH = Path(folder) / "activity.sqlite3"
            try:
                item = (-100123, 555, "some_user", "Test User", "2026-10-20T12:00:00+00:00", 789)
                self.assertTrue(pear_core.spool_activity(item))
                self.assertEqual(pear_core.activity_spool_size(), 1)
                self.assertEqual(pear_core.pending_activity(10), [item])
                pear_core.acknowledge_activity([item])
                self.assertEqual(pear_core.activity_spool_size(), 0)
            finally:
                pear_core._SPOOL_PATH = original


class VisualAndSchemaTests(unittest.TestCase):
    def test_panel_has_consistent_structure(self):
        rendered = pear_core.format_panel("ЗАГОЛОВОК", "Тело", "Футер")
        self.assertIn("🛡️ <b>ЗАГОЛОВОК</b>", rendered)
        self.assertIn("Тело", rendered)
        self.assertIn("<i>Футер</i>", rendered) if "<i>Футер</i>" in rendered else self.assertIn("<blockquote>— Футер</blockquote>", rendered)

    def test_schema_check_reports_missing_table_and_columns(self):
        class FakeQuery:
            def __init__(self, table, columns):
                self.table, self.columns = table, columns
            def limit(self, _count):
                return self
            def execute(self):
                if self.table == "bot_users" and "last_seen" not in self.columns:
                    raise RuntimeError("column missing")
                if self.table == "bot_activity_daily":
                    raise RuntimeError("table missing")
                return True
        class FakeClient:
            def table(self, table):
                class Select:
                    def __init__(self, name):
                        self.name = name
                    def select(self, columns):
                        return FakeQuery(self.name, columns)
                return Select(table)
        problems = pear_core.check_schema(FakeClient())
        self.assertTrue(any("bot_activity_daily" in problem for problem in problems))


if __name__ == "__main__":
    unittest.main()
