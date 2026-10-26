"""Regression checks for critical Pear CM security and performance invariants.

These tests deliberately inspect source-level guardrails without requiring live
Telegram or Supabase credentials.
"""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "iris.py").read_text(encoding="utf-8")
SCHEMA = (ROOT / "schema.sql").read_text(encoding="utf-8")


class SecurityAndPerformanceRegressions(unittest.TestCase):
    def test_bot_owner_is_rank_five_consistently(self):
        ensure_start = SOURCE.index("def ensure_user(")
        ensure_end = SOURCE.index("def set_warns(", ensure_start)
        ensure_user = SOURCE[ensure_start:ensure_end]
        self.assertEqual(ensure_user.count("rank = 5 if user_id == OWNER_ID"), 2)
        self.assertNotIn("rank = 4 if user_id == OWNER_ID", ensure_user)
        self.assertIn('get(OWNER_ID, (5, "", None))', SOURCE)
        self.assertNotIn("if message.from_user.id == OWNER_ID and rank > 4:", SOURCE)

    def test_internal_rank_hierarchy_applies_to_telegram_admins_too(self):
        start = SOURCE.index("async def moderation_target(")
        end = SOURCE.index("async def ensure_moderation_target_safe(", start)
        body = SOURCE[start:end]
        self.assertIn("actor_is_creator", body)
        self.assertIn("target_rank >= actor_rank", body)
        self.assertNotIn("if not actor_is_admin:", body)

    def test_warns_apply_the_same_internal_rank_hierarchy(self):
        start = SOURCE.index("async def warn_cmd(")
        end = SOURCE.index("@router.message", start + 20)
        body = SOURCE[start:end]
        self.assertIn("target_rank >= actor_rank", body)
        self.assertIn("actor_is_creator", body)

    def test_rank_five_is_immune_to_moderation_and_demotion(self):
        safe_start = SOURCE.index("async def ensure_moderation_target_safe(")
        safe_end = SOURCE.index("async def check_mod(", safe_start)
        safe = SOURCE[safe_start:safe_end]
        self.assertIn("if target_rank >= 5:", safe)
        demote_start = SOURCE.index("async def demote_rank_cmd(")
        demote_end = SOURCE.index("@router.message", demote_start + 20)
        demote = SOURCE[demote_start:demote_end]
        self.assertIn("if current == 5:", demote)
        self.assertNotIn("if current == 5 and not", demote)

    def test_economy_mutations_are_transactional_and_bounded(self):
        self.assertIn("create or replace function public.pear_economy_action", SCHEMA.lower())
        self.assertIn("for update", SCHEMA.lower())
        self.assertIn("grant execute on function public.pear_economy_action", SCHEMA.lower())
        self.assertIn('p_action not in (\'bonus\', \'purchase\', \'transfer\')', SCHEMA)

    def test_latency_observability_is_present(self):
        self.assertIn("Slow Supabase operation", SOURCE)
        self.assertIn("elapsed >= 1.0", SOURCE)
        self.assertIn("Slow update handler (target <1s)", SOURCE)

    def test_sensitive_settings_and_user_queries_are_chat_scoped(self):
        self.assertIn('db_one("bot_users", user_id=user_id, chat_id=chat_id)', SOURCE)
        self.assertIn('"user_id": user_id, "chat_id": chat_id', SOURCE)
        self.assertIn('"user_id,chat_id")', SOURCE)
        self.assertIn('primary key (user_id, chat_id)', SCHEMA.lower())

    def test_duplicate_activity_messages_are_idempotent(self):
        self.assertIn("create or replace function public.record_bot_activity_batch", SCHEMA.lower())
        self.assertIn("on conflict (chat_id, message_id) do nothing", SCHEMA.lower())
        self.assertIn("bot_activity_events_chat_message_uidx", SCHEMA.lower())

    def test_process_has_bounded_command_rate_limit_and_single_instance_guard(self):
        self.assertIn("class CommandRateLimitMiddleware", SOURCE)
        self.assertIn("fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)", SOURCE)
        self.assertIn("TelegramConflictError", SOURCE)

    def test_user_supplied_numbers_are_bounded_for_bulk_delete(self):
        self.assertIn("requested < 1", SOURCE)
        self.assertIn("requested > 100", SOURCE)

    def test_critical_audit_events_are_persisted_off_event_loop(self):
        self.assertIn("async def audit_wait(", SOURCE)
        self.assertIn('loop.create_task(asyncio.to_thread(db_insert, "bot_audit"', SOURCE)

    def test_ordinary_messages_skip_generic_rank_database_lookup(self):
        start = SOURCE.index("async def normalize_dot_dk_prefix(")
        end = SOURCE.index("router.message.outer_middleware(normalize_dot_dk_prefix)", start)
        middleware = SOURCE[start:end]
        self.assertIn("_RANK_CHECKED_TEXT_COMMANDS", middleware)
        self.assertIn("_RANK_CHECKED_TEXT_COMMANDS = {", SOURCE)
        self.assertIn("explicit_command or command_token in _RANK_CHECKED_TEXT_COMMANDS", middleware)
        self.assertIn("await asyncio.to_thread(get_chat_settings, event.chat.id)", middleware)
        self.assertIn("Keep ordinary chat traffic off the thread pool and database path", middleware)

    def test_main_loop_restarts_after_transient_crashes(self):
        self.assertIn("async def run_forever() -> None:", SOURCE)
        self.assertIn("await main()", SOURCE)
        self.assertIn('log.exception("Bot main loop crashed; restarting in %.0fs", delay)', SOURCE)
        self.assertIn("bot = Bot(BOT_TOKEN, default=DefaultBotProperties(", SOURCE)
        self.assertNotIn("async def main() -> None:\n    acquire_single_instance_lock()", SOURCE)
        self.assertIn("acquire_single_instance_lock()\n    failures = 0", SOURCE)

    def test_runtime_build_runs_regression_tests_and_cleans_api_session(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        self.assertIn("python -m unittest discover -s tests -v", dockerfile)
        self.assertIn("COPY tests ./tests", dockerfile)
        self.assertNotIn("tests", dockerignore)
        self.assertIn("await _API_SESSION.close()", SOURCE)
        self.assertIn("stop_for_polling_conflict = True", SOURCE)
        self.assertIn("Exiting with code 23", SOURCE)


if __name__ == "__main__":
    unittest.main()
