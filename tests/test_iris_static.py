import ast
import pathlib
import re
import unittest
from datetime import timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "iris.py"
SCHEMA = ROOT / "schema.sql"
POSTGRES_ADAPTER = ROOT / "postgres_adapter.py"
REQUIREMENTS = ROOT / "requirements.txt"


class PearCMStaticRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = SOURCE.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source, filename=str(SOURCE))
        cls.function_names = {
            node.name for node in ast.walk(cls.tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

    def test_existing_core_handlers_are_preserved(self):
        required = {
            "help_command", "admins", "mute_cmd", "unmute_cmd", "ban_cmd",
            "unban_cmd", "kick_cmd", "warn_cmd", "unwarn_cmd", "clearwarns_cmd",
            "purge_cmd", "top_cmd", "logs_cmd", "rank_cmd", "promote_rank_cmd",
            "demote_rank_cmd", "open_chat_cmd", "close_chat_cmd",
            "moderation_history_cmd", "owner_diagnostics",
        }
        self.assertTrue(required.issubset(self.function_names), required - self.function_names)

    def test_advanced_handlers_exist(self):
        required = {
            "temporary_ban_cmd", "admin_notes_cmd", "trusted_users_cmd",
            "punishment_reasons_cmd", "granular_permissions_cmd",
            "export_chat_settings_cmd", "import_chat_settings_cmd",
            "configure_welcome_cmd", "chat_schedule_cmd", "moderator_stats_cmd",
            "_schedule_worker", "_is_trusted_user", "_validate_imported_settings",
        }
        self.assertTrue(required.issubset(self.function_names), required - self.function_names)

    def test_anti_spam_penalties_are_opt_in_and_fail_closed(self):
        self.assertIn('"filter_action": "monitor"', self.source)
        self.assertIn('aliases = {', self.source[self.source.index("async def filter_action_cmd"):self.source.index("async def apply_configured_filter_action")])
        helper_start = self.source.index("async def apply_configured_filter_action")
        helper_end = self.source.index("async def auto_moderation_filters", helper_start)
        helper = self.source[helper_start:helper_end]
        self.assertIn('action not in {"delete", "mute"}', helper)
        self.assertIn("if member is None:", helper)
        self.assertIn("ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR", helper)
        self.assertIn("filteraction", self.source)

    def test_monitor_only_filter_does_not_apply_automatic_penalties(self):
        function = next(
            node for node in ast.walk(self.tree)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "auto_moderation_filters"
        )
        calls = {
            node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
            for node in ast.walk(function) if isinstance(node, ast.Call)
        }
        self.assertNotIn("delete_message", calls)
        self.assertNotIn("ban_chat_member", calls)
        self.assertNotIn("restrict_chat_member", calls)

    def test_dot_dk_nickname_rank_rules_are_routed_as_permissions(self):
        start = self.source.index("async def normalize_dot_dk_prefix")
        end = self.source.index("# Register participant-call aliases", start)
        middleware = self.source[start:end]
        self.assertIn("nick_rule = (", middleware)
        self.assertIn('in {"ник", "ники", "никнейм", "никнеймы"}', middleware)
        self.assertIn("await command_rank_rule_cmd(event)", middleware)
        self.assertIn('event = event.model_copy(update={"text": _normalize_natural_moderation_phrase(event.text)})', middleware)
        self.assertIn('event = event.model_copy(update={"text": body})', middleware)
        self.assertNotIn("event.text =", middleware)
        self.assertIn("router.message.outer_middleware(normalize_dot_dk_prefix)", self.source)
        rank_start = self.source.index("async def command_rank_rule_cmd")
        rank_end = self.source.index("@router.message(F.text.regexp", rank_start)
        rank_body = self.source[rank_start:rank_end]
        self.assertIn('"ник":"nick"', rank_body)
        self.assertIn('settings[f"modrank_{key}"] = required', rank_body)

    def test_warning_reasons_hide_legacy_metadata_and_target_username(self):
        start = self.source.index("def _clean_warning_reason")
        end = self.source.index("def active_warn_entries", start)
        helper = self.source[start:end]
        self.assertIn("expires_in_days", helper)
        self.assertIn(r"^@[A-Za-z0-9_]{5,32}\s+", helper)
        self.assertIn('"reason": _clean_warning_reason(details)', self.source)
        warns_start = self.source.index("async def chat_warns")
        warns_end = self.source.index('@router.message(F.text == "🛡 Кто админ")', warns_start)
        warns_body = self.source[warns_start:warns_end]
        self.assertIn("actor_name = custom_names.get(actor_id) or names.get(actor_id)", warns_body)
        self.assertIn("profile_link(actor_id, actor_name, usernames.get(actor_id))", warns_body)

    def test_chat_nickname_lookup_supports_reply_username_and_id(self):
        self.assertIn("async def show_chat_nickname_cmd", self.source)
        self.assertIn("async def show_chat_nickname_dispatch", self.source)
        self.assertIn("async def _resolve_nickname_lookup_target", self.source)
        self.assertIn('.eq("username_lower", username.casefold())', self.source)
        self.assertIn("_CHAT_NICKNAME_CACHE_TTL = 15.0", self.source)
        self.assertIn("_invalidate_chat_nickname_cache(message.chat.id, target_id)", self.source)

    def test_current_member_validation_filters_lists_and_keeps_profile_links_non_pinging(self):
        self.assertIn("async def is_current_chat_member", self.source)
        self.assertIn("async def filter_current_chat_members", self.source)
        self.assertIn("ChatMemberStatus.LEFT, ChatMemberStatus.KICKED", self.source)
        warns_start = self.source.index("async def chat_warns")
        warns_end = self.source.index('@router.message(F.text == "🛡 Кто админ")', warns_start)
        warns_body = self.source[warns_start:warns_end]
        self.assertIn("filter_current_chat_members", warns_body)
        self.assertIn("profile_link(uid, display_name, usernames.get(uid))", warns_body)
        self.assertNotIn("call_mention(", warns_body)
        nick_start = self.source.index("async def list_chat_nicknames_cmd")
        nick_end = self.source.index('@router.message(Command("logs"))', nick_start)
        nick_body = self.source[nick_start:nick_end]
        self.assertIn("rows = await filter_current_chat_members(message.chat.id, rows)", nick_body)
        self.assertNotIn('href="tg://user?id=', nick_body)
        self.assertIn("Without a username there is no reliable non-pinging public profile URL.", nick_body)
        top_start = self.source.index("async def top_cmd")
        top_end = self.source.index("async def show_chat_nickname_cmd", top_start)
        top_body = self.source[top_start:top_end]
        self.assertIn("rows = await filter_current_chat_members(message.chat.id, rows)", top_body)
        self.assertIn("display = profile_link(uid, display_name, username)", top_body)

    def test_additive_schema_for_new_features_exists(self):
        schema = SCHEMA.read_text(encoding="utf-8")
        for fragment in (
            "create table if not exists public.bot_trusted_users",
            "alter table public.bot_notes add column if not exists chat_id bigint",
            "alter table public.bot_notes add column if not exists actor_id bigint",
            "bot_trusted_users_chat_active_idx",
            "bot_notes_chat_user_created_idx",
        ):
            self.assertIn(fragment, schema.lower())

    def test_natural_temporary_ban_duration_phrases(self):
        from pear_core import parse_duration_prefix

        cases = [
            (["1", "день"], timedelta(days=1), 2),
            (["день"], timedelta(days=1), 1),
            (["один", "день", "спам"], timedelta(days=1), 2),
            (["на", "одну", "неделю", "спам"], timedelta(days=7), 3),
            (["1", "месяц"], timedelta(days=30), 2),
            (["год"], timedelta(days=365), 1),
            (["2", "дня", "и", "3", "часа", "флуд"], timedelta(days=2, hours=3), 5),
            (["1", "час", "30", "минут", "причина"], timedelta(hours=1, minutes=30), 4),
            (["30м"], timedelta(minutes=30), 1),
            (["1ч"], timedelta(hours=1), 1),
            (["1", "секунда"], timedelta(seconds=1), 2),
            (["1сек"], timedelta(seconds=1), 1),
            (["одну", "секунду"], timedelta(seconds=1), 2),
            (["два", "часа"], timedelta(hours=2), 2),
            (["полчаса"], timedelta(minutes=30), 1),
        ]
        for tokens, expected_duration, expected_consumed in cases:
            with self.subTest(tokens=tokens):
                result = parse_duration_prefix(tokens)
                self.assertIsNotNone(result)
                self.assertEqual(result[0], expected_duration)
                self.assertEqual(result[1], expected_consumed)
        self.assertIsNone(parse_duration_prefix(["2", "года"]))


    def test_moderation_and_runtime_safety_guards_are_present(self):
        for fragment in (
            "Activity queue is full",
            "Three active warnings",
            "Telegram cannot restrict a chat creator or administrator",
        ):
            self.assertIn(fragment.casefold(), self.source.casefold())

    def test_runtime_reliability_guards_exist(self):
        for fragment in (
            "def _is_transient_db_error",
            "def _db_read_with_retry",
            "acquire_single_instance_lock()",
            "_ACTIVITY_OVERFLOW_MAX = 20000",
            "async def _drain_activity_queue",
            "_ACTIVITY_WRITER_BUSY = False",
            "Temporary PostgreSQL",
        ):
            self.assertIn(fragment, self.source)

    def test_schema_has_query_indexes_for_chat_scoped_lists(self):
        schema = SCHEMA.read_text(encoding="utf-8").lower()
        self.assertIn("bot_activity_daily_chat_day_idx", schema)
        self.assertIn("bot_audit_chat_target_created_idx", schema)
        self.assertIn("bot_marriages_chat_idx", schema)
        self.assertIn("bot_users_chat_last_seen_idx", schema)
        self.assertIn("bot_marriages_chat_status_created_idx", schema)

    def test_activity_counters_are_idempotent_and_retryable(self):
        self.assertIn('"message_id": int(message_id)', self.source)
        self.assertIn('db.rpc("record_bot_activity_batch"', self.source)
        self.assertIn("Activity batch persistence failed (attempt %s/2; size=%s)", self.source)
        schema = SCHEMA.read_text(encoding="utf-8").lower()
        self.assertIn("alter table public.bot_activity_events add column if not exists message_id bigint", schema)
        self.assertIn("bot_activity_events_chat_message_uidx", schema)
        self.assertIn("create or replace function public.record_bot_activity_batch", schema)
        self.assertIn("on conflict (chat_id, message_id) do nothing", schema)

    def test_polling_conflicts_stop_the_restart_loop(self):
        self.assertIn("except TelegramConflictError", self.source)
        self.assertIn("raise SystemExit(23)", self.source)
        self.assertIn("stop_for_polling_conflict = True", self.source)
        self.assertIn("Exiting with code 23", self.source)
        self.assertNotIn("Polling conflict (Telegram 409); retrying", self.source)
        entrypoint = (ROOT / "entrypoint.sh").read_text(encoding="utf-8")
        self.assertIn('if [ "$code" -eq 23 ]', entrypoint)
        self.assertIn("Exiting to avoid a restart loop", entrypoint)
        self.assertNotIn("Retrying in 60s", entrypoint)

    def test_moderation_protects_chat_creator_and_telegram_admins(self):
        self.assertIn("async def ensure_moderation_target_safe", self.source)
        self.assertIn("Нельзя применять это действие к создателю чата", self.source)
        self.assertIn("Telegram не позволяет боту применять это действие к администратору чата", self.source)
        self.assertIn('ensure_moderation_target_safe(message, target, "mute")', self.source)
        self.assertIn('ensure_moderation_target_safe(message, target, "ban")', self.source)
        self.assertIn('ensure_moderation_target_safe(message, target, "kick")', self.source)

    def test_global_ban_is_enforced_before_message_handlers(self):
        self.assertIn("await asyncio.to_thread(_is_globally_banned, event.from_user.id)", self.source)
        self.assertIn("Доступ к функциям GDE.WTF для этого аккаунта ограничен", self.source)
        self.assertIn("class GlobalBanCallbackMiddleware", self.source)
        self.assertIn("await asyncio.to_thread(_is_globally_banned, user.id)", self.source)
        self.assertIn("router.callback_query.outer_middleware(GlobalBanCallbackMiddleware())", self.source)

    def test_rank_changes_follow_strict_hierarchy(self):
        for handler_name in ("promote_rank_cmd", "demote_rank_cmd"):
            start = self.source.index(f"async def {handler_name}")
            end = self.source.find("\n@router.message", start + 1)
            body = self.source[start:end if end >= 0 else len(self.source)]
            self.assertIn("target_member.status == ChatMemberStatus.CREATOR", body)
            self.assertIn("actor_is_telegram_admin", body)
            self.assertIn("current >= actor_rank or new_rank >= actor_rank", body)
            self.assertNotIn("new_rank >= max(3, actor_rank)", body)

    def test_admin_listing_includes_chat_creator_and_uses_plain_names(self):
        start = self.source.index("async def admins")
        end = self.source.index("# ── Activity tracking and welcome", start)
        body = self.source[start:end]
        self.assertIn('db_rows, "bot_users"', body)
        self.assertIn("bot.get_chat_administrators(message.chat.id)", body)
        self.assertIn("ChatMemberStatus.CREATOR", body)
        self.assertIn("Создатели чата", body)
        self.assertIn("Show GDE.WTF roles plus the real Telegram chat creator", body)
        self.assertNotIn("Администраторы Telegram", body)
        self.assertNotIn('<a href=', body)
        self.assertNotIn("https://t.me/", body)
        self.assertNotIn("tg://user?id=", body)

    def test_chat_creator_is_protected_from_warning_removal_and_reset(self):
        helper_start = self.source.index("async def ensure_moderation_target_safe")
        helper_end = self.source.index("async def check_mod", helper_start)
        helper = self.source[helper_start:helper_end]
        self.assertIn("member.status == ChatMemberStatus.CREATOR", helper)
        for function, action in (
            ("unwarn_cmd", "unwarn"),
            ("clearwarns_cmd", "clearwarns"),
        ):
            start = self.source.index(f"async def {function}")
            end = self.source.find("\n@router.message", start + 1)
            body = self.source[start:end if end >= 0 else len(self.source)]
            self.assertIn(f'ensure_moderation_target_safe(message, target, "{action}")', body)

    def test_rank_change_aliases_include_common_russian_forms(self):
        for fragment in (
            r"поднять(?:\s+ранг)?",
            r"назначить\s+(?:ранг|чин)",
            r"понизить\s+в\s+должности",
            r"снять\s+(?:ранг|чин",
            "разжаловать",
        ):
            self.assertIn(fragment, self.source)

    def test_moderation_aliases_cover_common_inflections(self):
        for fragment in (
            "заглуш(?:ить|и|ь|ьте|ите)?",
            "заблок(?:ировать|ируй|ируйте)?",
            "выдвор(?:ить|и|ите)",
            "предупреждай",
            "сними|снимите|снимайте|убрать",
            "бан(?:а|у|ом|е|ы|ов|ам|ами|ах)?",
        ):
            self.assertIn(fragment, self.source)

    def test_warning_removal_requires_audit_ledger_write(self):
        unwarn_start = self.source.index("async def unwarn_cmd")
        unwarn_end = self.source.index("@router.message(Command(\"clearwarns\")", unwarn_start)
        unwarn_body = self.source[unwarn_start:unwarn_end]
        self.assertLess(unwarn_body.index('ledger_saved = await audit_wait'), unwarn_body.index('db_upsert, "bot_users"'))
        clear_start = self.source.index("async def clearwarns_cmd")
        clear_end = self.source.index("# ── Message tools", clear_start)
        clear_body = self.source[clear_start:clear_end]
        self.assertLess(clear_body.index('ledger_saved = await audit_wait'), clear_body.index('db_upsert, "bot_users"'))

    def test_warning_updates_check_database_write_results(self):
        self.assertIn("Не удалось записать снятие предупреждения", self.source)
        self.assertIn("Не удалось записать сброс предупреждений", self.source)

    def test_warning_issue_recounts_from_active_audit_ledger(self):
        start = self.source.index("async def warn_cmd")
        end = self.source.index("# Three active warnings trigger a ban", start)
        body = self.source[start:end]
        self.assertIn("active_warn_entries, message.chat.id, target[0]", body)
        self.assertIn("Warning count mismatch; using audit ledger", body)
        self.assertIn("точный счётчик", body)
        self.assertIn("if count_verified and before is not None and count is not None", self.source)

    def test_explicit_moderation_target_is_removed_from_reason(self):
        self.assertIn("def is_target_token(token: str)", self.source)
        self.assertIn('re.fullmatch(r"\\d{7,}", token)', self.source)
        self.assertIn("reason_tokens, _removed_target = remove_explicit_target(target_tokens)", self.source)
        self.assertIn("remainder, _removed_target = remove_explicit_target(raw_args)", self.source)
        self.assertIn('duration_tokens, _removed_target = remove_explicit_target(raw_args)', self.source)
        self.assertIn('reason_tokens, explicit_target = remove_explicit_target(raw_args)', self.source)

    def test_chat_lock_persists_restore_state_before_telegram_change(self):
        close_start = self.source.index("async def close_chat_cmd")
        close_end = self.source.index("@router.message(Command(\"schedule\"", close_start)
        close_body = self.source[close_start:close_end]
        self.assertLess(close_body.index("pre_saved = await asyncio.to_thread(db_upsert"), close_body.index("await bot.set_chat_permissions"))
        self.assertIn("Не закрываю чат: база данных не настроена", close_body)
        self.assertIn("не удалось сохранить исходные разрешения", close_body)

    def test_unmute_restores_chat_default_permissions(self):
        start = self.source.index("async def unmute_cmd")
        end = self.source.index("@router.message(Command(\"ban\")", start)
        body = self.source[start:end]
        self.assertIn("default_permissions = getattr(chat_info, \"permissions\", None)", body)
        self.assertIn("ChatPermissions(**default_permissions.model_dump(exclude_none=True))", body)

    def test_sub_30_second_mutes_use_exact_timer_and_telegram_fallback(self):
        start = self.source.index("async def mute_cmd")
        end = self.source.index('@router.message(Command("unmute")', start)
        body = self.source[start:end]
        self.assertIn("requested_seconds < 30", body)
        self.assertIn("timedelta(seconds=35)", body)
        self.assertIn("_schedule_short_mute_expiry(message.chat.id, target[0], requested_seconds)", body)
        self.assertIn("async def _expire_short_mute", self.source)
        self.assertIn("_cancel_short_mute_expiry(message.chat.id, target[0])", self.source)

    def test_chat_lock_restores_saved_telegram_permissions(self):
        self.assertIn("current_permissions = getattr(chat_info, \"permissions\", None)", self.source)
        self.assertIn("current_permissions.model_dump(exclude_none=True)", self.source)
        self.assertIn('settings["chat_permissions_before_lock"]', self.source)

    def test_delete_command_is_bounded(self):
        self.assertIn("requested < 1", self.source)
        self.assertIn("requested > 100", self.source)
        self.assertIn('if match.group(1) is None:', self.source)
        self.assertIn('requested = int(match.group(1))', self.source)
        self.assertIn("count = requested", self.source)
        self.assertIn("deleted_messages = sum(1 for result in results if result)", self.source)
        self.assertIn("Команда <code>-смс</code> удаляется отдельно", self.source)
        self.assertIn("Delete individually", self.source)
        self.assertIn("Удалено сообщений:", self.source)
        self.assertIn("За один раз можно удалить максимум", self.source)


    def test_nickname_commands_accept_target_username_or_telegram_id(self):
        start = self.source.index("async def set_chat_nickname_cmd")
        end = self.source.index('@router.message(Command("nicks"))', start)
        body = self.source[start:end]
        self.assertIn("is_target_token", body)
        self.assertIn("explicit_target_in_args", body)
        self.assertIn("and not explicit_target_in_args", body)
        self.assertIn("An explicit target in the command must override a replied-to message", body)
        self.assertIn("resolved = await _resolve_nickname_lookup_target(message, normalized_first)", body)
        self.assertIn("normalized_first = first.strip().strip(\"()\")", body)
        self.assertIn("live_username != requested_username", body)
        self.assertIn("nickname = parts[1].strip() if len(parts) > 1 else", body)
        self.assertIn("target = await resolve_target(message, rest.split()[:1])", body)
        self.assertIn("first[1:] if first.startswith(\"@\")", body)
        self.assertIn("Refusing nickname assignment after username mismatch", body)
        self.assertIn("Ответь на сообщение нужного человека или укажи его Telegram ID", body)
        self.assertIn("live_member = await bot.get_chat_member(message.chat.id, target_id)", body)
        self.assertIn("live_username != requested_username", body)
        self.assertIn('resolved_username != requested_username', body)
        self.assertIn("Membership is checked once below", body)
        member_start = self.source.index("async def get_chat_member_cached")
        member_end = self.source.index("async def is_current_chat_member", member_start)
        member_body = self.source[member_start:member_end]
        self.assertIn("Telegram getChatMember failed fast; refusing unverified member check", member_body)
        self.assertIn("_CHAT_MEMBER_CACHE[key] = (None, now)", member_body)

    def test_removed_marriage_feature_is_not_advertised_as_working(self):
        # Marriage/relationship roleplay was intentionally removed. The retained
        # SQL table is a compatibility migration, not an exposed bot feature.
        self.assertNotIn('F.text == "💍 Брак"', self.source)
        self.assertNotIn('Command("marry")', self.source)
        self.assertNotIn('Command("marriage")', self.source)
        self.assertNotIn('Command("divorce")', self.source)
        self.assertNotIn('F.data.startswith("marriage:")', self.source)

    def test_database_enforces_single_active_marriage_and_valid_sql_function_quotes(self):
        schema = SCHEMA.read_text(encoding="utf-8").lower()
        self.assertIn("create or replace function public.prevent_overlapping_bot_marriages()", schema)
        self.assertIn("pg_advisory_xact_lock(new.chat_id)", schema)
        self.assertIn("create trigger bot_marriages_no_overlap", schema)
        self.assertNotRegex(schema, r"(?m)^as \\$")
        self.assertNotRegex(schema, r"(?m)^\\$;$")

    def test_removed_marriage_aliases_do_not_intercept_chat_commands(self):
        # Generic text handlers must not advertise or dispatch removed marriage aliases.
        self.assertNotIn('(?:брак|брачок|брачиться', self.source)
        self.assertNotIn('(?:браки|все\\s+браки|список\\s+браков)', self.source)
        self.assertNotIn('брак да', self.source)
        self.assertNotIn('брак нет', self.source)
        self.assertIn("CommandRateLimitMiddleware", self.source)
        self.assertIn("command_match = re.match", self.source)

    def test_postgresql_url_is_required_and_supabase_is_removed(self):
        config = self.source[self.source.index("DATABASE_URL ="):self.source.index("LOG_LEVEL =")]
        self.assertIn('os.getenv("DATABASE_URL", "").strip()', config)
        self.assertNotIn("SUPABASE_URL", self.source)
        self.assertNotIn("SUPABASE_SERVICE_KEY", self.source)
        self.assertIn("postgres_adapter", self.source)
        self.assertIn("Запуск без постоянной БД запрещён", self.source)

    def test_slow_database_operations_and_one_second_handlers_are_observable(self):
        self.assertIn("Slow PostgreSQL operation", self.source)
        self.assertIn("elapsed >= 1.0", self.source)
        self.assertIn("Slow update handler (target <1s)", self.source)

    def test_economy_mutations_use_atomic_rpc(self):
        start = self.source.index("async def _economy_rpc")
        end = self.source.index("# Clans", start)
        body = self.source[start:end]
        self.assertIn("async def economy_cmd", body)
        self.assertIn('"bonus"', body)
        self.assertIn('"purchase"', body)
        self.assertIn('"transfer"', body)
        self.assertNotIn('db_upsert, "bot_economy"', body)
        schema = SCHEMA.read_text(encoding="utf-8")
        self.assertIn("create or replace function public.pear_economy_action", schema)
        self.assertIn("for update", schema.lower())
        self.assertIn("grant execute on function public.pear_economy_action", schema.lower())

    def test_top_defaults_to_current_chat_in_groups(self):
        self.assertIn("chat_only = is_group(message) and not global_scope", self.source)
        self.assertIn('global_scope = arg.startswith(("все чаты"', self.source)


    def test_gdewtf_panel_and_reply_targets_are_supported(self):
        start = self.source.index("def _help() -> str:")
        end = self.source.index("if not BOT_TOKEN:", start)
        body = self.source[start:end]
        self.assertIn("def _gdewtf_panel_keyboard()", body)
        self.assertIn('callback_data="gdepanel:mods"', body)
        self.assertIn("async def gdewtf_panel_callback", body)
        self.assertIn('{"панель", "panel", "меню"}', body)
        self.assertIn("message.reply_to_message.from_user", body)
        self.assertIn('"журнал", "лог", "log"', body)
        self.assertIn('"снятьварны", "снять-варны", "warnclear"', body)
        self.assertIn('body={"q": q, "reason": api_reason, "by":', body)
        self.assertIn('api_reason = reason or "Выдал варн"', body)
        self.assertIn('verified_after_timeout', body)
        self.assertIn('await _gdewtf_safe_answer(message,', body)

    def test_aiogram_session_does_not_pass_unsupported_connector_options(self):
        # AiohttpSession forwards kwargs to aiogram BaseSession; `family` is an
        # aiohttp TCPConnector option, not a supported AiohttpSession argument.
        self.assertNotIn("family=socket.AF_INET6", self.source)
        self.assertEqual(self.source.count("AiohttpSession(timeout=8, limit=500)"), 2)

    def test_ordinary_telegram_requests_have_a_bounded_three_second_timeout(self):
        # GetUpdates uses the longer session timeout; ordinary calls such as
        # sendMessage must not hold a handler for 6–7 seconds on network stalls.
        self.assertIn("_TELEGRAM_API_TIMEOUT = 3", self.source)
        start = self.source.index("class LowLatencyBot")
        end = self.source.index("bot = LowLatencyBot(", start)
        self.assertIn("not isinstance(method, GetUpdates)", self.source[start:end])
        self.assertIn("request_timeout = _TELEGRAM_API_TIMEOUT", self.source[start:end])

    def test_owner_ping_uses_one_bounded_telegram_probe_and_parallel_db_probe(self):
        start = self.source.index("async def owner_ping(")
        end = self.source.index('@router.message(Command("diag"', start)
        ping = self.source[start:end]
        self.assertIn('await bot.get_me(request_timeout=0.8)', ping)
        self.assertNotIn('for sample_index in range(4)', ping)
        self.assertIn('asyncio.gather(_telegram_probe(), _PostgreSQL_probe())', ping)
        self.assertIn('_PING_CACHE_TTL = 15.0', self.source)
        self.assertIn('Failed probes are refreshed quickly', ping)

    def test_gdewtf_api_timeouts_are_bounded_and_warning_reads_are_reused(self):
        self.assertIn("request_timeout: aiohttp.ClientTimeout | None = None", self.source)
        self.assertIn("timeout=request_timeout or _TIMEOUT", self.source)
        self.assertIn('success = data.get("success")', self.source)
        self.assertIn("if isinstance(nested, dict):", self.source)
        self.assertIn("elif isinstance(nested, list):", self.source)
        self.assertIn('normalized_path == "/ping"', self.source)
        self.assertIn('data["ok"] = False', self.source)
        self.assertIn("не возвращает общее число всех участников", self.source)
        self.assertNotIn('for endpoint in ("/stats", "/people", "/users")', self.source)
        self.assertIn("before_mod = resolved_target_profile if isinstance(resolved_target_profile, dict) else {}", self.source)
        self.assertIn("request_timeout=aiohttp.ClientTimeout(total=3, connect=1.5, sock_read=2)", self.source)

    def test_gdewtf_panel_edits_existing_message_and_uses_api_membership(self):
        start = self.source.index("async def _gdewtf_authorize(")
        end = self.source.index("\n\n@gdewtf_router.message", start)
        auth = self.source[start:end]
        self.assertIn('*(_api("GET", "/mod", params={"q": query}) for query in queries)', auth)
        self.assertIn("transport_failure is not None and mod is None", auth)
        self.assertIn("request_timeout=aiohttp.ClientTimeout(total=5, connect=2, sock_read=4)", auth)
        self.assertNotIn("_GDEWTF_KNOWN_PERMS", auth)
        self.assertNotIn('role not in {"admin", "chief", "rgov", "amb", "mod"}', auth)
        callback_start = self.source.index('@gdewtf_router.callback_query(F.data.startswith("gdepanel:"))')
        callback_end = self.source.index("\n\nif not BOT_TOKEN", callback_start)
        callback = self.source[callback_start:callback_end]
        self.assertIn("callback.message.edit_text(", callback)
        self.assertNotIn("callback.message.answer(", callback)
        self.assertIn("message is not modified", callback)
        self.assertIn('await callback.answer("⛔ Нет прав в проекте GDE.WTF", show_alert=True)', callback)


    def test_gdewtf_authorization_rejects_recycled_username_identity(self):
        start = self.source.index("def _gdewtf_profile_matches_telegram(")
        end = self.source.index("\n\ndef _gdewtf_is_owner_or_chief(", start)
        matcher = self.source[start:end]
        self.assertIn("if explicit_ids:", matcher)
        self.assertIn("return str(user_id) in explicit_ids", matcher)
        auth_start = self.source.index("async def _gdewtf_authorize(")
        auth_end = self.source.index("\n\nasync def _gdewtf_safe_answer", auth_start)
        auth = self.source[auth_start:auth_end]
        self.assertIn("telegram_identity_mismatch", auth)
        self.assertIn("if explicit_ids and str(user_id) not in explicit_ids:", auth)

    def test_gdewtf_bot_is_restricted_to_live_api_project_owner_or_chief(self):
        helper_start = self.source.index("def _gdewtf_is_owner_or_chief(")
        helper_end = self.source.index("\n\nasync def _gdewtf_authorize(", helper_start)
        helper = self.source[helper_start:helper_end]
        for flag in ("is_owner", "is_project_owner", "is_chief", "is_chief_moderator"):
            self.assertIn(flag, helper)
        self.assertIn('"главный модератор"', helper)
        self.assertIn('"владелец проекта"', helper)
        self.assertNotIn('"is_admin",', helper)
        auth_start = self.source.index("async def _gdewtf_authorize(")
        auth_end = self.source.index("\n\n@gdewtf_router.message", auth_start)
        auth = self.source[auth_start:auth_end]
        self.assertIn('*(_api("GET", "/mod", params={"q": query}) for query in queries)', auth)
        self.assertIn("_gdewtf_is_owner_or_chief(mod)", auth)
        self.assertNotIn("_GDEWTF_ACCESS_CACHE", auth)
        callback_start = self.source.index('@gdewtf_router.callback_query(F.data.startswith("gdepanel:"))')
        callback_end = self.source.index("\n\nif not BOT_TOKEN", callback_start)
        callback = self.source[callback_start:callback_end]
        self.assertIn("await _gdewtf_authorize(callback.from_user.id, auth_action, callback.from_user.username)", callback)
        self.assertIn('reply_markup=_gdewtf_panel_keyboard()', callback)

    def test_postgres_adapter_reuses_bounded_connections(self):
        adapter = POSTGRES_ADAPTER.read_text(encoding="utf-8")
        requirements = REQUIREMENTS.read_text(encoding="utf-8")
        self.assertIn("psycopg-pool>=3.2,<4.0", requirements)
        self.assertIn("from psycopg_pool import ConnectionPool", adapter)
        self.assertIn("min_size=3", adapter)
        self.assertIn("max_size=max(8, min(24, configured_max))", adapter)
        self.assertIn("return self.pool.connection()", adapter)
        self.assertNotIn("return psycopg.connect(dsn, connect_timeout=4", adapter)

    def test_moderation_member_checks_are_bounded_and_cached(self):
        self.assertIn("_CHAT_MEMBER_REQUEST_TIMEOUT = 0.9", self.source)
        self.assertIn("_CHAT_MEMBER_FAILURE_TTL = 1.5", self.source)
        member_start = self.source.index("async def get_chat_member_cached")
        member_end = self.source.index("async def is_current_chat_member", member_start)
        member_helper = self.source[member_start:member_end]
        self.assertIn("request_timeout=_CHAT_MEMBER_REQUEST_TIMEOUT", member_helper)
        check_start = self.source.index("async def check_mod(")
        check_end = self.source.index("_SHORT_MUTE_TASKS:", check_start)
        check_body = self.source[check_start:check_end]
        self.assertIn("get_chat_member_cached(bot, message.chat.id, bot.id)", check_body)
        self.assertNotIn("await bot.get_chat_member(message.chat.id, bot.id)", check_body)

    def test_read_only_member_lists_do_not_fan_out_telegram_requests(self):
        start = self.source.index("async def filter_current_chat_members")
        end = self.source.index("async def is_admin", start)
        helper = self.source[start:end]
        self.assertNotIn("asyncio.gather", helper)
        self.assertNotIn("is_current_chat_member(", helper)
        self.assertIn("_CHAT_MEMBER_CACHE.get", helper)

    def test_only_mass_call_uses_notifying_profile_links(self):
        helper_start = self.source.index("def call_mention(")
        helper_end = self.source.index("# ── Database helpers", helper_start)
        self.assertIn("tg://user?id=", self.source[helper_start:helper_end])
        self.assertEqual(self.source.count("call_mention("), 2)
        profile_start = self.source.index("def profile_link(")
        profile_end = self.source.index("def call_mention(", profile_start)
        self.assertNotIn("tg://user?id=", self.source[profile_start:profile_end])


    def test_gdewtf_reads_fail_fast_and_safe_lists_can_use_short_stale_cache(self):
        self.assertIn("ClientTimeout(total=1.8, connect=0.7, sock_read=1.2)", self.source)
        self.assertIn("def _gdewtf_stale_read(", self.source)
        self.assertIn('path not in {"/mods", "/cities"}', self.source)
        self.assertIn('max_age = 30.0 if path == "/mods" else 300.0', self.source)
        self.assertIn('stale = _gdewtf_stale_read(cache_key, normalized_path)', self.source)
        # Locate helpers structurally instead of relying on exact blank lines
        # or the textual spelling of async function declarations.
        stale_node = next(
            node for node in self.tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_gdewtf_stale_read"
        )
        api_node = next(
            node for node in self.tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_api"
        )
        source_lines = self.source.splitlines()
        stale_helper = "\n".join(source_lines[stale_node.lineno - 1:api_node.lineno - 1])
        self.assertNotIn('"/mod"', stale_helper)
        self.assertIn('result["stale_cache"] = True', stale_helper)

if __name__ == "__main__":
    unittest.main()
