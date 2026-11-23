import ast
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "iris.py").read_text(encoding="utf-8")


class RuntimeReliabilityRegressionTests(unittest.TestCase):
    def test_gdewtf_api_circuit_breaker_is_wired_into_requests(self):
        self.assertIn("class _GDEWTFAPICircuitBreaker", SOURCE)
        self.assertIn("_GDEWTF_API_CIRCUIT.allow()", SOURCE)
        self.assertIn("_GDEWTF_API_CIRCUIT.failure()", SOURCE)
        self.assertIn("_GDEWTF_API_CIRCUIT.success()", SOURCE)
        self.assertIn('"detail": "circuit_open"', SOURCE)

    def test_get_updates_honours_telegram_retry_after(self):
        start = SOURCE.index("class LowLatencyBot")
        end = SOURCE.index("# Long polling needs an HTTP timeout", start)
        body = SOURCE[start:end]
        self.assertIn("isinstance(method, GetUpdates)", body)
        self.assertIn("except TelegramRetryAfter as exc:", body)
        self.assertIn("float(exc.retry_after)", body)
        self.assertIn("await asyncio.sleep(delay)", body)
        self.assertIn("except (TelegramNetworkError, TelegramServerError) as exc:", body)

    def test_transport_errors_do_not_trigger_secondary_chat_replies(self):
        start = SOURCE.index("async def global_error_handler")
        end = SOURCE.index("# Extra Russian moderation aliases", start)
        body = SOURCE[start:end]
        self.assertIn(
            "isinstance(exc, (TelegramNetworkError, TelegramServerError, TelegramRetryAfter))",
            body,
        )
        self.assertLess(body.index("return True"), body.index("update = getattr(event, \"update\", None)"))

    def test_gdewtf_authorization_is_bounded_and_fails_fast(self):
        start = SOURCE.index("async def _gdewtf_authorize")
        end = SOURCE.index("async def _gdewtf_safe_answer", start)
        body = SOURCE[start:end]
        self.assertIn("total=0.65", body)
        self.assertIn("transport_failure", body)
        self.assertIn("return False", body)

    def test_targetless_warn_and_ban_check_permissions_before_usage_hint(self):
        start = SOURCE.index("async def gdewtf_command")
        end = SOURCE.index("# The GDE.WTF router is mounted", start) if "# The GDE.WTF router is mounted" in SOURCE[start:] else start + 5000
        body = SOURCE[start:end]
        guard = 're.fullmatch(r"!\\s*(?:варн|warn|бан|ban)\\s*", text, re.IGNORECASE)'
        self.assertIn(guard, body)
        self.assertLess(body.index("await _gdewtf_authorize("), body.index(guard))
        self.assertIn("not has_reply_target", body)
        self.assertIn("has_reply_target = reply_user is not None", body)
        self.assertIn("replied_user = reply_user", body)

    def test_gdewtf_auth_outage_is_reported_as_unverified_permissions_not_action_timeout(self):
        start = SOURCE.index("async def _gdewtf_authorize_locked")
        end = SOURCE.index("async def _gdewtf_safe_answer", start)
        body = SOURCE[start:end]
        self.assertIn("Нет подтверждённых прав на эту команду", body)
        self.assertIn("Это не доказывает отсутствие прав", body)
        self.assertIn("доступ не предоставлен без проверки", body)
        self.assertNotIn('return False, "⚠️ " + _error_text(transport_failure)', body)

    def test_chat_moderation_owner_bypasses_chat_settings_and_membership_lookups(self):
        start = SOURCE.index("async def require_admin(")
        end = SOURCE.index("async def require_real_admin(", start)
        body = SOURCE[start:end]
        owner_guard = body.index("message.from_user.id == OWNER_ID")
        controller_guard = body.index("str(message.from_user.id) in _GDEWTF_CONTROLLER_TELEGRAM_IDS")
        first_settings_lookup = body.index("get_chat_settings")
        first_member_lookup = body.index("get_chat_member_cached")
        self.assertLess(owner_guard, first_settings_lookup)
        self.assertLess(controller_guard, first_settings_lookup)
        self.assertLess(owner_guard, first_member_lookup)
        self.assertLess(controller_guard, first_member_lookup)
        self.assertIn("return True", body[owner_guard:first_settings_lookup])

    def test_reply_delete_reports_the_specific_missing_delete_permission(self):
        start = SOURCE.index("async def delete_message_cmd")
        end = SOURCE.index('@router.message(Command("purge"))', start)
        body = SOURCE[start:end]
        self.assertIn("isinstance(target_error, TelegramForbiddenError)", body)
        self.assertIn("право «Удаление сообщений»", body)

    def test_reply_delete_runs_target_and_command_deletions_concurrently(self):
        start = SOURCE.index("async def delete_message_cmd")
        end = SOURCE.index('@router.message(Command("purge"))', start)
        body = SOURCE[start:end]
        self.assertIn("async def delete_one_message(item: Message)", body)
        self.assertIn("target_error, command_error = await asyncio.gather(", body)
        self.assertIn("delete_one_message(target_message)", body)
        self.assertIn("delete_one_message(message)", body)

    def test_configured_owner_bypasses_upstream_profile_lookup(self):
        start = SOURCE.index("async def _gdewtf_authorize(")
        end = SOURCE.index("async def _gdewtf_authorize_locked", start)
        body = SOURCE[start:end]
        self.assertIn('str(user_id) == str(OWNER_ID)', body)
        self.assertIn('str(user_id) in _GDEWTF_CONTROLLER_TELEGRAM_IDS', body)
        self.assertLess(body.index("return True, \"\""), body.index("lock = _GDEWTF_AUTH_LOCKS.setdefault"))

    def test_warn_and_ban_mutations_skip_slow_target_prelookup(self):
        start = SOURCE.index("target_actions_for_resolution = {")
        end = SOURCE.index("# Normalize target commands", start) if "# Normalize target commands" in SOURCE[start:] else start + 1200
        body = SOURCE[start:end]
        tree = ast.parse(SOURCE)
        resolver_assignment = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "target_actions_for_resolution"
                for target in node.targets
            )
        )
        configured_actions = {
            item.value for item in resolver_assignment.value.elts
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        }
        self.assertTrue(configured_actions.isdisjoint({"варн", "warn", "бан", "ban"}))
        self.assertIn("action in target_actions_for_resolution", body)
        self.assertIn("Mutation endpoints resolve the supplied identifier themselves.", body)

    def test_gdewtf_profile_is_public_read_only_for_every_telegram_user(self):
        start = SOURCE.index("async def _gdewtf_authorize(")
        end = SOURCE.index("async def _gdewtf_authorize_locked", start)
        body = SOURCE[start:end]
        public_guard = 'if action in {"профиль", "profile", "who", "mod"}:'
        self.assertIn(public_guard, body)
        self.assertLess(body.index(public_guard), body.index("lock = _GDEWTF_AUTH_LOCKS.setdefault"))
        self.assertIn("return True, \"\"", body)

    def test_bare_profile_uses_sender_and_reuses_single_resolved_profile(self):
        start = SOURCE.index("async def gdewtf_command")
        end = SOURCE.index('@gdewtf_router.callback_query(F.data.startswith("gdepanel:"))', start)
        body = SOURCE[start:end]
        self.assertIn('if action in {"профиль", "who", "mod"} and not rest and not replied_user:', body)
        self.assertIn("rest = [str(message.from_user.id)]", body)
        self.assertIn("mod, data = resolved_target_profile, {\"ok\": True, \"mod\": resolved_target_profile}", body)
        self.assertIn("_profile(mod)", body)
        self.assertNotIn("_gdewtf_chat_message_count", body)

    def test_gdewtf_profile_contains_only_upstream_profile_data(self):
        profile_start = SOURCE.index("def _profile(")
        profile_end = SOURCE.index("\n\n\ndef _help()", profile_start)
        profile = SOURCE[profile_start:profile_end]
        self.assertIn("ПРОФИЛЬ GDE.WTF", profile)
        self.assertIn("Рейтинг GDE.WTF", profile)
        self.assertNotIn("СТАТИСТИКА ПРОФИЛЯ ИМЕННО В ЭТОМ ЧАТЕ", profile)
        self.assertNotIn("bot_activity_daily", profile)
        self.assertNotIn("_gdewtf_chat_message_count", SOURCE)

    def test_bang_profile_is_not_captured_by_local_chat_profile(self):
        start = SOURCE.index('@router.message(F.text == "💠 Профиль")')
        end = SOURCE.index('@router.message(Command("time"))', start)
        local_profile = SOURCE[start:end]
        self.assertIn(r'(?i)^[./]?профиль(?:\s|$)', local_profile)
        self.assertNotIn(r'^[.!/]?профиль', local_profile)
        gde_start = SOURCE.index("async def gdewtf_command")
        gde_end = SOURCE.index('@gdewtf_router.callback_query(F.data.startswith("gdepanel:"))', gde_start)
        gde_handler = SOURCE[gde_start:gde_end]
        self.assertIn('if action in {"профиль", "who", "mod"}', gde_handler)
        self.assertIn("_profile(mod)", gde_handler)

    def test_chat_profile_query_selects_only_required_columns(self):
        start = SOURCE.index("def _chat_profile_stats(")
        end = SOURCE.index("\ndef db_rows(", start)
        body = SOURCE[start:end]
        self.assertIn('.select("messages,rank,warns")', body)
        self.assertIn('.eq("chat_id", int(chat_id))', body)
        self.assertIn('.eq("user_id", int(user_id))', body)
        self.assertIn('_db_read_with_retry("bot_users", "profile_stats", query)', body)

    def test_plain_profile_reads_only_the_current_chat_row_without_writes_or_gde_api(self):
        start = SOURCE.index("async def profile(message: Message)")
        end = SOURCE.index('@router.message(Command("time"))', start)
        body = SOURCE[start:end]
        # The helper accepts positional (chat_id, user_id); verify the actual call.
        self.assertIn("_chat_profile_stats, message.chat.id, message.from_user.id", body)
        self.assertNotIn("ensure_user(", body)
        self.assertNotIn("active_warn_count(", body)
        self.assertNotIn("_api(", body)
        self.assertIn("TelegramNetworkError", body)
        self.assertIn("not retrying", body)

    def test_mod_remove_timeout_does_not_chain_a_slow_verification_request(self):
        start = SOURCE.index('elif action in {"снять", "modremove"}:')
        end = SOURCE.index('elif action in {"бан", "ban"}:', start)
        body = SOURCE[start:end]
        self.assertIn("total=1.2", body)
        self.assertIn("automatic retry suppressed", body)
        self.assertNotIn("verify = await _api", body)
        self.assertNotIn("total=3", body)


    def test_gdewtf_router_ignores_unknown_bang_prose_before_authorization(self):
        start = SOURCE.index("async def gdewtf_command")
        end = SOURCE.index('@gdewtf_router.callback_query(F.data.startswith("gdepanel:"))', start)
        body = SOURCE[start:end]
        self.assertLess(body.index("if action_for_auth not in _GDEWTF_COMMAND_ACTIONS:"), body.index("await _gdewtf_authorize("))
        self.assertIn('"снять", "modremove", "бан", "ban"', SOURCE)
        self.assertIn('"профиль", "profile", "who"', SOURCE)

    def test_extra_moderation_aliases_match_only_at_message_start(self):
        start = SOURCE.index("_MODERATION_ALIAS_GROUPS = [")
        end = SOURCE.index("@router.message(F.text.regexp", start)
        body = SOURCE[start:end]
        self.assertIn(r'(?i)^\s*', body)
        self.assertIn('match = pattern.match(raw)', SOURCE)
        self.assertNotIn('(?i).*бан', body)

    def test_invisible_telegram_bidi_marks_are_removed_before_command_dispatch(self):
        start = SOURCE.index("async def normalize_dot_dk_prefix")
        end = SOURCE.index("router.message.outer_middleware(normalize_dot_dk_prefix)", start)
        body = SOURCE[start:end]
        self.assertIn(r"\u200e\u200f\u202a-\u202e\u2066-\u2069\ufeff", body)
        self.assertIn("_normalize_natural_moderation_phrase(command_text)", body)
        self.assertIn(r"^\s*-смс", SOURCE)

    def test_every_text_regexp_command_filter_is_anchored_to_message_start(self):
        tree = ast.parse(SOURCE)
        checked = 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "regexp" or not node.args:
                continue
            pattern = node.args[0]
            if not isinstance(pattern, ast.Constant) or not isinstance(pattern.value, str):
                continue
            value = pattern.value
            value = re.sub(r"^\(\?[aiLmsux-]+\)", "", value)
            self.assertTrue(
                value.startswith("^") or value.startswith(r"\A"),
                f"Command regexp is not anchored at message start: {pattern.value!r}",
            )
            checked += 1
        self.assertGreater(checked, 0)


    def test_city_assignment_commands_are_registered_and_accept_reply_targets(self):
        self.assertIn('"модератор", "рег", "rgov"', SOURCE)
        start = SOURCE.index("target_actions = {")
        end = SOURCE.index("if replied_user and action in target_actions:", start)
        target_actions = SOURCE[start:end]
        self.assertIn('"модератор"', target_actions)
        self.assertIn('"рег"', target_actions)
        decorator_start = SOURCE.index("async def gdewtf_command")
        decorator = SOURCE[SOURCE.rfind("@gdewtf_router.message", 0, decorator_start):decorator_start]
        self.assertIn("|модератор|рег|", decorator)

    def test_city_resolver_uses_cached_api_directory_and_rejects_ambiguous_names(self):
        start = SOURCE.index("async def _gdewtf_resolve_city_name(")
        end = SOURCE.index("async def _gdewtf_resolve_region_name(", start)
        body = SOURCE[start:end]
        self.assertIn('await _gdewtf_city_directory()', body)
        self.assertIn('city.get("id")', body)
        self.assertIn('"ambiguous_city"', body)
        self.assertIn('"city_not_found"', body)

    def test_region_resolver_uses_exact_region_and_directory_city_ids(self):
        start = SOURCE.index("async def _gdewtf_resolve_region_name(")
        end = SOURCE.index("def _gdewtf_profile_matches_telegram(", start)
        body = SOURCE[start:end]
        self.assertIn('city.get("region")', body)
        self.assertIn('city.get("id")', body)
        self.assertIn('return canonical, sorted(city_ids)', body)

    def test_ambassador_moderator_and_regional_commands_send_role_specific_payloads(self):
        start = SOURCE.index('elif action in {"амб", "amb", "модератор"}:')
        end = SOURCE.index('elif action in {"назначить", "modset"}:', start)
        body = SOURCE[start:end]
        self.assertIn('_gdewtf_resolve_city_name(" ".join(rest[1:]))', body)
        self.assertIn('"city": city.get("id")', body)
        self.assertIn('"perms": "Лента"', body)
        self.assertIn('body["amb"] = True', body)
        self.assertIn('elif action in {"рег", "rgov", "регион", "региональный"}:', body)
        self.assertIn('"rgov": True', body)
        self.assertIn('"perms": "Старший"', body)
        self.assertIn('body["cities"] = region_city_ids', body)
        self.assertIn('"POST", "/mod/set", body=body', body)

    def test_city_assignment_reports_directory_outage_without_claiming_permission_denial(self):
        start = SOURCE.index('elif action in {"амб", "amb", "модератор"}:')
        end = SOURCE.index('elif action in {"рег", "rgov"', start)
        body = SOURCE[start:end]
        self.assertIn('city_data.get("error") == "api_unavailable"', body)
        self.assertIn("Назначение НЕ выполнено", body)
        self.assertIn("это сбой внешнего api", body.casefold())

    def test_region_assignment_reports_directory_outage_without_claiming_permission_denial(self):
        start = SOURCE.index('elif action in {"рег", "rgov"')
        end = SOURCE.index('elif action in {"назначить", "modset"}:', start)
        body = SOURCE[start:end]
        self.assertIn('region_data.get("error") == "api_unavailable"', body)
        self.assertIn("Региональное назначение НЕ выполнено", body)
        self.assertIn("сбой внешнего api", body.casefold())

if __name__ == "__main__":
    unittest.main()
