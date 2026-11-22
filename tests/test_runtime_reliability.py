import ast
import pathlib
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

    def test_targetless_warn_and_ban_skip_external_authorization(self):
        start = SOURCE.index("async def gdewtf_command")
        end = SOURCE.index("# The GDE.WTF router is mounted", start) if "# The GDE.WTF router is mounted" in SOURCE[start:] else start + 5000
        body = SOURCE[start:end]
        guard = 're.fullmatch(r"!\\s*(?:варн|warn|бан|ban)\\s*", text, re.IGNORECASE)'
        self.assertIn(guard, body)
        self.assertLess(body.index(guard), body.index("await _gdewtf_authorize("))
        self.assertIn("not has_reply_target", body)
        self.assertIn("has_reply_target = reply_user is not None", body)
        self.assertIn("replied_user = reply_user", body)
        self.assertLess(body.index("has_reply_target"), body.index("await _gdewtf_authorize("))

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

    def test_mod_remove_timeout_does_not_chain_a_slow_verification_request(self):
        start = SOURCE.index('elif action in {"снять", "modremove"}:')
        end = SOURCE.index('elif action in {"бан", "ban"}:', start)
        body = SOURCE[start:end]
        self.assertIn("total=1.2", body)
        self.assertIn("automatic retry suppressed", body)
        self.assertNotIn("verify = await _api", body)
        self.assertNotIn("total=3", body)


if __name__ == "__main__":
    unittest.main()
