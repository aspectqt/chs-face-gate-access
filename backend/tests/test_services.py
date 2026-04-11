import unittest
from io import BytesIO
from unittest.mock import patch

from services.otp_service import generate_otp_code, hash_otp_code, verify_otp_code
from services.sms_provider import PhilSmsProvider, SmsProvider
import urllib.error


class OtpServiceTests(unittest.TestCase):
    def test_generate_otp_code_applies_length_bounds(self):
        self.assertEqual(len(generate_otp_code(2)), 4)
        self.assertEqual(len(generate_otp_code(20)), 10)

    def test_hash_and_verify_otp(self):
        otp_code = "123456"
        otp_hash = hash_otp_code(otp_code)

        self.assertTrue(verify_otp_code(otp_hash, otp_code))
        self.assertFalse(verify_otp_code(otp_hash, "654321"))


class SmsProviderTests(unittest.TestCase):
    def test_normalize_phone_number_variants(self):
        expected = "+639171234567"

        for raw in ("+639171234567", "639171234567", "09171234567", "9171234567"):
            with self.subTest(raw=raw):
                self.assertEqual(SmsProvider.normalize_phone_number(raw), expected)

    def test_normalize_phone_number_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            SmsProvider.normalize_phone_number("12345")

    def test_render_template_substitutes_variables(self):
        rendered = SmsProvider.render_template(
            "Hello {name}, your status is {status}.",
            {"name": "Alex", "status": "Present"},
        )

        self.assertEqual(rendered, "Hello Alex, your status is Present.")

    def test_map_result_to_log_fields_normalizes_sent_payload(self):
        payload = SmsProvider.map_result_to_log_fields({
            "status": "sent",
            "provider_message_id": "abc123",
            "provider_response": {"ok": True},
            "http_status": 200,
        })

        self.assertEqual(payload["status"], "sent")
        self.assertEqual(payload["providerMessageId"], "abc123")
        self.assertIsNone(payload["error"])
        self.assertEqual(payload["httpStatus"], 200)

    def test_philsms_provider_retries_transient_http_failure_before_succeeding(self):
        provider = PhilSmsProvider(
            base_url="https://app.philsms.com/api/v3",
            api_token="test-token",
            max_retries=2,
            backoff_seconds=0.01,
        )

        class FakeResponse:
            def __init__(self, status_code, body):
                self._status_code = status_code
                self._body = body

            def getcode(self):
                return self._status_code

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        transient_error = urllib.error.HTTPError(
            "https://app.philsms.com/api/v3/sms/send",
            503,
            "Service Unavailable",
            hdrs=None,
            fp=BytesIO(b'{"message":"temporary outage"}'),
        )
        success_response = FakeResponse(200, b'{"status":"success","message_id":"sms-123"}')

        with patch("services.sms_provider.urllib.request.urlopen", side_effect=[transient_error, success_response]) as urlopen_mock, \
                patch("services.sms_provider.time.sleep"):
            result = provider.send_sms("09171234567", "Hello world")

        self.assertEqual(urlopen_mock.call_count, 2)
        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["provider_message_id"], "sms-123")

    def test_philsms_provider_falls_back_to_next_auth_strategy_after_auth_failure(self):
        provider = PhilSmsProvider(
            base_url="https://app.philsms.com/api/v3",
            api_token="test-token",
            max_retries=1,
            backoff_seconds=0.01,
        )

        class FakeResponse:
            def __init__(self, status_code, body):
                self._status_code = status_code
                self._body = body

            def getcode(self):
                return self._status_code

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        auth_error = urllib.error.HTTPError(
            "https://app.philsms.com/api/v3/sms/send",
            401,
            "Unauthorized",
            hdrs=None,
            fp=BytesIO(b'{"message":"Unauthenticated."}'),
        )
        success_response = FakeResponse(200, b'{"status":"sent","message_id":"sms-456"}')

        with patch("services.sms_provider.urllib.request.urlopen", side_effect=[auth_error, success_response]) as urlopen_mock:
            result = provider.send_sms("09171234567", "Hello world")

        self.assertEqual(urlopen_mock.call_count, 2)
        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["provider_message_id"], "sms-456")
