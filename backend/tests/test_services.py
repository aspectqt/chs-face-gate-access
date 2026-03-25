import unittest

from services.otp_service import generate_otp_code, hash_otp_code, verify_otp_code
from services.sms_provider import SmsProvider


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
