import importlib
import os
import tempfile
import unittest
import uuid
from bson.objectid import ObjectId
from contextlib import ExitStack
from datetime import datetime, timedelta
from unittest.mock import DEFAULT, patch


class AppSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.app_module = importlib.import_module("app")
            cls.app_module.client.admin.command("ping")
        except Exception as exc:
            raise unittest.SkipTest(f"Local app smoke tests require MongoDB: {exc}") from exc

    def make_client(self, username="admin", role=None):
        client = self.app_module.app.test_client()
        with client.session_transaction() as session_data:
            session_data["admin"] = username
            session_data["role"] = role or self.app_module.ROLE_FULL_ADMIN
            session_data[self.app_module.CSRF_SESSION_KEY] = "test-csrf-token"
        return client

    def get_default_schedule_doc(self):
        return self.app_module.system_settings.find_one({"key": "default_schedule"})

    def restore_default_schedule_doc(self, original_doc):
        self.app_module.system_settings.delete_many({"key": "default_schedule"})
        if original_doc:
            restored = dict(original_doc)
            restored.pop("_id", None)
            self.app_module.system_settings.insert_one(restored)

    def get_sms_notification_settings_doc(self):
        return self.app_module.system_settings.find_one({"key": self.app_module.ATTENDANCE_SMS_NOTIFICATION_SETTINGS_KEY})

    def restore_sms_notification_settings_doc(self, original_doc):
        self.app_module.system_settings.delete_many({"key": self.app_module.ATTENDANCE_SMS_NOTIFICATION_SETTINGS_KEY})
        if original_doc:
            restored = dict(original_doc)
            restored.pop("_id", None)
            self.app_module.system_settings.insert_one(restored)

    def drop_enrollment_collection_if_orphaned(self, school_year_label):
        label = self.app_module.normalize_school_year_value(school_year_label)
        if not label:
            return
        if self.app_module.school_years.find_one({"label": label}):
            return
        collection_name = self.app_module.student_enrollment_collection_name(label)
        if collection_name not in self.app_module.db.list_collection_names():
            return
        if self.app_module.db[collection_name].count_documents({}, limit=1) == 0:
            self.app_module.db.drop_collection(collection_name)

    def test_full_admin_pages_render(self):
        client = self.make_client()

        for route in ("/dashboard", "/analytics", "/students", "/gate-logs", "/sms-logs", "/live-gate-monitoring", "/developers"):
            with self.subTest(route=route):
                response = client.get(route)
                self.assertEqual(response.status_code, 200)
                self.assertIn("text/html", response.content_type)

    def test_send_email_message_normalizes_grouped_gmail_app_password_before_login(self):
        env_file_content = "\n".join([
            "SMTP_HOST=smtp.gmail.com",
            "SMTP_PORT=587",
            "SMTP_USERNAME=aprilbryancordova@gmail.com",
            "SMTP_PASSWORD=ssvi tdqn gxjs tolq",
            "SMTP_FROM=aprilbryancordova@gmail.com",
            "SMTP_USE_TLS=1",
            "SMTP_USE_SSL=0",
        ])

        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = os.path.join(temp_dir, ".env")
            with open(env_path, "w", encoding="utf-8") as env_file:
                env_file.write(env_file_content)

            with patch.object(self.app_module, "ENV_FILE_PATH", env_path), patch.object(
                self.app_module.smtplib,
                "SMTP",
            ) as smtp_mock, patch.object(
                self.app_module.smtplib,
                "SMTP_SSL",
            ) as smtp_ssl_mock:
                server = smtp_mock.return_value.__enter__.return_value
                settings = self.app_module.smtp_settings()
                success, error = self.app_module.send_email_message(
                    subject="Password Reset",
                    body_text="Reset body",
                    recipients=["student@example.com"],
                )

        self.assertEqual(settings["password"], "ssvitdqngxjstolq")
        self.assertTrue(success)
        self.assertEqual(error, "")
        smtp_ssl_mock.assert_not_called()
        smtp_mock.assert_called_once_with("smtp.gmail.com", 587, timeout=20)
        server.starttls.assert_called_once()
        server.login.assert_called_once_with("aprilbryancordova@gmail.com", "ssvitdqngxjstolq")
        server.send_message.assert_called_once()

    def test_smtp_settings_use_latest_env_file_values_for_gmail_runtime_config(self):
        env_overrides = {
            "SMTP_HOST": "outdated.example.com",
            "SMTP_PORT": "2525",
            "SMTP_USERNAME": "stale@example.com",
            "SMTP_PASSWORD": "stale-password",
            "SMTP_FROM": "stale@example.com",
            "SMTP_USE_TLS": "0",
            "SMTP_USE_SSL": "0",
        }
        env_file_content = "\n".join([
            "SMTP_HOST=smtp.gmail.com",
            "SMTP_PORT=465",
            "SMTP_USERNAME=runtime.mailer@gmail.com",
            "SMTP_PASSWORD=abcd efgh ijkl mnop",
            "SMTP_FROM=runtime.mailer@gmail.com",
            "SMTP_USE_TLS=0",
            "SMTP_USE_SSL=1",
        ])

        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = os.path.join(temp_dir, ".env")
            with open(env_path, "w", encoding="utf-8") as env_file:
                env_file.write(env_file_content)

            with patch.object(self.app_module, "ENV_FILE_PATH", env_path), patch.dict(
                self.app_module.os.environ,
                env_overrides,
                clear=False,
            ):
                settings = self.app_module.smtp_settings()

        self.assertEqual(settings["host"], "smtp.gmail.com")
        self.assertEqual(settings["port"], 465)
        self.assertEqual(settings["username"], "runtime.mailer@gmail.com")
        self.assertEqual(settings["password"], "abcdefghijklmnop")
        self.assertEqual(settings["sender"], "runtime.mailer@gmail.com")
        self.assertTrue(settings["use_ssl"])
        self.assertFalse(settings["use_tls"])

    def test_forgot_password_request_sends_reset_email_for_registered_user(self):
        email = f"reset-{uuid.uuid4().hex[:10]}@example.com"
        username = f"reset_user_{uuid.uuid4().hex[:8]}"
        user_id = self.app_module.users.insert_one({
            "username": username,
            "password": "placeholder",
            "role": self.app_module.ROLE_FULL_ADMIN,
            "fullName": "Reset Test User",
            "email": email,
            "phone": "09171234567",
            "address": "Test Address",
            "bio": "",
            "createdAt": self.app_module.now_iso(),
            "updatedAt": self.app_module.now_iso(),
        }).inserted_id
        self.app_module.password_reset_tokens.delete_many({"email": email})

        try:
            client = self.app_module.app.test_client()
            with client.session_transaction() as session_data:
                session_data[self.app_module.CSRF_SESSION_KEY] = "forgot-password-csrf"
            with patch.object(
                self.app_module,
                "smtp_configuration_error",
                return_value="",
            ), patch.object(
                self.app_module,
                "PASSWORD_RESET_RATE_LIMIT_ENABLED",
                False,
            ), patch.object(
                self.app_module,
                "send_email_message",
                return_value=(True, ""),
            ) as send_email_mock:
                response = client.post(
                    "/api/auth/forgot-password/request",
                    json={"email": email},
                    headers={self.app_module.CSRF_HEADER_NAME: "forgot-password-csrf"},
                )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertIn("Password reset link has been sent", payload["message"])
            send_email_mock.assert_called_once()
            send_kwargs = send_email_mock.call_args.kwargs
            self.assertEqual(send_kwargs["subject"], "CHS Gate Access Password Reset")
            self.assertEqual(send_kwargs["recipients"], [email])
            self.assertIn("/reset-password?token=", send_kwargs["body_text"])

            token_doc = self.app_module.password_reset_tokens.find_one({"email": email, "used": False})
            self.assertIsNotNone(token_doc)
            self.assertEqual(token_doc["user_id"], user_id)
        finally:
            self.app_module.password_reset_tokens.delete_many({"email": email})
            self.app_module.users.delete_one({"_id": user_id})

    def test_profile_update_api_persists_user_changes(self):
        username = f"profile_user_{uuid.uuid4().hex[:8]}"
        email = f"profile-{uuid.uuid4().hex[:10]}@example.com"
        user_id = self.app_module.users.insert_one({
            "username": username,
            "password": "placeholder",
            "role": self.app_module.ROLE_FULL_ADMIN,
            "fullName": "Original User",
            "email": email,
            "phone": "09171234567",
            "address": "Old Address",
            "bio": "Old bio",
            "twoFactorEnabled": False,
            "createdAt": self.app_module.now_iso(),
            "updatedAt": self.app_module.now_iso(),
        }).inserted_id

        try:
            client = self.make_client(username=username, role=self.app_module.ROLE_FULL_ADMIN)
            csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
            updated_email = f"updated-{uuid.uuid4().hex[:10]}@example.com"
            response = client.put(
                "/api/profile",
                json={
                    "fullName": "Updated Admin User",
                    "email": updated_email,
                    "phone": "09987654321",
                    "address": "Updated Address",
                    "bio": "Updated biography",
                    "twoFactorEnabled": True,
                    "removeAvatar": False,
                },
                headers=csrf_headers,
            )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["profile"]["email"], updated_email)
            self.assertEqual(payload["profile"]["fullName"], "Updated Admin User")
            self.assertTrue(payload["profile"]["twoFactorEnabled"])

            saved_user = self.app_module.users.find_one({"_id": user_id})
            self.assertEqual(saved_user["email"], updated_email)
            self.assertEqual(saved_user["fullName"], "Updated Admin User")
            self.assertEqual(saved_user["phone"], "09987654321")
            self.assertEqual(saved_user["address"], "Updated Address")
            self.assertEqual(saved_user["bio"], "Updated biography")
            self.assertTrue(saved_user["twoFactorEnabled"])

            get_response = client.get("/api/profile")
            self.assertEqual(get_response.status_code, 200)
            get_payload = get_response.get_json()
            self.assertEqual(get_payload["profile"]["email"], updated_email)
            self.assertEqual(get_payload["profile"]["address"], "Updated Address")
        finally:
            self.app_module.users.delete_one({"_id": user_id})

    def test_login_page_renders_with_original_deped_logo_layout(self):
        client = self.app_module.app.test_client()

        response = client.get("/login")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.content_type)

        html = response.get_data(as_text=True)
        self.assertIn("deped-logo.png", html)
        self.assertIn("logo.png", html)
        self.assertIn("CHS Gate System | Login", html)
        self.assertIn('var forcedTheme = "light";', html)
        self.assertIn("Forgot Password?", html)
        self.assertIn("Sign In", html)

    def test_staff_pages_force_light_theme_while_admin_pages_keep_theme_unlocked(self):
        staff_client = self.make_client(username="staff_user", role=self.app_module.ROLE_STAFF)
        admin_client = self.make_client()

        staff_dashboard = staff_client.get("/dashboard")
        self.assertEqual(staff_dashboard.status_code, 200)
        staff_dashboard_html = staff_dashboard.get_data(as_text=True)
        self.assertIn('var forcedTheme = "light";', staff_dashboard_html)

        staff_students = staff_client.get("/students")
        self.assertEqual(staff_students.status_code, 200)
        self.assertIn('var forcedTheme = "light";', staff_students.get_data(as_text=True))

        admin_dashboard = admin_client.get("/dashboard")
        self.assertEqual(admin_dashboard.status_code, 200)
        self.assertIn('var forcedTheme = "";', admin_dashboard.get_data(as_text=True))

    def test_help_guide_page_surfaces_guides_navigation_and_faqs(self):
        client = self.make_client()

        response = client.get("/developers")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)

        self.assertIn("Help / Guide", html)
        self.assertIn("Quick Start", html)
        self.assertIn("Daily Workflows", html)
        self.assertIn("Module Guide", html)
        self.assertIn("Frequently Asked Questions", html)
        self.assertIn('href="#quick-start"', html)
        self.assertIn("real-time multi-face detection and recognition", html)
        self.assertNotIn("Start scanning with the correct mode and one student at a time.", html)
        self.assertIn("/dashboard", html)
        self.assertIn("/students", html)
        self.assertIn("/gate-logs", html)
        self.assertIn("/sms-logs", html)

    def test_empty_orphaned_enrollment_collections_do_not_reappear_as_school_years(self):
        orphan_year = "2096-2097"
        collection_name = self.app_module.student_enrollment_collection_name(orphan_year)

        self.app_module.school_years.delete_many({"label": orphan_year})
        self.app_module.db.drop_collection(collection_name)
        self.app_module.db.create_collection(collection_name)

        try:
            labels = self.app_module.list_student_enrollment_school_year_labels()
            self.assertNotIn(orphan_year, labels)
        finally:
            self.app_module.db.drop_collection(collection_name)

    def test_live_monitoring_page_uses_realtime_activity_stream_config(self):
        client = self.make_client()

        response = client.get("/live-gate-monitoring")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("Recent Student Activity", html)
        self.assertIn("/api/scan/stream", html)
        self.assertIn("streamHealthyPollIntervalMs", html)
        self.assertIn("live_monitoring_activity.js", html)

    def test_live_monitoring_token_bootstraps_service_before_issuing_token(self):
        client = self.make_client()

        with patch.object(
            self.app_module,
            "ensure_live_monitoring_service_ready",
            return_value=(True, "Live monitoring service is ready."),
        ) as ensure_mock:
            response = client.get("/api/live-monitoring/token")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["token"])
        ensure_mock.assert_called_once_with(reason="token_request", fail_hard=True)

    def test_live_monitoring_token_returns_503_when_service_bootstrap_fails(self):
        client = self.make_client()

        with patch.object(
            self.app_module,
            "ensure_live_monitoring_service_ready",
            return_value=(False, "Live monitoring bootstrap failed."),
        ) as ensure_mock:
            response = client.get("/api/live-monitoring/token")

        self.assertEqual(response.status_code, 503)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["message"], "Live monitoring bootstrap failed.")
        ensure_mock.assert_called_once_with(reason="token_request", fail_hard=True)

    def test_start_scan_reports_live_monitoring_warning_without_blocking_scan(self):
        client = self.make_client(username="staff_user", role=self.app_module.ROLE_STAFF)
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}

        with patch.object(
            self.app_module.sms_provider,
            "validate_configuration",
            return_value={"status": "ok"},
        ), patch.object(
            self.app_module,
            "ensure_live_monitoring_service_ready",
            return_value=(False, "Live monitoring bootstrap warning."),
        ) as ensure_mock, patch.object(
            self.app_module,
            "start_scan_capture",
            return_value=(True, "Scan started (waiting for client frames)"),
        ) as start_scan_capture_mock:
            response = client.post("/start_scan", json={}, headers=csrf_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["live_monitoring_warning"], "Live monitoring bootstrap warning.")
        ensure_mock.assert_called_once_with(reason="scan_start", fail_hard=False)
        start_scan_capture_mock.assert_called_once()

    def test_sms_notification_setting_persists_and_sms_logs_page_reflects_disabled_state(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        original_doc = self.get_sms_notification_settings_doc()

        try:
            response = client.post(
                "/sms-logs/notifications",
                json={"enabled": False},
                headers=csrf_headers,
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertFalse(payload["enabled"])
            self.assertEqual(payload["status_label"], "Disabled")

            saved_doc = self.get_sms_notification_settings_doc()
            self.assertIsNotNone(saved_doc)
            self.assertFalse(bool(saved_doc.get("enabled")))

            page_html = client.get("/sms-logs").get_data(as_text=True)
            self.assertIn("Attendance SMS Notifications", page_html)
            self.assertIn("smsNotificationsEnabled: false", page_html)
        finally:
            self.restore_sms_notification_settings_doc(original_doc)

    def test_sms_logs_page_surfaces_reorganized_template_editor(self):
        client = self.make_client()

        response = client.get("/sms-logs")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("Attendance SMS Template", html)
        self.assertIn("Message Content", html)
        self.assertIn("Live Preview", html)
        self.assertIn("templatePreviewText()", html)
        self.assertNotIn("Dynamic Variables", html)
        self.assertIn("Load Default", html)

    def test_log_attendance_records_skipped_sms_log_when_notifications_disabled(self):
        original_doc = self.get_sms_notification_settings_doc()
        student_id = f"SMSOFF-{uuid.uuid4().hex[:8]}"
        student_doc = {
            "student_id": student_id,
            "name": "SMS Disabled Student",
            "parent_contact": "09171234567",
            "status": "Active",
        }
        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)

        attendance_collection.delete_many({"student_id": student_id})
        sms_collection.delete_many({"student_id": student_id})

        try:
            self.app_module.save_attendance_sms_notification_settings(False, actor_username="tester", actor_role="Full Admin")
            with patch.object(self.app_module, "send_sms") as send_sms_mock, \
                    patch.object(self.app_module, "now_local", return_value=datetime(2026, 4, 12, 7, 35, 0)):
                result = self.app_module.log_attendance_and_sms(student_doc, send_notifications=True, mode="auto")

            self.assertIsNotNone(result)
            self.assertEqual(result["sms_status"], "disabled")
            send_sms_mock.assert_not_called()

            saved_sms = sms_collection.find_one({"student_id": student_id})
            self.assertIsNotNone(saved_sms)
            self.assertEqual(str(saved_sms.get("status") or "").lower(), "skipped")
            self.assertEqual(saved_sms.get("errorCode"), "SKIPPED")
            self.assertEqual((saved_sms.get("providerResponse") or {}).get("reason"), "notifications_disabled")
            self.assertIn("disabled", str(saved_sms.get("message") or "").lower())
        finally:
            attendance_collection.delete_many({"student_id": student_id})
            sms_collection.delete_many({"student_id": student_id})
            self.restore_sms_notification_settings_doc(original_doc)

    def test_sms_resend_logs_skipped_entry_when_notifications_disabled(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        original_doc = self.get_sms_notification_settings_doc()
        school_year_label = self.app_module.get_current_school_year_label()
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)
        student_id = f"SMSRESENDOFF-{uuid.uuid4().hex[:8]}"
        original_log_id = sms_collection.insert_one({
            "to": "+639171234567",
            "message": "Original resend message",
            "type": "transactional",
            "status": "failed",
            "provider": "PHILSMS",
            "providerMessageId": "",
            "providerResponse": {"phase": "provider_send", "meta": {"context": "attendance_gate_scan"}},
            "error": "Temporary gateway failure",
            "httpStatus": 503,
            "errorCode": "PROVIDER_ERROR",
            "errorMessage": "Temporary gateway failure",
            "createdAt": "2026-04-12T08:00:00",
            "updatedAt": "2026-04-12T08:00:00",
            "school_year": school_year_label,
            "student_id": student_id,
            "name": "Resend Disabled Student",
            "parent_contact": "09171234567",
            "parent_contact_raw": "09171234567",
            "retryEligible": True,
            "retryCount": 0,
            "retryMaxAttempts": 3,
            "nextRetryAt": "2026-04-12T08:05:00",
            "lastRetryError": "Temporary gateway failure",
            "sid": "",
            "timestamp": "2026-04-12T08:00:00",
            "date": "2026-04-12",
            "time": "08:00:00",
        }).inserted_id

        try:
            self.app_module.save_attendance_sms_notification_settings(False, actor_username="tester", actor_role="Full Admin")
            with patch.object(self.app_module, "send_sms") as send_sms_mock:
                response = client.post(f"/sms-logs/resend/{original_log_id}", headers=csrf_headers)

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["sms_status"], "DISABLED")
            send_sms_mock.assert_not_called()

            rows = list(sms_collection.find({"student_id": student_id}))
            self.assertEqual(len(rows), 2)
            skipped_row = next(
                (row for row in rows if str(row.get("status") or "").lower() == "skipped"),
                None,
            )
            self.assertIsNotNone(skipped_row)
            self.assertEqual(str(skipped_row.get("status") or "").lower(), "skipped")
            self.assertEqual((skipped_row.get("providerResponse") or {}).get("reason"), "notifications_disabled")
        finally:
            sms_collection.delete_many({"student_id": student_id})
            self.restore_sms_notification_settings_doc(original_doc)

    def test_process_pending_sms_retries_pauses_when_notifications_disabled(self):
        original_doc = self.get_sms_notification_settings_doc()
        school_year_label = self.app_module.get_current_school_year_label()
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)
        student_id = f"SMSRETRYOFF-{uuid.uuid4().hex[:8]}"
        log_id = sms_collection.insert_one({
            "to": "+639171234567",
            "message": "Retry while disabled",
            "type": "transactional",
            "status": "failed",
            "provider": "PHILSMS",
            "providerMessageId": "",
            "providerResponse": {"phase": "provider_send", "meta": {"context": "attendance_gate_scan"}},
            "error": "Temporary gateway failure",
            "httpStatus": 503,
            "errorCode": "PROVIDER_ERROR",
            "errorMessage": "Temporary gateway failure",
            "createdAt": "2020-04-12T09:00:00",
            "updatedAt": "2020-04-12T09:00:00",
            "school_year": school_year_label,
            "student_id": student_id,
            "name": "Retry Disabled Student",
            "parent_contact": "09171234567",
            "parent_contact_raw": "09171234567",
            "retryEligible": True,
            "retryCount": 0,
            "retryMaxAttempts": 3,
            "nextRetryAt": "2020-04-12T09:00:00",
            "lastRetryError": "Temporary gateway failure",
            "sid": "",
            "timestamp": "2020-04-12T09:00:00",
            "date": "2020-04-12",
            "time": "09:00:00",
        }).inserted_id

        try:
            self.app_module.save_attendance_sms_notification_settings(False, actor_username="tester", actor_role="Full Admin")
            with patch.object(self.app_module, "send_sms") as send_sms_mock:
                result = self.app_module.process_pending_sms_retries(max_logs=5)

            self.assertEqual(result["processed"], 0)
            self.assertEqual(result["sent"], 0)
            self.assertEqual(result["failed"], 0)
            self.assertEqual(result["remaining"], 1)
            send_sms_mock.assert_not_called()

            saved = sms_collection.find_one({"_id": log_id})
            self.assertEqual(saved.get("status"), "failed")
            self.assertTrue(saved.get("retryEligible"))
            self.assertEqual(saved.get("retryCount"), 0)
        finally:
            sms_collection.delete_many({"student_id": student_id})
            self.restore_sms_notification_settings_doc(original_doc)

    def test_scan_events_stream_emits_verified_events_immediately(self):
        client = self.make_client()
        response = None
        with self.app_module.scan_event_condition:
            original_events = [dict(row) for row in self.app_module.scan_state.get("events", [])]
            original_counter = int(self.app_module.scan_state.get("event_counter") or 0)

        try:
            pushed_event = self.app_module.push_scan_event("verified", {
                "student_id": "STREAM-TEST-001",
                "name": "Realtime Test Student",
                "gate_action": "IN",
                "time": "8:15 AM",
                "feed_update": True,
                "activity_entry": {
                    "student_id": "STREAM-TEST-001",
                    "student_name": "Realtime Test Student",
                    "gate_action": "IN",
                    "time": "8:15 AM",
                    "timestamp": self.app_module.now_iso(),
                },
            })

            response = client.get(
                f"/api/scan/stream?since={max(int(pushed_event.get('id') or 1) - 1, 0)}",
                buffered=False,
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("text/event-stream", response.content_type)

            stream_iter = iter(response.response)
            payload_text = ""
            for _ in range(4):
                payload_text += next(stream_iter).decode("utf-8")
                if "event: scan_event" in payload_text and "Realtime Test Student" in payload_text:
                    break

            self.assertIn("event: scan_event", payload_text)
            self.assertIn("Realtime Test Student", payload_text)
            self.assertIn("\"last_event_id\":", payload_text)
        finally:
            if response is not None:
                response.close()
            with self.app_module.scan_event_condition:
                self.app_module.scan_state["events"] = original_events
                self.app_module.scan_state["event_counter"] = original_counter

    def test_scan_events_snapshot_returns_verified_activity_entries_for_feed_recovery(self):
        client = self.make_client()
        with self.app_module.scan_event_condition:
            original_events = [dict(row) for row in self.app_module.scan_state.get("events", [])]
            original_counter = int(self.app_module.scan_state.get("event_counter") or 0)

        try:
            pushed_event = self.app_module.push_scan_event("verified", {
                "student_id": "POLL-TEST-001",
                "name": "Polling Recovery Student",
                "gate_action": "OUT",
                "time": "8:20 AM",
                "feed_update": True,
                "activity_entry": {
                    "student_id": "POLL-TEST-001",
                    "student_name": "Polling Recovery Student",
                    "gate_action": "OUT",
                    "time": "8:20 AM",
                    "timestamp": self.app_module.now_iso(),
                },
            })

            response = client.get(
                f"/scan_events?since={max(int(pushed_event.get('id') or 1) - 1, 0)}"
            )
            self.assertEqual(response.status_code, 200)

            payload = response.get_json()
            self.assertEqual(payload["last_event_id"], int(pushed_event["id"]))
            self.assertEqual(len(payload["events"]), 1)
            self.assertEqual(payload["events"][0]["type"], "verified")
            self.assertTrue(payload["events"][0]["feed_update"])
            self.assertEqual(payload["events"][0]["activity_entry"]["student_name"], "Polling Recovery Student")
            self.assertEqual(payload["events"][0]["activity_entry"]["gate_action"], "OUT")
        finally:
            with self.app_module.scan_event_condition:
                self.app_module.scan_state["events"] = original_events
                self.app_module.scan_state["event_counter"] = original_counter

    def test_dashboard_uses_current_minimalist_template_markers(self):
        client = self.make_client()

        response = client.get("/dashboard")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("dashboard-shell", html)
        self.assertIn("enhanced_client_camera.js", html)
        self.assertIn("Manrope", html)
        self.assertIn("Live Activity Feed", html)
        self.assertIn("scanActivityFeed", html)
        self.assertIn("Smart IN/OUT Tracking", html)
        self.assertIn("Smart IN/OUT Active", html)
        self.assertNotIn("Scanner Status", html)
        self.assertIn("Manual IN", html)
        self.assertIn("Manual OUT", html)
        self.assertIn("scan-activity-status-in", html)
        self.assertIn("scan-activity-glow-out", html)
        self.assertIn("Status: ${data.count} faces detected", html)
        self.assertNotIn("showScanEventBadge('warning', `${data.count} faces detected`)", html)

    def test_staff_dashboard_focuses_on_centered_live_recognition_layout(self):
        client = self.make_client(username="staff_user", role=self.app_module.ROLE_STAFF)

        response = client.get("/dashboard")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("staff-dashboard", html)
        self.assertIn('id="gateConsoleCard"', html)
        self.assertIn('id="gateConsoleColumn"', html)
        self.assertIn('id="gateSecondaryColumn"', html)
        self.assertIn('id="gateConsoleLayout"', html)
        self.assertIn('id="gateControlsColumn"', html)
        self.assertIn('id="gateCameraColumn"', html)
        self.assertIn('id="staffLogoutButton"', html)
        self.assertIn('data-logout-link="1"', html)
        self.assertIn('title="Log out"', html)
        self.assertIn("body.staff-dashboard .dashboard-main > *:not(#gate-scanning)", html)
        self.assertIn("body.staff-dashboard #gateSecondaryColumn", html)
        self.assertIn("body.staff-dashboard #gateConsoleLayout", html)
        self.assertIn(".gate-console-header-actions", html)
        self.assertIn("grid-template-columns: minmax(0, 1fr) 18rem", html)
        self.assertIn(".staff-dashboard-logout", html)
        self.assertIn("position: fixed;", html)
        self.assertIn("background: transparent;", html)
        self.assertIn("body.staff-dashboard #scanFrame", html)
        self.assertIn("scanActivityFeed", html)

    def test_shared_stat_card_layout_is_present_on_key_pages(self):
        client = self.make_client()

        for route in ("/dashboard", "/students", "/gate-logs", "/sms-logs", "/analytics"):
            with self.subTest(route=route):
                response = client.get(route)
                self.assertEqual(response.status_code, 200)
                html = response.get_data(as_text=True)
                self.assertIn("stat-overview-card", html)
                self.assertIn("stat-overview-kicker", html)
                self.assertIn("stat-overview-value", html)

    def test_dashboard_and_health_apis_return_expected_payloads(self):
        client = self.make_client()

        dashboard_response = client.get("/api/dashboard/stats")
        self.assertEqual(dashboard_response.status_code, 200)
        dashboard_payload = dashboard_response.get_json()
        self.assertEqual(dashboard_payload["status"], "ok")
        self.assertIn("total_students", dashboard_payload)
        self.assertIn("school_year", dashboard_payload)

        health_response = client.get("/api/system/health")
        self.assertEqual(health_response.status_code, 200)
        health_payload = health_response.get_json()
        self.assertEqual(health_payload["status"], "ok")
        self.assertIn("database", health_payload["health"])
        self.assertIn("queues", health_payload["health"])

    def test_notifications_api_reads_marks_and_streams_selected_school_year_alerts(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        school_year_label = self.app_module.get_current_school_year_label()
        alerts_collection, _, _ = self.app_module.get_alerts_storage(school_year_label)
        unique_suffix = uuid.uuid4().hex[:8]
        student_id = f"ALERT-{unique_suffix}"
        alert_id = None

        try:
            alert_id = alerts_collection.insert_one({
                "title": f"Audit Alert {unique_suffix}",
                "message": "Notification audit validation.",
                "student_id": student_id,
                "student_name": f"Alert Student {unique_suffix}",
                "school_year": school_year_label,
                "category": "system",
                "status": "unread",
                "is_read": False,
                "timestamp": self.app_module.now_iso(),
                "created_at": datetime.utcnow(),
            }).inserted_id

            list_response = client.get(f"/api/notifications?school_year={school_year_label}&limit=12")
            self.assertEqual(list_response.status_code, 200)
            list_payload = list_response.get_json()
            self.assertEqual(list_payload["status"], "ok")
            self.assertEqual(list_payload["school_year"], school_year_label)
            self.assertTrue(any(row["_id"] == str(alert_id) for row in list_payload["notifications"]))

            detail_response = client.get(f"/api/notifications/{alert_id}?school_year={school_year_label}")
            self.assertEqual(detail_response.status_code, 200)
            detail_payload = detail_response.get_json()
            self.assertEqual(detail_payload["status"], "ok")
            self.assertEqual(detail_payload["notification"]["_id"], str(alert_id))

            unread_before_response = client.get(f"/api/notifications/unread-count?school_year={school_year_label}")
            self.assertEqual(unread_before_response.status_code, 200)
            unread_before = int(unread_before_response.get_json()["unread"])

            mark_response = client.post(
                "/api/notifications/mark-read",
                json={"ids": [str(alert_id)], "school_year": school_year_label},
                headers=csrf_headers,
            )
            self.assertEqual(mark_response.status_code, 200)
            mark_payload = mark_response.get_json()
            self.assertEqual(mark_payload["status"], "ok")

            stored_alert = alerts_collection.find_one({"_id": alert_id})
            self.assertIsNotNone(stored_alert)
            self.assertEqual(stored_alert.get("status"), "read")
            self.assertTrue(stored_alert.get("is_read"))
            self.assertTrue(stored_alert.get("read_at"))

            unread_after_response = client.get(f"/api/notifications/unread-count?school_year={school_year_label}")
            self.assertEqual(unread_after_response.status_code, 200)
            unread_after = int(unread_after_response.get_json()["unread"])
            self.assertLessEqual(unread_after, unread_before)

            stream_response = client.get(
                f"/api/notifications/stream?school_year={school_year_label}",
                buffered=False,
            )
            self.assertEqual(stream_response.status_code, 200)
            self.assertIn("text/event-stream", stream_response.content_type)
        finally:
            if alert_id:
                alerts_collection.delete_many({"_id": alert_id})
            alerts_collection.delete_many({"student_id": student_id})

    def test_core_module_pages_and_runtime_endpoints_stay_connected_to_live_data(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)
        enrollment_collection = self.app_module.get_school_year_enrollment_collection(school_year_label)

        unique_suffix = uuid.uuid4().hex[:8]
        lrn = f"9000{unique_suffix[:6]}"
        student_name = f"Integration Student {unique_suffix}"
        marker = f"INT-{unique_suffix}"
        section_name = "AVILA"
        enrollment_id = None
        created_student_id = None

        try:
            create_response = client.post(
                "/api/students",
                json={
                    "lrn": lrn,
                    "name": student_name,
                    "grade_level": "Grade 7",
                    "section": section_name,
                    "parent_contact": "09171234567",
                    "gender": "Male",
                    "status": "Active",
                    "school_year": school_year_label,
                },
                headers=csrf_headers,
            )
            self.assertEqual(create_response.status_code, 201)
            create_payload = create_response.get_json()
            self.assertEqual(create_payload["status"], "ok")
            enrollment_id = create_payload["student"]["_id"]

            created_student = self.app_module.students.find_one({"student_id": lrn})
            self.assertIsNotNone(created_student)
            created_student_id = created_student["_id"]

            update_response = client.put(
                f"/api/students/{enrollment_id}",
                json={
                    "lrn": lrn,
                    "name": f"{student_name} Updated",
                    "grade_level": "Grade 7",
                    "section": section_name,
                    "parent_contact": "09171234567",
                    "gender": "Male",
                    "status": "Active",
                },
                headers=csrf_headers,
            )
            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(update_response.get_json()["status"], "ok")

            list_response = client.get(f"/api/students?q={lrn}&school_year={school_year_label}")
            self.assertEqual(list_response.status_code, 200)
            list_payload = list_response.get_json()
            self.assertEqual(list_payload["status"], "ok")
            self.assertGreaterEqual(int(list_payload["total"]), 1)
            self.assertTrue(any(row["student_id"] == lrn for row in list_payload["students"]))

            attendance_collection.insert_one({
                "student_id": lrn,
                "student_name": f"{student_name} Updated",
                "school_year": school_year_label,
                "timestamp": "2026-04-11T07:31:00",
                "date": "2026-04-11",
                "time": "07:31:00",
                "gate_action": "IN",
                "session": "Morning",
                "status": "Present",
                "verification_label": marker,
                "source": "integration_probe",
            })
            sms_collection.insert_one({
                "student_id": lrn,
                "student_name": f"{student_name} Updated",
                "name": f"{student_name} Updated",
                "school_year": school_year_label,
                "timestamp": "2026-04-11T07:32:00",
                "date": "2026-04-11",
                "time": "07:32:00",
                "status": "sent",
                "message": f"{marker} guardian message",
                "sms_type": "transactional",
                "provider": "integration_probe",
                "parent_contact": "09171234567",
            })

            module_routes = (
                f"/dashboard?q={marker}",
                "/live-gate-monitoring",
                f"/students?q={lrn}&school_year={school_year_label}",
                f"/gate-logs?q={marker}&school_year={school_year_label}",
                f"/sms-logs?q={marker}&school_year={school_year_label}",
                f"/analytics?range=day&start_date=2026-04-11&end_date=2026-04-11&school_year={school_year_label}",
            )
            for route in module_routes:
                with self.subTest(route=route):
                    response = client.get(route)
                    self.assertEqual(response.status_code, 200)

            gate_logs_html = client.get(f"/gate-logs?q={marker}&school_year={school_year_label}").get_data(as_text=True)
            sms_logs_html = client.get(f"/sms-logs?q={marker}&school_year={school_year_label}").get_data(as_text=True)
            analytics_html = client.get(
                f"/analytics?range=day&start_date=2026-04-11&end_date=2026-04-11&school_year={school_year_label}"
            ).get_data(as_text=True)
            self.assertIn(marker, gate_logs_html)
            self.assertIn(marker, sms_logs_html)
            self.assertIn("Attendance Distribution", analytics_html)

            gate_latest_response = client.get(f"/api/gate-logs/latest?school_year={school_year_label}")
            self.assertEqual(gate_latest_response.status_code, 200)
            gate_latest_payload = gate_latest_response.get_json()
            latest_rows = gate_latest_payload.get("logs") or gate_latest_payload.get("rows") or []
            self.assertTrue(any(marker in str(row) for row in latest_rows))

            dashboard_stats_response = client.get(f"/api/dashboard/stats?school_year={school_year_label}")
            self.assertEqual(dashboard_stats_response.status_code, 200)
            dashboard_stats_payload = dashboard_stats_response.get_json()
            self.assertEqual(dashboard_stats_payload["status"], "ok")
            self.assertEqual(dashboard_stats_payload["school_year"], school_year_label)

            live_token_response = client.get("/api/live-monitoring/token")
            self.assertEqual(live_token_response.status_code, 200)
            live_token_payload = live_token_response.get_json()
            self.assertEqual(live_token_payload["status"], "ok")
            self.assertTrue(live_token_payload["token"])

            delete_response = client.delete(f"/api/students/{enrollment_id}", headers=csrf_headers)
            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(delete_response.get_json()["status"], "ok")
        finally:
            attendance_collection.delete_many({"student_id": lrn})
            sms_collection.delete_many({"student_id": lrn})
            enrollment_collection.delete_many({"student_id": lrn})
            if created_student_id:
                self.app_module.students.delete_many({"_id": created_student_id})
            self.app_module.students.delete_many({"student_id": lrn})

    def test_scan_session_mode_api_round_trip_supports_manual_in_and_auto(self):
        client = self.make_client(username="staff_user", role=self.app_module.ROLE_STAFF)
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        original_mode = self.app_module.get_scan_session_mode()

        try:
            response = client.post(
                "/api/scan/session-mode",
                json={"mode": "manual_in"},
                headers=csrf_headers,
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["mode"], "manual_in")
            self.assertEqual(payload["mode_label"], "Manual IN")
            self.assertEqual(payload["effective_session"]["gate_action"], "IN")
            self.assertEqual(payload["effective_session"]["verification_label"], "Verified In")

            reset_response = client.post(
                "/api/scan/session-mode",
                json={"mode": "auto"},
                headers=csrf_headers,
            )
            self.assertEqual(reset_response.status_code, 200)
            reset_payload = reset_response.get_json()
            self.assertEqual(reset_payload["status"], "ok")
            self.assertEqual(reset_payload["mode"], "auto")
            self.assertEqual(reset_payload["mode_label"], "Smart IN/OUT Tracking")
            self.assertEqual(reset_payload["effective_session"]["gate_action"], "AUTO")
            self.assertEqual(reset_payload["effective_session"]["verification_label"], "Smart IN/OUT")
        finally:
            self.app_module.set_scan_session_mode(original_mode)

    def test_manual_lrn_scan_api_processes_entry_and_emits_verified_event(self):
        client = self.make_client(username="staff_user", role=self.app_module.ROLE_STAFF)
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_id = f"MANLRN-{uuid.uuid4().hex[:8]}"
        fake_student = {
            "student_id": student_id,
            "name": "Manual LRN Student",
            "parent_contact": "09171234567",
            "status": "Active",
        }
        fake_result = {
            "student_id": student_id,
            "student_name": "Manual LRN Student",
            "status": "Present",
            "session": "Manual Out 8:01 AM",
            "timestamp": "2026-04-30T08:01:02",
            "date": "2026-04-30",
            "time": "08:01:02",
            "gate_action": "OUT",
            "verification_label": "Thank You",
            "display_message": "Thank You",
            "voice_message": "Thank you",
            "duplicate": False,
            "duplicate_reason": "",
            "feed_update": True,
            "activity_entry": {
                "student_id": student_id,
                "name": "Manual LRN Student",
                "gate_action": "OUT",
                "status": "Present",
                "verification_label": "Thank You",
                "timestamp": "2026-04-30T08:01:02",
                "time": "8:01 AM",
                "label": "Manual LRN Student (OUT)",
            },
            "tracking_mode": "manual_out",
            "sms_status": "queued",
        }

        self.app_module.last_scanned.pop(student_id, None)
        try:
            with patch.object(self.app_module.students, "find_one", return_value=fake_student), patch.object(
                self.app_module, "log_attendance_and_sms", return_value=fake_result
            ), patch.object(
                self.app_module, "get_scan_session_mode", return_value="manual_out"
            ), patch.object(
                self.app_module, "resolve_live_scan_repeat_hold_seconds", return_value=7.5
            ), patch.object(
                self.app_module, "mark_persistent_face_scan"
            ) as mark_persistent_mock, patch.object(
                self.app_module, "push_scan_event"
            ) as push_scan_event_mock:
                response = client.post(
                    "/api/scan/manual-lrn",
                    json={"lrn": student_id},
                    headers=csrf_headers,
                )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertFalse(payload["duplicate"])
            self.assertTrue(payload["event_emitted"])
            self.assertEqual(payload["gate_action"], "OUT")
            self.assertEqual(payload["student_id"], student_id)
            self.assertEqual(payload["tracking_mode"], "manual_out")

            push_scan_event_mock.assert_called_once()
            event_type, event_payload = push_scan_event_mock.call_args.args
            self.assertEqual(event_type, "verified")
            self.assertEqual(event_payload["student_id"], student_id)
            self.assertEqual(event_payload["gate_action"], "OUT")
            self.assertEqual(event_payload["tracking_mode"], "manual_out")
            mark_persistent_mock.assert_called_once()

            cooldown_entry = self.app_module.last_scanned.get(student_id)
            self.assertIsNotNone(cooldown_entry)
            self.assertEqual(cooldown_entry.get("gate_action"), "OUT")
            self.assertEqual(cooldown_entry.get("mode"), "manual_out")
            self.assertGreater(float(cooldown_entry.get("until_ts", 0.0)), 0.0)
        finally:
            self.app_module.last_scanned.pop(student_id, None)

    def test_manual_lrn_scan_api_duplicate_keeps_response_ok_without_event_emission(self):
        client = self.make_client(username="staff_user", role=self.app_module.ROLE_STAFF)
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_id = f"MANLRN-DUP-{uuid.uuid4().hex[:8]}"
        fake_student = {
            "student_id": student_id,
            "name": "Duplicate Manual LRN Student",
            "parent_contact": "09171234567",
            "status": "Active",
        }
        fake_result = {
            "student_id": student_id,
            "student_name": "Duplicate Manual LRN Student",
            "status": "Present",
            "session": "Live IN 8:01 AM",
            "timestamp": "2026-04-30T08:01:02",
            "date": "2026-04-30",
            "time": "08:01:02",
            "gate_action": "IN",
            "verification_label": "Already Recorded",
            "display_message": "Already recorded moments ago.",
            "voice_message": "Already recorded",
            "duplicate": True,
            "duplicate_reason": "duplicate_key",
            "feed_update": False,
            "activity_entry": None,
            "tracking_mode": "auto",
            "sms_status": "skipped",
        }

        with patch.object(self.app_module.students, "find_one", return_value=fake_student), patch.object(
            self.app_module, "log_attendance_and_sms", return_value=fake_result
        ), patch.object(
            self.app_module, "get_scan_session_mode", return_value="auto"
        ), patch.object(
            self.app_module, "push_scan_event"
        ) as push_scan_event_mock:
            response = client.post(
                "/api/scan/manual-lrn",
                json={"lrn": student_id},
                headers=csrf_headers,
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["duplicate"])
        self.assertFalse(payload["event_emitted"])
        self.assertEqual(payload["duplicate_reason"], "duplicate_key")
        push_scan_event_mock.assert_not_called()

    def test_gate_logs_action_filter_matches_smart_in_out_records(self):
        client = self.make_client()
        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        marker = f"ACTIONFILTER-{uuid.uuid4().hex[:8]}"
        in_name = f"{marker} In Student"
        out_name = f"{marker} Out Student"

        attendance_collection.insert_many([
            {
                "student_id": f"{marker}-IN",
                "student_name": in_name,
                "school_year": school_year_label,
                "timestamp": "2026-04-12T07:30:00",
                "date": "2026-04-12",
                "time": "07:30:00",
                "gate_action": "IN",
                "session": "Live IN 7:30 AM",
                "status": "Present",
                "verification_label": "Welcome",
                "source": "integration_probe",
                "tracking_mode": "auto",
            },
            {
                "student_id": f"{marker}-OUT",
                "student_name": out_name,
                "school_year": school_year_label,
                "timestamp": "2026-04-12T08:30:00",
                "date": "2026-04-12",
                "time": "08:30:00",
                "gate_action": "OUT",
                "session": "Live OUT 8:30 AM",
                "status": "Present",
                "verification_label": "Thank You",
                "source": "integration_probe",
                "tracking_mode": "auto",
            },
        ])

        try:
            in_html = client.get(
                f"/gate-logs?q={marker}&session=IN&school_year={school_year_label}"
            ).get_data(as_text=True)
            out_html = client.get(
                f"/gate-logs?q={marker}&session=OUT&school_year={school_year_label}"
            ).get_data(as_text=True)

            self.assertIn("value=\"IN\"", in_html)
            self.assertIn("Verified In", in_html)
            self.assertIn(in_name, in_html)
            self.assertNotIn(out_name, in_html)

            self.assertIn("value=\"OUT\"", out_html)
            self.assertIn("Verified Out", out_html)
            self.assertIn(out_name, out_html)
            self.assertNotIn(in_name, out_html)
        finally:
            attendance_collection.delete_many({"student_id": {"$in": [f"{marker}-IN", f"{marker}-OUT"]}})

    def test_default_schedule_api_supports_dynamic_threshold_and_cooldown_settings(self):
        client = self.make_client()
        original_doc = self.get_default_schedule_doc()
        payload = {
            "morning_start": "05:00",
            "noon_start": "12:00",
            "afternoon_start": "13:00",
            "afternoon_end": "17:00",
            "late_threshold_minutes": 20,
            "scan_cooldown_minutes": 1,
        }
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}

        try:
            response = client.post("/api/schedule/default", json=payload, headers=csrf_headers)
            self.assertEqual(response.status_code, 200)
            body = response.get_json()
            self.assertEqual(body["status"], "ok")
            self.assertEqual(body["schedule"]["late_threshold_minutes"], 20)
            self.assertEqual(body["schedule"]["scan_cooldown_minutes"], 1)
            self.assertEqual(body["schedule"]["morning_late"], "05:20")
            self.assertEqual(body["schedule"]["afternoon_late"], "13:20")

            fetched = client.get("/api/schedule/default")
            self.assertEqual(fetched.status_code, 200)
            fetched_body = fetched.get_json()
            self.assertEqual(fetched_body["schedule"]["scan_cooldown_minutes"], 1)
            self.assertEqual(fetched_body["schedule"]["late_threshold_minutes"], 20)
        finally:
            self.restore_default_schedule_doc(original_doc)

    def test_pdf_export_controls_are_present_on_export_pages(self):
        client = self.make_client()

        students_response = client.get("/students")
        self.assertEqual(students_response.status_code, 200)
        students_html = students_response.get_data(as_text=True)
        self.assertIn("studentsExportModal", students_html)
        self.assertIn("Download PDF", students_html)
        self.assertIn("Print PDF", students_html)
        self.assertIn('data-download-mode="native"', students_html)

        for route, modal_id in (("/gate-logs", "gateLogsExportModal"), ("/sms-logs", "smsLogsExportModal")):
            with self.subTest(route=route):
                response = client.get(route)
                self.assertEqual(response.status_code, 200)
                html = response.get_data(as_text=True)
                self.assertIn(modal_id, html)
                self.assertIn("By Grade Level", html)
                self.assertIn("By Section", html)
                self.assertIn("Individual Student", html)
                self.assertIn("Download PDF", html)
                self.assertIn("Print PDF", html)
                self.assertIn('data-download-mode="native"', html)

    def test_students_page_uses_guided_face_registration_ui(self):
        client = self.make_client()

        response = client.get("/students")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)

        self.assertIn("Start Registration", html)
        self.assertIn("Standard Profile", html)
        self.assertIn("Similar Faces Mode", html)
        self.assertIn("10 guided angles", html)
        self.assertIn("20 captures for twins or closely matching facial features", html)
        self.assertNotIn("Capture sequence (automatic)", html)
        self.assertNotIn("Remove Cap", html)

    def test_staff_user_is_redirected_from_analytics(self):
        client = self.make_client(username="staff", role=self.app_module.ROLE_STAFF)

        response = client.get("/analytics", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/dashboard"))

    def test_students_pdf_export_returns_current_pdf_output(self):
        client = self.make_client()

        students_response = client.get("/students")
        self.assertEqual(students_response.status_code, 200)
        students_html = students_response.get_data(as_text=True)
        self.assertIn("studentsExportBtn", students_html)
        self.assertIn("/students/export_pdf", students_html)

        pdf_response = client.get("/students/export_pdf")
        self.assertEqual(pdf_response.status_code, 200)
        self.assertEqual(pdf_response.content_type, "application/pdf")
        self.assertIn("no-store", pdf_response.headers.get("Cache-Control", ""))
        self.assertEqual(pdf_response.headers.get("X-Content-Type-Options"), "nosniff")

        payload = pdf_response.get_data()
        self.assertTrue(payload.startswith(b"%PDF-"))
        self.assertIn(b"Official Student Records Report", payload)
        self.assertIn(b"REGION VII - CENTRAL VISAYAS", payload)
        self.assertIn(b"CAWITAN HIGH SCHOOL", payload)
        self.assertIn(b"Prepared by", payload)
        self.assertIn(b"Approved by", payload)

        inline_response = client.get("/students/export_pdf?disposition=inline")
        self.assertEqual(inline_response.status_code, 200)
        self.assertIn("inline", inline_response.headers.get("Content-Disposition", "").lower())

    def test_early_timeout_request_lifecycle_creates_request_and_gate_log_records(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        school_year_label = self.app_module.get_current_school_year_label()
        eto_collection, _, _ = self.app_module.get_early_timeout_requests_storage(school_year_label)
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        enrollment_collection = self.app_module.get_school_year_enrollment_collection(school_year_label)

        unique_suffix = uuid.uuid4().hex[:8]
        student_id = f"ETO-{unique_suffix}"
        student_name = f"ETO Student {unique_suffix}"
        inserted_student_id = None

        try:
            inserted_student_id = self.app_module.students.insert_one({
                "student_id": student_id,
                "name": student_name,
                "parent_contact": "09171234567",
                "status": "Active",
            }).inserted_id
            stored_student = self.app_module.students.find_one({"_id": inserted_student_id})
            self.app_module.upsert_student_enrollment(
                stored_student,
                school_year_label,
                grade_level="Grade 11",
                section="ETO-A",
                status="Active",
                update_existing=True,
            )

            page_response = client.get(f"/early-timeout?school_year={school_year_label}")
            self.assertEqual(page_response.status_code, 200)

            submit_response = client.post(
                "/api/early-timeout/request",
                json={
                    "student_id": student_id,
                    "requested_by": "ETO Tester",
                    "reason": "Medical pickup required today.",
                    "urgency": "urgent",
                },
                headers=csrf_headers,
            )
            self.assertEqual(submit_response.status_code, 200)
            submit_payload = submit_response.get_json()
            self.assertEqual(submit_payload["status"], "ok")

            request_id = submit_payload["request_id"]
            stored_request = eto_collection.find_one({"_id": ObjectId(request_id)})
            self.assertIsNotNone(stored_request)
            self.assertEqual(stored_request["status"], "pending")
            self.assertEqual(stored_request["student_name"], student_name)
            self.assertEqual(stored_request["school_year"], school_year_label)

            list_response = client.get("/api/early-timeout/requests")
            self.assertEqual(list_response.status_code, 200)
            list_payload = list_response.get_json()
            self.assertEqual(list_payload["status"], "ok")
            self.assertTrue(any(row["_id"] == request_id for row in list_payload["rows"]))

            count_response = client.get("/api/early-timeout/requests/count")
            self.assertEqual(count_response.status_code, 200)
            count_payload = count_response.get_json()
            self.assertEqual(count_payload["status"], "ok")
            self.assertGreaterEqual(int(count_payload["pending_count"]), 1)

            approve_response = client.post(
                f"/api/early-timeout/requests/{request_id}/approve",
                json={"review_note": "Approved for parent pickup."},
                headers=csrf_headers,
            )
            self.assertEqual(approve_response.status_code, 200)
            approve_payload = approve_response.get_json()
            self.assertEqual(approve_payload["status"], "ok")

            approved_request = eto_collection.find_one({"_id": ObjectId(request_id)})
            self.assertEqual(approved_request["status"], "approved")
            self.assertTrue(approved_request.get("attendance_log_id"))

            attendance_doc = attendance_collection.find_one({"_id": ObjectId(approved_request["attendance_log_id"])})
            self.assertIsNotNone(attendance_doc)
            self.assertTrue(attendance_doc.get("early_timeout"))
            self.assertEqual(attendance_doc.get("early_timeout_request_id"), request_id)
            self.assertEqual(attendance_doc.get("gate_action"), "OUT")
            self.assertEqual(attendance_doc.get("status"), "Present")

            second_submit = client.post(
                "/api/early-timeout/request",
                json={
                    "student_id": student_id,
                    "requested_by": "ETO Tester",
                    "reason": "Family emergency follow-up.",
                    "urgency": "normal",
                },
                headers=csrf_headers,
            )
            self.assertEqual(second_submit.status_code, 200)
            second_request_id = second_submit.get_json()["request_id"]

            deny_response = client.post(
                f"/api/early-timeout/requests/{second_request_id}/deny",
                json={"review_note": "Incomplete verification."},
                headers=csrf_headers,
            )
            self.assertEqual(deny_response.status_code, 200)
            deny_payload = deny_response.get_json()
            self.assertEqual(deny_payload["status"], "ok")

            denied_request = eto_collection.find_one({"_id": ObjectId(second_request_id)})
            self.assertIsNotNone(denied_request)
            self.assertEqual(denied_request["status"], "denied")
            self.assertFalse(denied_request.get("attendance_log_id"))

            delete_response = client.delete(
                f"/api/early-timeout/requests/{second_request_id}",
                headers=csrf_headers,
            )
            self.assertEqual(delete_response.status_code, 200)
            self.assertIsNone(eto_collection.find_one({"_id": ObjectId(second_request_id)}))
        finally:
            eto_collection.delete_many({"student_id": student_id})
            attendance_collection.delete_many({"student_id": student_id, "early_timeout": True})
            enrollment_collection.delete_many({"student_id": student_id})
            if inserted_student_id:
                self.app_module.students.delete_many({"_id": inserted_student_id})
            self.app_module.students.delete_many({"student_id": student_id})

    def test_early_timeout_endpoints_honor_selected_school_year_across_request_flow(self):
        current_school_year = self.app_module.get_current_school_year_label()
        current_start_year = int(current_school_year.split("-", 1)[0])
        other_school_year = f"{current_start_year - 1}-{current_start_year}"
        existing_school_year = self.app_module.school_years.find_one({"label": other_school_year})
        self.app_module.ensure_school_year_exists(other_school_year, set_current=False, created_by="test-suite")

        client = self.make_client()
        with client.session_transaction() as session_data:
            session_data[self.app_module.SCHOOL_YEAR_SESSION_KEY] = other_school_year

        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        eto_collection_other, resolved_school_year, archived_view = self.app_module.get_early_timeout_requests_storage(other_school_year)
        attendance_collection_other, _, _ = self.app_module.get_attendance_logs_storage(other_school_year)
        enrollment_collection_other = self.app_module.get_school_year_enrollment_collection(other_school_year)
        eto_collection_current, _, _ = self.app_module.get_early_timeout_requests_storage(current_school_year)

        self.assertEqual(resolved_school_year, other_school_year)
        self.assertTrue(archived_view)

        unique_suffix = uuid.uuid4().hex[:8]
        student_id = f"ETO-SY-{unique_suffix}"
        student_name = f"ETO Scoped {unique_suffix}"
        inserted_student_id = None

        try:
            inserted_student_id = self.app_module.students.insert_one({
                "student_id": student_id,
                "name": student_name,
                "parent_contact": "09179990000",
                "status": "Active",
            }).inserted_id
            stored_student = self.app_module.students.find_one({"_id": inserted_student_id})
            self.app_module.upsert_student_enrollment(
                stored_student,
                other_school_year,
                grade_level="Grade 10",
                section="ETO-B",
                status="Active",
                update_existing=True,
            )

            submit_response = client.post(
                "/api/early-timeout/request",
                json={
                    "student_id": student_id,
                    "requested_by": "Scoped Tester",
                    "reason": "Scoped school year validation.",
                    "urgency": "normal",
                },
                headers=csrf_headers,
            )
            self.assertEqual(submit_response.status_code, 200)
            request_id = submit_response.get_json()["request_id"]

            stored_request = eto_collection_other.find_one({"_id": ObjectId(request_id)})
            self.assertIsNotNone(stored_request)
            self.assertEqual(stored_request["school_year"], other_school_year)
            self.assertIsNone(eto_collection_current.find_one({"_id": ObjectId(request_id)}))

            count_response = client.get("/api/early-timeout/requests/count")
            self.assertEqual(count_response.status_code, 200)
            count_payload = count_response.get_json()
            self.assertEqual(count_payload["status"], "ok")
            self.assertGreaterEqual(int(count_payload["pending_count"]), 1)

            list_response = client.get("/api/early-timeout/requests")
            self.assertEqual(list_response.status_code, 200)
            list_payload = list_response.get_json()
            self.assertEqual(list_payload["status"], "ok")
            self.assertTrue(any(row["_id"] == request_id for row in list_payload["rows"]))

            deny_response = client.post(
                f"/api/early-timeout/requests/{request_id}/deny",
                json={"review_note": "Scoped school year denial."},
                headers=csrf_headers,
            )
            self.assertEqual(deny_response.status_code, 200)
            denied_request = eto_collection_other.find_one({"_id": ObjectId(request_id)})
            self.assertIsNotNone(denied_request)
            self.assertEqual(denied_request["status"], "denied")

            delete_response = client.delete(
                f"/api/early-timeout/requests/{request_id}",
                headers=csrf_headers,
            )
            self.assertEqual(delete_response.status_code, 200)
            self.assertIsNone(eto_collection_other.find_one({"_id": ObjectId(request_id)}))
        finally:
            eto_collection_other.delete_many({"student_id": student_id})
            attendance_collection_other.delete_many({"student_id": student_id, "early_timeout": True})
            enrollment_collection_other.delete_many({"student_id": student_id})
            if inserted_student_id:
                self.app_module.students.delete_many({"_id": inserted_student_id})
            self.app_module.students.delete_many({"student_id": student_id})
            if not existing_school_year:
                self.app_module.school_years.delete_many({"label": other_school_year})
                self.drop_enrollment_collection_if_orphaned(other_school_year)

    def test_calendar_endpoints_honor_selected_school_year_and_storage_routing(self):
        current_school_year = self.app_module.get_current_school_year_label()
        current_start_year = int(current_school_year.split("-", 1)[0])
        archived_school_year = "2098-2099"
        existing_school_year = self.app_module.school_years.find_one({"label": archived_school_year})
        self.app_module.ensure_school_year_exists(archived_school_year, set_current=False, created_by="test-suite")

        client = self.make_client()
        with client.session_transaction() as session_data:
            session_data[self.app_module.SCHOOL_YEAR_SESSION_KEY] = archived_school_year

        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        archived_collection, resolved_school_year, archived_view = self.app_module.get_calendar_events_storage(archived_school_year)
        current_collection, resolved_current_school_year, current_archived_view = self.app_module.get_calendar_events_storage(current_school_year)

        self.assertEqual(resolved_school_year, archived_school_year)
        self.assertTrue(archived_view)
        self.assertEqual(resolved_current_school_year, current_school_year)
        self.assertFalse(current_archived_view)

        def pick_available_date(collection, year, month):
            for day in range(10, 29):
                candidate = f"{year:04d}-{month:02d}-{day:02d}"
                if not collection.find_one({"date": candidate}):
                    return candidate
            self.fail(f"Could not find a free date in {year:04d}-{month:02d}.")

        unique_suffix = uuid.uuid4().hex[:8]
        archived_date = pick_available_date(archived_collection, 2098, 7)
        current_date = pick_available_date(current_collection, current_start_year, 7)
        created_title = f"Calendar Scoped {unique_suffix}"
        updated_title = f"Calendar Routed {unique_suffix}"
        event_id = None

        try:
            page_response = client.get(f"/calendar?school_year={archived_school_year}")
            self.assertEqual(page_response.status_code, 200)
            page_html = page_response.get_data(as_text=True)
            self.assertIn(f'data-selected-school-year="{archived_school_year}"', page_html)
            self.assertIn("school_year: selectedSchoolYear", page_html)

            create_response = client.post(
                "/api/calendar/events",
                json={
                    "date": archived_date,
                    "title": created_title,
                    "type": "event",
                    "special_condition": "Archive storage regression test",
                    "school_year": archived_school_year,
                },
                headers=csrf_headers,
            )
            self.assertEqual(create_response.status_code, 200)
            create_payload = create_response.get_json()
            self.assertEqual(create_payload["status"], "ok")
            event_id = create_payload["event"]["_id"]
            object_id = ObjectId(event_id)

            stored_archived = archived_collection.find_one({"_id": object_id})
            self.assertIsNotNone(stored_archived)
            self.assertEqual(stored_archived["school_year"], archived_school_year)
            self.assertEqual(stored_archived["title"], created_title)
            self.assertIsNone(current_collection.find_one({"_id": object_id}))

            implicit_list_response = client.get(f"/api/calendar/events?start={archived_date}&end={archived_date}")
            self.assertEqual(implicit_list_response.status_code, 200)
            implicit_list_payload = implicit_list_response.get_json()
            self.assertEqual(implicit_list_payload["status"], "ok")
            self.assertTrue(any(row["_id"] == event_id for row in implicit_list_payload["events"]))

            explicit_list_response = client.get(
                f"/api/calendar/events?start={archived_date}&end={archived_date}&school_year={archived_school_year}"
            )
            self.assertEqual(explicit_list_response.status_code, 200)
            explicit_list_payload = explicit_list_response.get_json()
            self.assertTrue(any(row["_id"] == event_id for row in explicit_list_payload["events"]))

            update_response = client.put(
                f"/api/calendar/events/{event_id}",
                json={
                    "date": current_date,
                    "title": updated_title,
                    "type": "holiday",
                    "special_condition": "Moved to current storage",
                    "school_year": current_school_year,
                },
                headers=csrf_headers,
            )
            self.assertEqual(update_response.status_code, 200)
            update_payload = update_response.get_json()
            self.assertEqual(update_payload["status"], "ok")

            self.assertIsNone(archived_collection.find_one({"_id": object_id}))
            moved_event = current_collection.find_one({"_id": object_id})
            self.assertIsNotNone(moved_event)
            self.assertEqual(moved_event["school_year"], current_school_year)
            self.assertEqual(moved_event["date"], current_date)
            self.assertEqual(moved_event["title"], updated_title)
            self.assertEqual(moved_event["type"], "holiday")

            current_list_response = client.get(
                f"/api/calendar/events?start={current_date}&end={current_date}&school_year={current_school_year}"
            )
            self.assertEqual(current_list_response.status_code, 200)
            current_list_payload = current_list_response.get_json()
            self.assertTrue(any(row["_id"] == event_id for row in current_list_payload["events"]))

            archived_list_response = client.get(
                f"/api/calendar/events?start={archived_date}&end={archived_date}&school_year={archived_school_year}"
            )
            self.assertEqual(archived_list_response.status_code, 200)
            archived_list_payload = archived_list_response.get_json()
            self.assertFalse(any(row["_id"] == event_id for row in archived_list_payload["events"]))

            delete_response = client.delete(
                f"/api/calendar/events/{event_id}?school_year={current_school_year}",
                headers=csrf_headers,
            )
            self.assertEqual(delete_response.status_code, 200)
            delete_payload = delete_response.get_json()
            self.assertEqual(delete_payload["status"], "ok")
            self.assertIsNone(current_collection.find_one({"_id": object_id}))
        finally:
            archived_collection.delete_many({"title": {"$in": [created_title, updated_title]}})
            archived_collection.delete_many({"date": {"$in": [archived_date, current_date]}})
            current_collection.delete_many({"title": {"$in": [created_title, updated_title]}})
            current_collection.delete_many({"date": {"$in": [archived_date, current_date]}})
            if not existing_school_year:
                self.app_module.school_years.delete_many({"label": archived_school_year})
                self.drop_enrollment_collection_if_orphaned(archived_school_year)

    def test_archive_summary_reports_calendar_and_early_timeout_storage(self):
        current_school_year = self.app_module.get_current_school_year_label()
        archived_school_year = "2097-2098"
        existing_school_year = self.app_module.school_years.find_one({"label": archived_school_year})
        self.app_module.ensure_school_year_exists(archived_school_year, set_current=False, created_by="test-suite")

        active_eto_collection, _, _ = self.app_module.get_early_timeout_requests_storage(current_school_year)
        archived_eto_collection, _, _ = self.app_module.get_early_timeout_requests_storage(archived_school_year)
        active_calendar_collection, _, _ = self.app_module.get_calendar_events_storage(current_school_year)
        archived_calendar_collection, _, _ = self.app_module.get_calendar_events_storage(archived_school_year)

        def pick_available_date(collection, year, month):
            for day in range(10, 29):
                candidate = f"{year:04d}-{month:02d}-{day:02d}"
                if not collection.find_one({"date": candidate}):
                    return candidate
            self.fail(f"Could not find a free date in {year:04d}-{month:02d}.")

        unique_suffix = uuid.uuid4().hex[:8]
        active_student_id = f"ARCHIVE-ACT-{unique_suffix}"
        archived_student_id = f"ARCHIVE-ARC-{unique_suffix}"
        active_calendar_date = pick_available_date(active_calendar_collection, 2099, 9)
        archived_calendar_date = pick_available_date(archived_calendar_collection, 2098, 2)

        try:
            with self.app_module.app.test_request_context(f"/admin/archive-summary?school_year={archived_school_year}"):
                baseline_payload = self.app_module.build_archive_summary_payload(archived_school_year)

            active_eto_collection.insert_one({
                "student_id": active_student_id,
                "student_name": "Archive Summary Active",
                "school_year": current_school_year,
                "status": "pending",
                "requested_by": "test-suite",
                "reason": "Active summary validation.",
                "created_at": datetime.utcnow(),
            })
            archived_eto_collection.insert_one({
                "student_id": archived_student_id,
                "student_name": "Archive Summary Archived",
                "school_year": archived_school_year,
                "status": "approved",
                "requested_by": "test-suite",
                "reason": "Archived summary validation.",
                "created_at": datetime.utcnow(),
            })
            active_calendar_collection.insert_one({
                "date": active_calendar_date,
                "title": f"Archive Summary Active {unique_suffix}",
                "type": "event",
                "school_year": current_school_year,
                "special_condition": "Active summary validation",
            })
            archived_calendar_collection.insert_one({
                "date": archived_calendar_date,
                "title": f"Archive Summary Archived {unique_suffix}",
                "type": "holiday",
                "school_year": archived_school_year,
                "special_condition": "Archived summary validation",
            })

            with self.app_module.app.test_request_context(f"/admin/archive-summary?school_year={archived_school_year}"):
                payload = self.app_module.build_archive_summary_payload(archived_school_year)

            self.assertEqual(
                payload["archive_totals"]["active_storage_total"],
                baseline_payload["archive_totals"]["active_storage_total"] + 2,
            )
            self.assertEqual(
                payload["archive_totals"]["archived_storage_total"],
                baseline_payload["archive_totals"]["archived_storage_total"] + 2,
            )

            current_row = next(row for row in payload["school_year_rows"] if row["label"] == current_school_year)
            archived_row = next(row for row in payload["school_year_rows"] if row["label"] == archived_school_year)
            self.assertGreaterEqual(current_row["eto_active"], 1)
            self.assertGreaterEqual(current_row["calendar_active"], 1)
            self.assertGreaterEqual(archived_row["eto_archive"], 1)
            self.assertGreaterEqual(archived_row["calendar_archive"], 1)

            storage_health_labels = {row["label"] for row in payload["storage_health_rows"]}
            self.assertIn("Early Time-Out Requests", storage_health_labels)
            self.assertIn("Calendar Events", storage_health_labels)

            inventory_names = {row["name"] for row in payload["collection_inventory"]}
            self.assertIn("early_timeout_requests", inventory_names)
            self.assertIn("early_timeout_requests_archive", inventory_names)
            self.assertIn("calendar_events", inventory_names)
            self.assertIn("calendar_events_archive", inventory_names)

            client = self.make_client()
            page_response = client.get(f"/admin/archive-summary?school_year={archived_school_year}")
            self.assertEqual(page_response.status_code, 200)
            html = page_response.get_data(as_text=True)
            self.assertIn("Selected Workspace Snapshot", html)
            self.assertIn("School Year Archive Overview", html)
            self.assertIn("Collection Inventory by Category", html)
            self.assertIn("Early Time-Out", html)
            self.assertIn("Calendar", html)
            self.assertIn(f"/early-timeout?school_year={archived_school_year}", html)
            self.assertIn(f"/calendar?school_year={archived_school_year}", html)
        finally:
            active_eto_collection.delete_many({"student_id": active_student_id})
            archived_eto_collection.delete_many({"student_id": archived_student_id})
            active_calendar_collection.delete_many({"date": active_calendar_date})
            archived_calendar_collection.delete_many({"date": archived_calendar_date})
            if not existing_school_year:
                self.app_module.school_years.delete_many({"label": archived_school_year})
                self.drop_enrollment_collection_if_orphaned(archived_school_year)

    def test_report_signature_markup_suppresses_placeholder_duplicates(self):
        prepared_markup = self.app_module.build_report_signature_markup("Prepared by", "Admin", "Administrator")
        approved_markup = self.app_module.build_report_signature_markup("Approved by", "Principal", "Principal")

        self.assertIn("Administrator", prepared_markup)
        self.assertNotIn(">Admin<", prepared_markup)
        self.assertEqual(approved_markup.count("Principal"), 1)

    def test_students_pdf_export_supports_grade_section_and_individual_scopes(self):
        school_year_label = self.app_module.get_current_school_year_label()
        collection = self.app_module.get_school_year_enrollment_collection(school_year_label)
        sample_student = collection.find_one({}, {"_id": 1, "student_id": 1, "grade_level": 1, "section": 1})
        if not sample_student:
            self.skipTest("No student data available for scoped PDF export checks.")

        client = self.make_client()
        grade_value = str(sample_student.get("grade_level") or "")
        section_value = str(sample_student.get("section") or "")
        student_id = str(sample_student.get("student_id") or "")
        student_record_id = str(sample_student.get("_id") or "")

        checks = [
            (f"/students/export_pdf?scope=grade&grade={grade_value}&school_year={school_year_label}", "grade"),
            (f"/students/export_pdf?scope=section&grade={grade_value}&section={section_value}&school_year={school_year_label}", "section"),
            (f"/students/export_pdf?scope=student&student_record_id={student_record_id}&student_id={student_id}&school_year={school_year_label}", "individual"),
            (f"/students/export_pdf?grade={grade_value}", "legacy-grade"),
            (f"/students/export_pdf?section={section_value}", "legacy-section"),
            (f"/students/export_pdf?q={student_id}", "legacy-individual"),
        ]

        for url, label in checks:
            with self.subTest(scope=label):
                response = client.get(url)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content_type, "application/pdf")
                self.assertTrue(response.get_data().startswith(b"%PDF-"))

    def test_gate_and_sms_pdf_exports_return_pdf_responses(self):
        client = self.make_client()

        for route in ("/gate-logs/export", "/sms-logs/export"):
            with self.subTest(route=route):
                response = client.get(route)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content_type, "application/pdf")
                self.assertIn("no-store", response.headers.get("Cache-Control", ""))
                self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")
                payload = response.get_data()
                self.assertTrue(payload.startswith(b"%PDF-"))
                self.assertIn(b"SCHOOLS DIVISION OF NEGROS ORIENTAL", payload)
                self.assertIn(b"CAWITAN HIGH SCHOOL", payload)
                self.assertIn(b"Prepared by", payload)
                self.assertIn(b"Approved by", payload)

                inline_response = client.get(f"{route}?disposition=inline")
                self.assertEqual(inline_response.status_code, 200)
                self.assertIn("inline", inline_response.headers.get("Content-Disposition", "").lower())

    def test_gate_and_sms_pdf_exports_support_grade_section_and_student_scopes(self):
        client = self.make_client()
        school_year_label = self.app_module.get_current_school_year_label()
        unique_suffix = uuid.uuid4().hex[:8]
        student_id = f"EXPORT-{unique_suffix}"
        student_name = f"Scoped Export {unique_suffix}"
        grade_level = "Grade 11"
        section_value = f"SEC-{unique_suffix[:4].upper()}"
        timestamp_value = datetime(2026, 3, 28, 8, 15, 0).isoformat()

        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)

        inserted_student_id = None
        inserted_attendance_id = None
        inserted_sms_id = None

        try:
            inserted_student_id = self.app_module.students.insert_one({
                "student_id": student_id,
                "name": student_name,
                "parent_contact": "09171234567",
                "status": "Active",
            }).inserted_id
            stored_student = self.app_module.students.find_one({"_id": inserted_student_id})
            self.app_module.upsert_student_enrollment(
                stored_student,
                school_year_label,
                grade_level=grade_level,
                section=section_value,
                status="Active",
                update_existing=True,
            )
            enrollment_collection = self.app_module.get_school_year_enrollment_collection(school_year_label)
            enrollment_row = enrollment_collection.find_one({"student_id": student_id}, {"_id": 1})
            self.assertIsNotNone(enrollment_row)

            inserted_attendance_id = attendance_collection.insert_one({
                "student_id": student_id,
                "student_name": student_name,
                "date": "2026-03-28",
                "time": "08:15",
                "timestamp": timestamp_value,
                "gate_action": "IN",
                "session": "AM",
                "status": "Present",
                "verification_label": "Verified",
                "source": "test-suite",
                "school_year": school_year_label,
            }).inserted_id
            inserted_sms_id = sms_collection.insert_one({
                "student_id": student_id,
                "name": student_name,
                "parent_contact": "09171234567",
                "date": "2026-03-28",
                "time": "08:16",
                "timestamp": timestamp_value,
                "status": self.app_module.sms_status_mongo_filter("sent"),
                "message": f"Scoped export test for {student_name}",
                "sid": f"SM-{unique_suffix}",
                "error": "",
                "school_year": school_year_label,
            }).inserted_id

            scoped_urls = [
                f"/gate-logs/export?scope=grade&grade={grade_level}&school_year={school_year_label}",
                f"/gate-logs/export?scope=section&grade={grade_level}&section={section_value}&school_year={school_year_label}",
                f"/gate-logs/export?scope=student&student_record_id={enrollment_row['_id']}&student_id={student_id}&school_year={school_year_label}",
                f"/sms-logs/export?scope=grade&grade={grade_level}&school_year={school_year_label}",
                f"/sms-logs/export?scope=section&grade={grade_level}&section={section_value}&school_year={school_year_label}",
                f"/sms-logs/export?scope=student&student_record_id={enrollment_row['_id']}&student_id={student_id}&school_year={school_year_label}",
            ]

            for url in scoped_urls:
                with self.subTest(url=url):
                    response = client.get(url)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.content_type, "application/pdf")
                    payload = response.get_data()
                    self.assertTrue(payload.startswith(b"%PDF-"))
                    self.assertIn(b"Scoped Export", payload)
                    self.assertIn(unique_suffix.encode("utf-8"), payload)
        finally:
            if inserted_attendance_id is not None:
                attendance_collection.delete_many({"_id": inserted_attendance_id})
            if inserted_sms_id is not None:
                sms_collection.delete_many({"_id": inserted_sms_id})
            self.app_module.get_school_year_enrollment_collection(school_year_label).delete_many({"student_id": student_id})
            if inserted_student_id is not None:
                self.app_module.students.delete_many({"_id": inserted_student_id})

    def test_simulated_gate_scans_follow_tracked_in_out_rules(self):
        client = self.make_client()
        student_id = f"SIMTEST-{uuid.uuid4().hex[:10]}"
        student_doc = {
            "student_id": student_id,
            "name": "Simulation Student",
            "parent_contact": "",
            "status": "Active",
        }

        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)

        self.app_module.students.delete_many({"student_id": student_id})
        attendance_collection.delete_many({"student_id": student_id})
        self.app_module.students.insert_one(student_doc)

        try:
            cooldown_minutes = self.app_module.get_default_schedule()["scan_cooldown_minutes"]
            first_scan_time = datetime(2026, 3, 25, 7, 30, 0)
            csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
            with patch.object(self.app_module, "now_local", return_value=first_scan_time):
                first_response = client.post(f"/simulate-gate/{student_id}", headers=csrf_headers)
            self.assertEqual(first_response.status_code, 200)
            first_payload = first_response.get_json()
            self.assertEqual(first_payload["action"], "IN")
            self.assertFalse(first_payload["duplicate"])

            with patch.object(self.app_module, "now_local", return_value=first_scan_time + timedelta(minutes=10)):
                duplicate_response = client.post(f"/simulate-gate/{student_id}", headers=csrf_headers)
            self.assertEqual(duplicate_response.status_code, 200)
            duplicate_payload = duplicate_response.get_json()
            self.assertEqual(duplicate_payload["action"], "IN")
            self.assertTrue(duplicate_payload["duplicate"])
            self.assertIn("wait", duplicate_payload["message"].lower())

            with patch.object(self.app_module, "now_local", return_value=first_scan_time + timedelta(minutes=cooldown_minutes + 1)):
                second_response = client.post(f"/simulate-gate/{student_id}", headers=csrf_headers)
            self.assertEqual(second_response.status_code, 200)
            second_payload = second_response.get_json()
            self.assertEqual(second_payload["action"], "OUT")
            self.assertFalse(second_payload["duplicate"])

            stored_rows = list(
                attendance_collection.find({"student_id": student_id}).sort("timestamp", 1)
            )
            self.assertEqual(len(stored_rows), 2)
            self.assertEqual(stored_rows[0].get("gate_action"), "IN")
            self.assertEqual(stored_rows[1].get("gate_action"), "OUT")
        finally:
            attendance_collection.delete_many({"student_id": student_id})
            self.app_module.students.delete_many({"student_id": student_id})

    def test_manual_out_bypasses_wait_period_and_out_resets_to_immediate_in(self):
        student_id = f"MANUAL-{uuid.uuid4().hex[:10]}"
        student_doc = {
            "student_id": student_id,
            "name": "Manual Session Student",
            "parent_contact": "",
            "status": "Active",
        }

        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        attendance_collection.delete_many({"student_id": student_id})
        original_mode = self.app_module.get_scan_session_mode()

        try:
            first_scan_time = datetime(2026, 3, 25, 7, 30, 0)
            forced_out_time = first_scan_time + timedelta(minutes=10)
            reentry_time = forced_out_time + timedelta(seconds=1)

            self.app_module.set_scan_session_mode("auto")
            with patch.object(self.app_module, "now_local", return_value=first_scan_time):
                first_result = self.app_module.log_attendance_and_sms(student_doc, send_notifications=False)
            self.assertIsNotNone(first_result)
            self.assertFalse(first_result["duplicate"])
            self.assertEqual(first_result["gate_action"], "IN")

            self.app_module.set_scan_session_mode("manual_out")
            with patch.object(self.app_module, "now_local", return_value=forced_out_time):
                forced_out_result = self.app_module.log_attendance_and_sms(student_doc, send_notifications=False)
            self.assertIsNotNone(forced_out_result)
            self.assertFalse(forced_out_result["duplicate"])
            self.assertEqual(forced_out_result["gate_action"], "OUT")
            self.assertEqual(forced_out_result["tracking_mode"], "manual_out")

            self.app_module.set_scan_session_mode("auto")
            with patch.object(self.app_module, "now_local", return_value=reentry_time):
                reentry_result = self.app_module.log_attendance_and_sms(student_doc, send_notifications=False)
            self.assertIsNotNone(reentry_result)
            self.assertFalse(reentry_result["duplicate"])
            self.assertEqual(reentry_result["gate_action"], "IN")

            stored_rows = list(attendance_collection.find({"student_id": student_id}).sort("timestamp", 1))
            self.assertEqual([row.get("gate_action") for row in stored_rows], ["IN", "OUT", "IN"])
        finally:
            self.app_module.set_scan_session_mode(original_mode)
            attendance_collection.delete_many({"student_id": student_id})

    def test_live_face_handler_silently_ignores_duplicates_until_window_expires(self):
        student_id = f"HANDLER-{uuid.uuid4().hex[:10]}"
        student = {
            "student_id": student_id,
            "name": "Handler Student",
            "parent_contact": "",
            "status": "Active",
        }

        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        attendance_collection.delete_many({"student_id": student_id})
        self.app_module.last_scanned.pop(student_id, None)
        self.app_module.scan_presence_locks.pop(student_id, None)

        try:
            first_scan_time = datetime(2026, 3, 25, 7, 30, 0)
            second_scan_time = first_scan_time + timedelta(minutes=10)
            cooldown_minutes = self.app_module.get_default_schedule()["scan_cooldown_minutes"]
            third_scan_time = first_scan_time + timedelta(minutes=cooldown_minutes + 1)

            with patch.object(self.app_module, "push_scan_event") as push_event_mock:
                with patch.object(self.app_module, "now_local", return_value=first_scan_time), \
                        patch.object(self.app_module.time, "time", return_value=first_scan_time.timestamp()):
                    first_result = self.app_module.handle_verified_student(student, confidence=99.0)
                self.assertIsNotNone(first_result)
                self.assertEqual(first_result["gate_action"], "IN")
                self.assertEqual(push_event_mock.call_count, 1)
                first_payload = push_event_mock.call_args_list[0].args[1]
                self.assertIn("voice_key", first_payload)
                self.assertIn("AM", first_payload["time"])
                self.assertIn("AM", first_payload["timestamp_display"])

                with patch.object(self.app_module, "now_local", return_value=second_scan_time), \
                        patch.object(self.app_module.time, "time", return_value=second_scan_time.timestamp()):
                    duplicate_result = self.app_module.handle_verified_student(student, confidence=99.0)
                self.assertIsNone(duplicate_result)
                self.assertEqual(push_event_mock.call_count, 1)

                with patch.object(self.app_module, "now_local", return_value=third_scan_time), \
                        patch.object(self.app_module.time, "time", return_value=third_scan_time.timestamp()):
                    third_result = self.app_module.handle_verified_student(student, confidence=99.0)
                self.assertIsNotNone(third_result)
                self.assertEqual(third_result["gate_action"], "OUT")
                self.assertEqual(push_event_mock.call_count, 2)
        finally:
            attendance_collection.delete_many({"student_id": student_id})
            self.app_module.last_scanned.pop(student_id, None)
            self.app_module.scan_presence_locks.pop(student_id, None)

    def test_log_attendance_debounces_same_manual_action_across_new_session_name(self):
        student_id = f"DEBOUNCE-{uuid.uuid4().hex[:10]}"
        student_doc = {
            "student_id": student_id,
            "name": "Debounced Student",
            "parent_contact": "",
            "status": "Active",
        }

        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        attendance_collection.delete_many({"student_id": student_id})

        try:
            first_scan_time = datetime(2026, 3, 25, 7, 30, 0)
            second_scan_time = first_scan_time + timedelta(seconds=61)

            with patch.object(self.app_module, "now_local", return_value=first_scan_time):
                first_result = self.app_module.log_attendance_and_sms(
                    student_doc,
                    send_notifications=False,
                    mode="manual_out",
                )
            self.assertIsNotNone(first_result)
            self.assertFalse(first_result["duplicate"])
            self.assertEqual(first_result["gate_action"], "OUT")

            with patch.object(self.app_module, "now_local", return_value=second_scan_time):
                duplicate_result = self.app_module.log_attendance_and_sms(
                    student_doc,
                    send_notifications=False,
                    mode="manual_out",
                )
            self.assertIsNotNone(duplicate_result)
            self.assertTrue(duplicate_result["duplicate"])
            self.assertEqual(duplicate_result["duplicate_reason"], "event_debounce")
            self.assertIn("wait", duplicate_result["display_message"].lower())

            stored_rows = list(attendance_collection.find({"student_id": student_id}).sort("timestamp", 1))
            self.assertEqual(len(stored_rows), 1)
            self.assertEqual(stored_rows[0].get("gate_action"), "OUT")
        finally:
            attendance_collection.delete_many({"student_id": student_id})

    def test_live_face_handler_preserves_manual_duplicate_block_after_runtime_reset(self):
        student_id = f"MANUALLOCK-{uuid.uuid4().hex[:10]}"
        student = {
            "student_id": student_id,
            "name": "Manual Lock Student",
            "parent_contact": "",
            "status": "Active",
        }

        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        attendance_collection.delete_many({"student_id": student_id})
        self.app_module.last_scanned.pop(student_id, None)
        self.app_module.scan_presence_locks.pop(student_id, None)
        original_mode = self.app_module.get_scan_session_mode()

        try:
            first_scan_time = datetime(2026, 3, 25, 7, 30, 0)
            second_scan_time = first_scan_time + timedelta(seconds=61)

            self.app_module.set_scan_session_mode("manual_out")
            with patch.object(self.app_module, "push_scan_event") as push_event_mock:
                with patch.object(self.app_module, "now_local", return_value=first_scan_time), \
                        patch.object(self.app_module.time, "time", return_value=first_scan_time.timestamp()):
                    first_result = self.app_module.handle_verified_student(student, confidence=99.0)
                self.assertIsNotNone(first_result)
                self.assertEqual(first_result["gate_action"], "OUT")
                self.assertEqual(push_event_mock.call_count, 1)

                self.app_module.last_scanned.pop(student_id, None)
                self.app_module.scan_presence_locks.pop(student_id, None)

                with patch.object(self.app_module, "now_local", return_value=second_scan_time), \
                        patch.object(self.app_module.time, "time", return_value=second_scan_time.timestamp()):
                    duplicate_result = self.app_module.handle_verified_student(student, confidence=99.0)
                self.assertIsNone(duplicate_result)
                self.assertEqual(push_event_mock.call_count, 1)

            stored_rows = list(attendance_collection.find({"student_id": student_id}).sort("timestamp", 1))
            self.assertEqual(len(stored_rows), 1)
            self.assertEqual(stored_rows[0].get("gate_action"), "OUT")
        finally:
            self.app_module.set_scan_session_mode(original_mode)
            attendance_collection.delete_many({"student_id": student_id})
            self.app_module.last_scanned.pop(student_id, None)
            self.app_module.scan_presence_locks.pop(student_id, None)

    def test_resolve_live_track_face_quality_reuses_recent_stable_cache(self):
        track = {
            "track_id": 17,
            "face_quality": {
                "brightness": 128.0,
                "contrast": 22.0,
                "sharpness": 18.0,
                "texture": 11.0,
                "highlights": 0.03,
                "area_ratio": 0.08,
            },
            "face_quality_ts": 100.0,
            "liveness_motion_component": 0.01,
            "liveness_area_component": 0.02,
            "liveness_pose_component": 0.01,
        }

        with patch.object(self.app_module, "measure_live_face_quality") as quality_mock, \
                patch.object(self.app_module, "set_live_face_track_liveness_state") as update_mock:
            quality = self.app_module.resolve_live_track_face_quality(
                track,
                frame=None,
                face_location=(0, 10, 10, 0),
                now_ts=100.05,
            )

        self.assertEqual(quality["brightness"], 128.0)
        quality_mock.assert_not_called()
        update_mock.assert_not_called()

    def test_get_cached_live_track_encoding_requires_recent_stable_track(self):
        encoding = self.app_module.np.array([0.11] * 128, dtype=self.app_module.np.float64)
        stable_track = {
            "last_encoding": encoding,
            "last_encoding_ts": 50.0,
            "liveness_motion_component": 0.01,
            "liveness_area_component": 0.02,
            "liveness_pose_component": 0.01,
        }
        moving_track = {
            **stable_track,
            "liveness_motion_component": float(self.app_module.LIVE_RECOGNITION_CACHE_MAX_MOTION_COMPONENT) + 0.02,
        }

        cache_window = max(float(self.app_module.LIVE_RECOGNITION_ENCODING_CACHE_SECONDS or 0.0), 0.05)
        cached = self.app_module.get_cached_live_track_encoding(stable_track, now_ts=50.0 + min(cache_window * 0.5, 0.1))
        stale = self.app_module.get_cached_live_track_encoding(stable_track, now_ts=50.0 + cache_window + 0.05)
        moving = self.app_module.get_cached_live_track_encoding(moving_track, now_ts=50.0 + min(cache_window * 0.5, 0.1))

        self.assertIsNotNone(cached)
        self.assertEqual(cached.shape[0], 128)
        self.assertIsNone(stale)
        self.assertIsNone(moving)

    def test_cache_live_track_encoding_updates_track_and_store(self):
        track = {"track_id": 9}
        encoding = self.app_module.np.array([0.25] * 128, dtype=self.app_module.np.float64)

        with patch.object(self.app_module, "set_live_face_track_liveness_state") as update_mock:
            cached = self.app_module.cache_live_track_encoding(track, encoding, now_ts=44.0)

        self.assertIsNotNone(cached)
        self.assertEqual(track["last_encoding_ts"], 44.0)
        self.assertEqual(track["last_encoding"].shape[0], 128)
        update_mock.assert_called_once()

    def test_load_face_index_uses_current_year_student_ref_link(self):
        student_oid = self.app_module.ObjectId()
        student_row = {
            "_id": student_oid,
            "student_id": "REF-001",
            "name": "Reference Linked Student",
            "parent_contact": "",
            "face_encodings": [[0.12] * 128],
        }

        class FakeStudentsCollection:
            def find(self, query, projection):
                clauses = query.get("$and", [])
                roster_clause = clauses[2] if len(clauses) > 2 else {}
                for clause in roster_clause.get("$or", []):
                    object_ids = clause.get("_id", {}).get("$in", [])
                    if student_oid in object_ids:
                        return [student_row]
                return []

        class FakeEnrollmentCollection:
            def find(self, query, projection):
                return [{
                    "student_ref_id": str(student_oid),
                    "student_id": "",
                    "lrn": "",
                    "status": "Active",
                }]

        with patch.object(self.app_module, "students", FakeStudentsCollection()), \
                patch.object(self.app_module, "get_current_school_year_label", return_value="2025-2026"), \
                patch.object(self.app_module, "get_student_enrollment_collection", return_value=FakeEnrollmentCollection()):
            encodings, known_students = self.app_module.load_face_index_from_db()

        self.assertEqual(len(encodings), 1)
        self.assertEqual(len(known_students), 1)
        self.assertEqual(known_students[0]["student_id"], "REF-001")
        self.assertEqual(known_students[0]["name"], "Reference Linked Student")

    def test_load_face_index_falls_back_when_roster_filter_hides_valid_faces(self):
        student_oid = self.app_module.ObjectId()
        student_row = {
            "_id": student_oid,
            "student_id": "FALLBACK-001",
            "name": "Fallback Student",
            "parent_contact": "",
            "face_encodings": [[0.34] * 128],
        }

        class FakeStudentsCollection:
            def find(self, query, projection):
                clauses = query.get("$and", [])
                if len(clauses) > 2:
                    return []
                return [student_row]

        class FakeEnrollmentCollection:
            def find(self, query, projection):
                return [{
                    "student_ref_id": "",
                    "student_id": "MISMATCH-999",
                    "lrn": "MISMATCH-999",
                    "status": "Active",
                }]

        with patch.object(self.app_module, "students", FakeStudentsCollection()), \
                patch.object(self.app_module, "get_current_school_year_label", return_value="2025-2026"), \
                patch.object(self.app_module, "get_student_enrollment_collection", return_value=FakeEnrollmentCollection()):
            encodings, known_students = self.app_module.load_face_index_from_db()

        self.assertEqual(len(encodings), 1)
        self.assertEqual(len(known_students), 1)
        self.assertEqual(known_students[0]["student_id"], "FALLBACK-001")

    def test_process_client_frame_suppresses_persistent_auto_rescan_until_face_leaves(self):
        student_id = f"PERSIST-{uuid.uuid4().hex[:10]}"
        original_active = None
        original_encodings = None
        original_students = None
        original_model_status = None
        with self.app_module.scan_lock:
            original_active = self.app_module.scan_state.get("active")
            original_encodings = self.app_module.scan_state.get("known_encodings")
            original_students = self.app_module.scan_state.get("known_students")
            original_model_status = self.app_module.scan_state.get("model_status")
            self.app_module.scan_state["active"] = True
            self.app_module.scan_state["known_encodings"] = self.app_module.np.array(
                [[0.11] * 128],
                dtype=self.app_module.np.float64,
            )
            self.app_module.scan_state["known_students"] = [
                {"student_id": student_id, "name": "Persistent Student"}
            ]
            self.app_module.scan_state["model_status"] = "ready"

        self.app_module.last_scanned.pop(student_id, None)
        self.app_module.scan_presence_locks.pop(student_id, None)

        frame = self.app_module.np.zeros((240, 320, 3), dtype=self.app_module.np.uint8)
        face_location = [(0, 32, 32, 0)]
        encoding = [self.app_module.np.array([0.11] * 128, dtype=self.app_module.np.float64)]
        start_ts = 1_700_000_000.0

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(self.app_module.cv2, "imdecode", return_value=frame))
                stack.enter_context(patch.object(self.app_module.cv2, "cvtColor", return_value=frame))
                stack.enter_context(patch.object(self.app_module.face_recognition, "face_locations", return_value=face_location))
                stack.enter_context(patch.object(self.app_module.face_recognition, "face_encodings", return_value=encoding))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_distance",
                    return_value=self.app_module.np.array([0.21], dtype=self.app_module.np.float64),
                ))
                stack.enter_context(patch.object(self.app_module, "calculate_match_confidence", return_value=99.1))
                live_mocks = stack.enter_context(patch.multiple(
                    self.app_module,
                    sample_live_landmark_liveness=DEFAULT,
                    evaluate_live_blink_liveness=DEFAULT,
                    evaluate_live_landmark_pose_liveness=DEFAULT,
                    evaluate_live_patch_parallax_liveness=DEFAULT,
                    evaluate_live_track_liveness=DEFAULT,
                    measure_live_face_quality=DEFAULT,
                    evaluate_live_texture_liveness=DEFAULT,
                    evaluate_live_display_liveness=DEFAULT,
                    should_suppress_recent_live_scan=DEFAULT,
                ))
                self.app_module.sample_live_landmark_liveness.return_value = {"signature": {"ear": 0.28}, "updates": {}}
                self.app_module.evaluate_live_blink_liveness.return_value = {"accepted": True, "reason": "blink_ok", "message": "", "updates": {}, "blink_detected": False}
                self.app_module.evaluate_live_landmark_pose_liveness.return_value = {"accepted": True, "reason": "landmark_pose_ok", "message": ""}
                self.app_module.evaluate_live_patch_parallax_liveness.return_value = {"accepted": True, "reason": "patch_parallax_ok", "message": "", "updates": {}}
                self.app_module.evaluate_live_track_liveness.return_value = {"accepted": True, "reason": "liveness_ok", "message": ""}
                self.app_module.measure_live_face_quality.return_value = {"brightness": 120.0, "contrast": 24.0, "sharpness": 18.0, "texture": 12.0, "highlights": 0.03, "area_ratio": 0.06}
                self.app_module.evaluate_live_texture_liveness.return_value = {"accepted": True, "reason": "texture_ok", "message": ""}
                self.app_module.evaluate_live_display_liveness.return_value = {"accepted": True, "reason": "display_ok", "message": "", "updates": {}}
                self.app_module.should_suppress_recent_live_scan.side_effect = [False, True, False]
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_TRACK_STABILITY_FRAMES", 1))
                verified_mock = stack.enter_context(patch.object(
                    self.app_module,
                    "handle_verified_student",
                    wraps=self.app_module.handle_verified_student,
                ))
                stack.enter_context(patch.object(
                    self.app_module,
                    "log_attendance_and_sms",
                    return_value={
                        "student_id": student_id,
                        "student_name": "Persistent Student",
                        "status": "Present",
                        "gate_action": "OUT",
                        "verification_label": "Thank You",
                        "display_message": "Thank You",
                        "voice_message": "Thank you",
                        "timestamp": "2026-03-27T08:00:00",
                        "time": "08:00:00",
                        "feed_update": True,
                        "activity_entry": {"student_id": student_id, "name": "Persistent Student", "gate_action": "OUT"},
                        "tracking_mode": "auto",
                        "duplicate": False,
                        "duplicate_reason": "",
                        "sms_status": "skipped",
                    },
                ))
                push_event_mock = stack.enter_context(patch.object(self.app_module, "push_scan_event"))
                stack.enter_context(patch.object(self.app_module.time, "time", side_effect=[
                    start_ts,
                    start_ts,
                    start_ts + 0.4,
                    start_ts + 0.4,
                    start_ts + self.app_module.SCAN_FACE_PRESENCE_RESET_SECONDS + 0.6,
                    start_ts + self.app_module.SCAN_FACE_PRESENCE_RESET_SECONDS + 0.6,
                ]))
                first_success, _, _ = self.app_module.process_client_frame(b"frame")
                second_success, second_message, _ = self.app_module.process_client_frame(b"frame")
                third_success, _, _ = self.app_module.process_client_frame(b"frame")

            self.assertTrue(first_success)
            self.assertTrue(second_success)
            self.assertTrue(third_success)
            self.assertTrue(
                ("Duplicate scan" in second_message)
                or ("Face detected" in second_message)
                or ("Verified" in second_message)
            )
            self.assertGreaterEqual(verified_mock.call_count, 1)
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_encodings
                self.app_module.scan_state["known_students"] = original_students
                self.app_module.scan_state["model_status"] = original_model_status
            self.app_module.last_scanned.pop(student_id, None)
            self.app_module.scan_presence_locks.pop(student_id, None)

    def test_process_client_frame_verifies_multiple_students_without_multi_face_warning(self):
        original_active = None
        original_encodings = None
        original_students = None
        original_model_status = None
        with self.app_module.scan_lock:
            original_active = self.app_module.scan_state.get("active")
            original_encodings = self.app_module.scan_state.get("known_encodings")
            original_students = self.app_module.scan_state.get("known_students")
            original_model_status = self.app_module.scan_state.get("model_status")
            self.app_module.scan_state["active"] = True
            self.app_module.scan_state["known_encodings"] = self.app_module.np.array(
                [
                    [0.11] * 128,
                    [0.77] * 128,
                ],
                dtype=self.app_module.np.float64,
            )
            self.app_module.scan_state["known_students"] = [
                {"student_id": "MF-001", "name": "Multi Face One"},
                {"student_id": "MF-002", "name": "Multi Face Two"},
            ]
            self.app_module.scan_state["model_status"] = "ready"

        frame = self.app_module.np.zeros((240, 320, 3), dtype=self.app_module.np.uint8)
        encodings = [
            self.app_module.np.array([0.10] * 128, dtype=self.app_module.np.float64),
            self.app_module.np.array([0.78] * 128, dtype=self.app_module.np.float64),
        ]

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(self.app_module.cv2, "imdecode", return_value=frame))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_locations",
                    return_value=[(10, 110, 110, 10), (40, 250, 160, 140)],
                ))
                stack.enter_context(patch.object(self.app_module.face_recognition, "face_encodings", return_value=encodings))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_distance",
                    side_effect=[
                        self.app_module.np.array([0.21, 0.74], dtype=self.app_module.np.float64),
                        self.app_module.np.array([0.72, 0.19], dtype=self.app_module.np.float64),
                    ],
                ))
                stack.enter_context(patch.object(self.app_module, "calculate_match_confidence", side_effect=[99.2, 98.6]))
                live_mocks = stack.enter_context(patch.multiple(
                    self.app_module,
                    sample_live_landmark_liveness=DEFAULT,
                    evaluate_live_blink_liveness=DEFAULT,
                    evaluate_live_landmark_pose_liveness=DEFAULT,
                    evaluate_live_patch_parallax_liveness=DEFAULT,
                    evaluate_live_track_liveness=DEFAULT,
                    measure_live_face_quality=DEFAULT,
                    evaluate_live_texture_liveness=DEFAULT,
                    evaluate_live_display_liveness=DEFAULT,
                    should_suppress_recent_live_scan=DEFAULT,
                    track_pending_live_recognition=DEFAULT,
                ))
                self.app_module.sample_live_landmark_liveness.return_value = {"signature": {"ear": 0.28}, "updates": {}}
                self.app_module.evaluate_live_blink_liveness.return_value = {"accepted": True, "reason": "blink_ok", "message": "", "updates": {}, "blink_detected": False}
                self.app_module.evaluate_live_landmark_pose_liveness.return_value = {"accepted": True, "reason": "landmark_pose_ok", "message": ""}
                self.app_module.evaluate_live_patch_parallax_liveness.return_value = {"accepted": True, "reason": "patch_parallax_ok", "message": "", "updates": {}}
                self.app_module.evaluate_live_track_liveness.return_value = {"accepted": True, "reason": "liveness_ok", "message": ""}
                self.app_module.measure_live_face_quality.return_value = {"brightness": 122.0, "contrast": 26.0, "sharpness": 20.0, "texture": 12.4, "highlights": 0.02, "area_ratio": 0.07}
                self.app_module.evaluate_live_texture_liveness.return_value = {"accepted": True, "reason": "texture_ok", "message": ""}
                self.app_module.evaluate_live_display_liveness.return_value = {"accepted": True, "reason": "display_ok", "message": "", "updates": {}}
                self.app_module.should_suppress_recent_live_scan.return_value = False
                self.app_module.track_pending_live_recognition.return_value = {"confirmed": True, "observed_frames": 1, "required_frames": 1}
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_TRACK_STABILITY_FRAMES", 1))
                verified_mock = stack.enter_context(patch.object(
                    self.app_module,
                    "handle_verified_student",
                    side_effect=[
                        {"student_id": "MF-001", "gate_action": "IN"},
                        {"student_id": "MF-002", "gate_action": "IN"},
                    ],
                ))
                not_registered_mock = stack.enter_context(patch.object(self.app_module, "push_not_registered_event"))
                multi_face_mock = stack.enter_context(patch.object(self.app_module, "push_multi_face_event"))
                success, message, _ = self.app_module.process_client_frame(b"frame")

            self.assertTrue(success)
            self.assertIn("Verified", message)
            self.assertGreaterEqual(verified_mock.call_count, 1)
            not_registered_mock.assert_not_called()
            multi_face_mock.assert_not_called()
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_encodings
                self.app_module.scan_state["known_students"] = original_students
                self.app_module.scan_state["model_status"] = original_model_status

    def test_process_client_frame_recognizes_cached_multi_face_tracks_in_same_cycle(self):
        original_active = None
        original_encodings = None
        original_students = None
        original_model_status = None
        with self.app_module.scan_lock:
            original_active = self.app_module.scan_state.get("active")
            original_encodings = self.app_module.scan_state.get("known_encodings")
            original_students = self.app_module.scan_state.get("known_students")
            original_model_status = self.app_module.scan_state.get("model_status")
            self.app_module.scan_state["active"] = True
            self.app_module.scan_state["known_encodings"] = self.app_module.np.array(
                [
                    [0.11] * 128,
                    [0.33] * 128,
                    [0.77] * 128,
                ],
                dtype=self.app_module.np.float64,
            )
            self.app_module.scan_state["known_students"] = [
                {"student_id": "CF-001", "name": "Cached Face One"},
                {"student_id": "CF-002", "name": "Cached Face Two"},
                {"student_id": "CF-003", "name": "Cached Face Three"},
            ]
            self.app_module.scan_state["model_status"] = "ready"

        frame = self.app_module.np.zeros((240, 320, 3), dtype=self.app_module.np.uint8)
        cached_ts = self.app_module.time.time()
        cached_tracks = [
            {
                "track_id": 1,
                "stability": 2,
                "next_attempt_ts": 0.0,
                "small_location": (5, 40, 45, 5),
                "full_location": (10, 80, 90, 10),
                "area": 2800.0,
                "student_id": "",
                "last_result": "",
                "last_confidence": 0.0,
                "face_quality": {},
                "face_quality_ts": 0.0,
                "last_encoding": self.app_module.np.array([0.11] * 128, dtype=self.app_module.np.float64),
                "last_encoding_ts": cached_ts,
                "liveness_motion_component": 0.0,
                "liveness_area_component": 0.0,
                "liveness_pose_component": 0.0,
            },
            {
                "track_id": 2,
                "stability": 2,
                "next_attempt_ts": 0.0,
                "small_location": (15, 120, 55, 80),
                "full_location": (30, 240, 110, 160),
                "area": 3000.0,
                "student_id": "",
                "last_result": "",
                "last_confidence": 0.0,
                "face_quality": {},
                "face_quality_ts": 0.0,
                "last_encoding": self.app_module.np.array([0.33] * 128, dtype=self.app_module.np.float64),
                "last_encoding_ts": cached_ts,
                "liveness_motion_component": 0.0,
                "liveness_area_component": 0.0,
                "liveness_pose_component": 0.0,
            },
            {
                "track_id": 3,
                "stability": 2,
                "next_attempt_ts": 0.0,
                "small_location": (20, 220, 65, 170),
                "full_location": (40, 300, 130, 230),
                "area": 3200.0,
                "student_id": "",
                "last_result": "",
                "last_confidence": 0.0,
                "face_quality": {},
                "face_quality_ts": 0.0,
                "last_encoding": self.app_module.np.array([0.77] * 128, dtype=self.app_module.np.float64),
                "last_encoding_ts": cached_ts,
                "liveness_motion_component": 0.0,
                "liveness_area_component": 0.0,
                "liveness_pose_component": 0.0,
            },
        ]

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(self.app_module.cv2, "imdecode", return_value=frame))
                stack.enter_context(patch.object(self.app_module.cv2, "cvtColor", return_value=frame))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_locations",
                    return_value=[(5, 40, 45, 5), (15, 120, 55, 80), (20, 220, 65, 170)],
                ))
                face_encodings_mock = stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_encodings",
                    return_value=[],
                ))
                stack.enter_context(patch.object(self.app_module, "update_live_face_tracks", return_value=cached_tracks))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_distance",
                    side_effect=[
                        self.app_module.np.array([0.21, 0.74, 0.81], dtype=self.app_module.np.float64),
                        self.app_module.np.array([0.75, 0.19, 0.78], dtype=self.app_module.np.float64),
                        self.app_module.np.array([0.79, 0.82, 0.20], dtype=self.app_module.np.float64),
                    ],
                ))
                stack.enter_context(patch.object(self.app_module, "calculate_match_confidence", side_effect=[99.2, 98.8, 98.4]))
                stack.enter_context(patch.object(self.app_module, "measure_live_face_quality", return_value={
                    "brightness": 122.0,
                    "contrast": 26.0,
                    "sharpness": 20.0,
                    "texture": 12.4,
                    "highlights": 0.02,
                    "area_ratio": 0.07,
                }))
                stack.enter_context(patch.object(self.app_module, "should_suppress_recent_live_scan", return_value=False))
                stack.enter_context(patch.object(
                    self.app_module,
                    "track_pending_live_recognition",
                    return_value={"confirmed": True, "observed_frames": 1, "required_frames": 1},
                ))
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_TRACK_STABILITY_FRAMES", 1))
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_MAX_RECOGNITIONS_PER_FRAME", 1))
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_MULTI_FACE_MAX_CANDIDATES", 3))
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_MAX_NEW_ENCODINGS_PER_FRAME", 1))
                verified_mock = stack.enter_context(patch.object(
                    self.app_module,
                    "handle_verified_student",
                    side_effect=[
                        {"student_id": "CF-001", "gate_action": "IN"},
                        {"student_id": "CF-002", "gate_action": "IN"},
                        {"student_id": "CF-003", "gate_action": "IN"},
                    ],
                ))
                success, message, payload = self.app_module.process_client_frame(b"frame")

            self.assertTrue(success)
            self.assertIn("Verified 3 students", message)
            self.assertEqual(len(payload.get("faces") or []), 3)
            self.assertEqual(verified_mock.call_count, 3)
            face_encodings_mock.assert_not_called()
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_encodings
                self.app_module.scan_state["known_students"] = original_students
                self.app_module.scan_state["model_status"] = original_model_status

    def test_process_client_frame_reuses_cached_match_results_for_stable_tracks(self):
        student_id = f"CACHEMATCH-{uuid.uuid4().hex[:10]}"
        student = {"student_id": student_id, "name": "Cached Match Student"}
        with self.app_module.scan_lock:
            original_active = self.app_module.scan_state.get("active")
            original_encodings = self.app_module.scan_state.get("known_encodings")
            original_students = self.app_module.scan_state.get("known_students")
            original_model_status = self.app_module.scan_state.get("model_status")
            original_face_tracks = dict(self.app_module.scan_state.get("face_tracks") or {})
            original_pending = dict(self.app_module.scan_state.get("pending_recognition") or {})
            original_track_id = self.app_module.scan_state.get("next_face_track_id")
            original_cursor = self.app_module.scan_state.get("face_track_cursor")
            original_faces_payload = list(self.app_module.scan_state.get("last_faces_payload") or [])
            self.app_module.scan_state["active"] = True
            self.app_module.scan_state["known_encodings"] = self.app_module.np.array(
                [[0.19] * 128],
                dtype=self.app_module.np.float64,
            )
            self.app_module.scan_state["known_students"] = [student]
            self.app_module.scan_state["model_status"] = "ready"
            self.app_module.scan_state["face_tracks"] = {}
            self.app_module.scan_state["pending_recognition"] = {}
            self.app_module.scan_state["next_face_track_id"] = 1
            self.app_module.scan_state["face_track_cursor"] = 0
            self.app_module.scan_state["last_faces_payload"] = []

        frame = self.app_module.np.zeros((240, 320, 3), dtype=self.app_module.np.uint8)
        cached_ts = self.app_module.time.time()
        cached_tracks = [{
            "track_id": 1,
            "stability": 2,
            "next_attempt_ts": 0.0,
            "small_location": (0, 40, 40, 0),
            "full_location": (0, 80, 80, 0),
            "area": 3200.0,
            "student_id": "",
            "last_result": "",
            "last_confidence": 0.0,
            "face_quality": {},
            "face_quality_ts": 0.0,
            "last_encoding": self.app_module.np.array([0.19] * 128, dtype=self.app_module.np.float64),
            "last_encoding_ts": cached_ts,
            "last_match_result": {
                "recognized": True,
                "student": dict(student),
                "confidence": 99.4,
                "distance": 0.19,
                "candidate": {
                    "student": dict(student),
                    "best_distance": 0.19,
                },
                "reason": "match",
            },
            "last_match_ts": cached_ts,
            "last_match_encoding_ts": cached_ts,
            "liveness_motion_component": 0.0,
            "liveness_area_component": 0.0,
            "liveness_pose_component": 0.0,
        }]

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(self.app_module.cv2, "imdecode", return_value=frame))
                stack.enter_context(patch.object(self.app_module.cv2, "cvtColor", return_value=frame))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_locations",
                    return_value=[(0, 40, 40, 0)],
                ))
                face_encodings_mock = stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_encodings",
                    return_value=[],
                ))
                face_distance_mock = stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_distance",
                    side_effect=AssertionError("face_distance should not run when a stable cached match is available"),
                ))
                stack.enter_context(patch.object(self.app_module, "update_live_face_tracks", return_value=cached_tracks))
                stack.enter_context(patch.object(self.app_module, "measure_live_face_quality", return_value={
                    "brightness": 121.0,
                    "contrast": 24.0,
                    "sharpness": 19.0,
                    "texture": 12.1,
                    "highlights": 0.02,
                    "area_ratio": 0.06,
                }))
                stack.enter_context(patch.object(self.app_module, "should_suppress_recent_live_scan", return_value=False))
                stack.enter_context(patch.object(
                    self.app_module,
                    "track_pending_live_recognition",
                    return_value={"confirmed": True, "observed_frames": 1, "required_frames": 1},
                ))
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_TRACK_STABILITY_FRAMES", 1))
                verified_mock = stack.enter_context(patch.object(
                    self.app_module,
                    "handle_verified_student",
                    return_value={"student_id": student_id, "gate_action": "IN"},
                ))
                success, message, payload = self.app_module.process_client_frame(b"frame")

            self.assertTrue(success)
            self.assertIn("Verified", message)
            self.assertEqual(len(payload.get("faces") or []), 1)
            verified_mock.assert_called_once()
            face_encodings_mock.assert_not_called()
            face_distance_mock.assert_not_called()
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_encodings
                self.app_module.scan_state["known_students"] = original_students
                self.app_module.scan_state["model_status"] = original_model_status
                self.app_module.scan_state["face_tracks"] = original_face_tracks
                self.app_module.scan_state["pending_recognition"] = original_pending
                self.app_module.scan_state["next_face_track_id"] = original_track_id
                self.app_module.scan_state["face_track_cursor"] = original_cursor
                self.app_module.scan_state["last_faces_payload"] = original_faces_payload

    def test_process_client_frame_bypasses_anti_spoofing_checks(self):
        student_id = f"BYPASS-{uuid.uuid4().hex[:10]}"
        with self.app_module.scan_lock:
            original_active = self.app_module.scan_state.get("active")
            original_encodings = self.app_module.scan_state.get("known_encodings")
            original_students = self.app_module.scan_state.get("known_students")
            original_model_status = self.app_module.scan_state.get("model_status")
            original_face_tracks = dict(self.app_module.scan_state.get("face_tracks") or {})
            original_pending = dict(self.app_module.scan_state.get("pending_recognition") or {})
            original_track_id = self.app_module.scan_state.get("next_face_track_id")
            original_cursor = self.app_module.scan_state.get("face_track_cursor")
            original_faces_payload = list(self.app_module.scan_state.get("last_faces_payload") or [])
            self.app_module.scan_state["active"] = True
            self.app_module.scan_state["known_encodings"] = self.app_module.np.array(
                [[0.11] * 128],
                dtype=self.app_module.np.float64,
            )
            self.app_module.scan_state["known_students"] = [
                {"student_id": student_id, "name": "Bypass Student"}
            ]
            self.app_module.scan_state["model_status"] = "ready"
            self.app_module.scan_state["face_tracks"] = {}
            self.app_module.scan_state["pending_recognition"] = {}
            self.app_module.scan_state["next_face_track_id"] = 1
            self.app_module.scan_state["face_track_cursor"] = 0
            self.app_module.scan_state["last_faces_payload"] = []

        frame = self.app_module.np.zeros((240, 320, 3), dtype=self.app_module.np.uint8)
        face_location = [(0, 32, 32, 0)]
        encoding = [self.app_module.np.array([0.11] * 128, dtype=self.app_module.np.float64)]

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(self.app_module.cv2, "imdecode", return_value=frame))
                stack.enter_context(patch.object(self.app_module.cv2, "cvtColor", return_value=frame))
                stack.enter_context(patch.object(self.app_module.face_recognition, "face_locations", return_value=face_location))
                stack.enter_context(patch.object(self.app_module.face_recognition, "face_encodings", return_value=encoding))
                stack.enter_context(patch.object(
                    self.app_module.face_recognition,
                    "face_distance",
                    return_value=self.app_module.np.array([0.21], dtype=self.app_module.np.float64),
                ))
                stack.enter_context(patch.object(self.app_module, "calculate_match_confidence", return_value=99.1))
                live_mocks = stack.enter_context(patch.multiple(
                    self.app_module,
                    sample_live_landmark_liveness=DEFAULT,
                    evaluate_live_blink_liveness=DEFAULT,
                    evaluate_live_landmark_pose_liveness=DEFAULT,
                    evaluate_live_patch_parallax_liveness=DEFAULT,
                    evaluate_live_track_liveness=DEFAULT,
                    evaluate_live_texture_liveness=DEFAULT,
                    evaluate_live_display_liveness=DEFAULT,
                ))
                stack.enter_context(patch.object(self.app_module, "measure_live_face_quality", return_value={
                    "brightness": 120.0,
                    "contrast": 24.0,
                    "sharpness": 18.0,
                    "texture": 12.0,
                    "highlights": 0.03,
                    "area_ratio": 0.06,
                }))
                stack.enter_context(patch.object(self.app_module, "should_suppress_recent_live_scan", return_value=False))
                stack.enter_context(patch.object(
                    self.app_module,
                    "track_pending_live_recognition",
                    return_value={"confirmed": True, "observed_frames": 1, "required_frames": 1},
                ))
                stack.enter_context(patch.object(self.app_module, "LIVE_RECOGNITION_TRACK_STABILITY_FRAMES", 1))
                verified_mock = stack.enter_context(patch.object(
                    self.app_module,
                    "handle_verified_student",
                    return_value={"student_id": student_id, "gate_action": "IN"},
                ))
                success, message, payload = self.app_module.process_client_frame(b"frame")

            self.assertTrue(success)
            self.assertIn("Verified", message)
            self.assertEqual(len(payload.get("faces") or []), 1)
            verified_mock.assert_called_once()
            live_mocks["sample_live_landmark_liveness"].assert_not_called()
            live_mocks["evaluate_live_blink_liveness"].assert_not_called()
            live_mocks["evaluate_live_landmark_pose_liveness"].assert_not_called()
            live_mocks["evaluate_live_patch_parallax_liveness"].assert_not_called()
            live_mocks["evaluate_live_track_liveness"].assert_not_called()
            live_mocks["evaluate_live_texture_liveness"].assert_not_called()
            live_mocks["evaluate_live_display_liveness"].assert_not_called()
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_encodings
                self.app_module.scan_state["known_students"] = original_students
                self.app_module.scan_state["model_status"] = original_model_status
                self.app_module.scan_state["face_tracks"] = original_face_tracks
                self.app_module.scan_state["pending_recognition"] = original_pending
                self.app_module.scan_state["next_face_track_id"] = original_track_id
                self.app_module.scan_state["face_track_cursor"] = original_cursor
                self.app_module.scan_state["last_faces_payload"] = original_faces_payload

    def test_evaluate_live_blink_liveness_marks_pending_without_hard_reject(self):
        gate = self.app_module.evaluate_live_blink_liveness(
            {"liveness_last_blink_ts": 0.0, "liveness_last_ear_ts": 0.0},
            now_ts=10.0,
            landmark_signature={"ear": 0.29},
        )

        self.assertTrue(gate["accepted"])
        self.assertEqual(gate["reason"], "liveness_blink_pending")
        self.assertEqual(gate["message"], "")

    def test_sample_live_landmark_liveness_accumulates_pose_span(self):
        track = {}
        with patch.object(
            self.app_module,
            "extract_live_landmark_signature",
            side_effect=[
                {"ear": 0.29, "yaw": 0.01, "pitch": 0.62},
                {"ear": 0.27, "yaw": 0.14, "pitch": 0.61},
            ],
        ):
            first = self.app_module.sample_live_landmark_liveness(track, now_ts=100.0)
            track.update(first["updates"])
            second = self.app_module.sample_live_landmark_liveness(track, now_ts=100.25)

        self.assertGreaterEqual(int(second["updates"]["liveness_landmark_pose_samples"]), 2)
        self.assertGreater(float(second["updates"]["liveness_landmark_pose_span"]), 0.1)

    def test_sample_live_landmark_liveness_uses_faster_interval_for_new_tracks(self):
        with patch.object(
            self.app_module,
            "extract_live_landmark_signature",
            return_value={"ear": 0.28, "yaw": 0.08, "pitch": 0.61},
        ):
            sample = self.app_module.sample_live_landmark_liveness(
                {
                    "first_seen_ts": 100.0,
                    "liveness_last_landmark_ts": 100.05,
                    "liveness_landmark_pose_samples": 0,
                    "liveness_last_blink_ts": 0.0,
                },
                rgb_small=self.app_module.np.zeros((120, 120, 3), dtype=self.app_module.np.uint8),
                face_location=(10, 80, 90, 20),
                now_ts=100.14,
            )

        self.assertIsInstance(sample["signature"], dict)
        self.assertGreaterEqual(float(sample["updates"]["liveness_last_landmark_ts"]), 100.14)

    def test_evaluate_live_landmark_pose_liveness_requires_pose_span(self):
        liveness_config = self.app_module.resolve_live_liveness_profile_thresholds()
        min_span = float(liveness_config.get("min_landmark_pose_span") or 0.0)
        pose_window = float(liveness_config.get("pose_window_seconds") or 1.0)
        now_ts = 200.0

        blocked = self.app_module.evaluate_live_landmark_pose_liveness(
            {
                "liveness_last_landmark_ts": now_ts,
                "liveness_landmark_pose_span": max(min_span - 0.02, 0.0),
                "liveness_landmark_pose_samples": int(liveness_config.get("min_landmark_pose_samples") or 2),
            },
            now_ts=now_ts + min(pose_window * 0.25, 0.4),
        )
        allowed = self.app_module.evaluate_live_landmark_pose_liveness(
            {
                "liveness_last_landmark_ts": now_ts,
                "liveness_landmark_pose_span": min_span + 0.03,
                "liveness_landmark_pose_samples": int(liveness_config.get("min_landmark_pose_samples") or 2),
            },
            now_ts=now_ts + min(pose_window * 0.25, 0.4),
        )

        self.assertFalse(blocked["accepted"])
        self.assertTrue(allowed["accepted"])

    def test_evaluate_live_landmark_pose_liveness_allows_fast_strong_track(self):
        liveness_config = self.app_module.resolve_live_liveness_profile_thresholds()
        min_motion = float(liveness_config.get("min_motion_score") or 0.0)
        min_pose = float(liveness_config.get("min_pose_score") or 0.0)
        min_span = float(liveness_config.get("min_landmark_pose_span") or 0.0)
        min_samples = int(liveness_config.get("min_landmark_pose_samples") or 2)
        min_parallax = float(liveness_config.get("min_parallax_score") or 0.0)
        min_patch_diversity = float(liveness_config.get("min_patch_diversity") or 0.0)
        now_ts = 300.9

        gate = self.app_module.evaluate_live_landmark_pose_liveness(
            {
                "first_seen_ts": 300.0,
                "liveness_frames": int(liveness_config.get("min_track_frames") or 3),
                "liveness_motion_score": min_motion + 0.08,
                "liveness_pose_score": min_pose + 0.02,
                "liveness_parallax_score": min_parallax + 0.02,
                "liveness_patch_diversity": min_patch_diversity + 0.05,
                "liveness_last_landmark_ts": now_ts,
                "liveness_landmark_pose_span": max(min_span - 0.008, 0.03),
                "liveness_landmark_pose_samples": max(min_samples - 1, 1),
                "liveness_last_blink_ts": 0.0,
            },
            now_ts=now_ts,
        )

        self.assertTrue(gate["accepted"])
        self.assertIn(gate["reason"], {"landmark_pose_building", "landmark_pose_compensated"})

    def test_evaluate_live_track_liveness_allows_strong_non_blink_fallback(self):
        liveness_config = self.app_module.resolve_live_liveness_profile_thresholds()
        gate = self.app_module.evaluate_live_track_liveness(
            {
                "first_seen_ts": 100.0,
                "liveness_frames": int(liveness_config.get("min_track_frames") or 3) + 2,
                "liveness_motion_score": float(liveness_config.get("min_motion_score") or 0.14) + 0.08,
                "liveness_pose_score": float(liveness_config.get("min_pose_score") or 0.015) + 0.03,
                "liveness_planar_streak": 1,
                "liveness_parallax_score": float(liveness_config.get("min_parallax_score") or 0.03) + 0.025,
                "liveness_patch_diversity": float(liveness_config.get("min_patch_diversity") or 0.09) + 0.05,
                "liveness_landmark_pose_span": float(liveness_config.get("min_landmark_pose_span") or 0.055) + 0.02,
                "liveness_landmark_pose_samples": int(liveness_config.get("min_landmark_pose_samples") or 2),
                "liveness_last_blink_ts": 0.0,
            },
            now_ts=102.8,
        )

        self.assertTrue(gate["accepted"])
        self.assertEqual(gate["reason"], "liveness_strong_non_blink_ok")

    def test_evaluate_live_track_liveness_allows_fast_non_blink_path(self):
        liveness_config = self.app_module.resolve_live_liveness_profile_thresholds()
        gate = self.app_module.evaluate_live_track_liveness(
            {
                "first_seen_ts": 400.0,
                "liveness_frames": int(liveness_config.get("min_track_frames") or 3),
                "liveness_motion_score": float(liveness_config.get("min_motion_score") or 0.14) + 0.08,
                "liveness_pose_score": float(liveness_config.get("min_pose_score") or 0.015) + 0.018,
                "liveness_planar_streak": 1,
                "liveness_parallax_score": float(liveness_config.get("min_parallax_score") or 0.03) + 0.02,
                "liveness_patch_diversity": float(liveness_config.get("min_patch_diversity") or 0.09) + 0.05,
                "liveness_landmark_pose_span": max(
                    float(liveness_config.get("min_landmark_pose_span") or 0.05) - 0.008,
                    0.03,
                ),
                "liveness_landmark_pose_samples": max(
                    int(liveness_config.get("min_landmark_pose_samples") or 2) - 1,
                    1,
                ),
                "liveness_last_blink_ts": 0.0,
            },
            now_ts=400.9,
        )

        self.assertTrue(gate["accepted"])
        self.assertEqual(gate["reason"], "liveness_fast_non_blink_ok")

    def test_evaluate_live_track_liveness_still_blocks_weak_non_blink_tracks(self):
        liveness_config = self.app_module.resolve_live_liveness_profile_thresholds()
        gate = self.app_module.evaluate_live_track_liveness(
            {
                "first_seen_ts": 100.0,
                "liveness_frames": int(liveness_config.get("min_track_frames") or 3) + 1,
                "liveness_motion_score": float(liveness_config.get("min_motion_score") or 0.14) + 0.01,
                "liveness_pose_score": float(liveness_config.get("min_pose_score") or 0.015) + 0.003,
                "liveness_planar_streak": int(liveness_config.get("planar_streak_frames") or 5),
                "liveness_parallax_score": max(float(liveness_config.get("min_parallax_score") or 0.03) - 0.01, 0.0),
                "liveness_patch_diversity": max(float(liveness_config.get("min_patch_diversity") or 0.09) - 0.03, 0.0),
                "liveness_landmark_pose_span": max(float(liveness_config.get("min_landmark_pose_span") or 0.055) - 0.02, 0.0),
                "liveness_landmark_pose_samples": max(int(liveness_config.get("min_landmark_pose_samples") or 2) - 1, 1),
                "liveness_last_blink_ts": 0.0,
            },
            now_ts=102.0,
        )

        self.assertFalse(gate["accepted"])
        self.assertTrue(
            ("blink" in gate["message"].lower())
            or ("verification failed" in gate["message"].lower())
        )

    def test_evaluate_live_display_liveness_rejects_screen_like_border(self):
        frame = self.app_module.np.zeros((260, 360, 3), dtype=self.app_module.np.uint8)
        self.app_module.cv2.rectangle(frame, (78, 36), (286, 224), (255, 255, 255), 4)
        self.app_module.cv2.rectangle(frame, (116, 78), (242, 204), (160, 160, 160), -1)

        gate = self.app_module.evaluate_live_display_liveness(
            {"liveness_display_streak": 2},
            frame,
            (84, 236, 198, 122),
            face_quality={
                "brightness": 148.0,
                "contrast": 9.0,
                "sharpness": 6.5,
                "texture": 4.0,
                "highlights": 0.12,
                "area_ratio": 0.08,
            },
            scale_back=1.0,
        )

        self.assertFalse(gate["accepted"])
        self.assertIn("screen", gate["message"].lower())

    def test_dynamic_schedule_changes_runtime_late_and_cooldown_behavior(self):
        client = self.make_client()
        original_doc = self.get_default_schedule_doc()
        student_id = f"CFG-{uuid.uuid4().hex[:10]}"
        student_doc = {
            "student_id": student_id,
            "name": "Configured Student",
            "parent_contact": "",
            "status": "Active",
        }
        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}

        self.app_module.students.delete_many({"student_id": student_id})
        attendance_collection.delete_many({"student_id": student_id})
        self.app_module.students.insert_one(student_doc)
        self.app_module.system_settings.update_one(
            {"key": "default_schedule"},
            {"$set": {"schedule": {
                "morning_start": "05:00",
                "noon_start": "12:00",
                "afternoon_start": "13:00",
                "afternoon_end": "17:00",
                "late_threshold_minutes": 15,
                "scan_cooldown_minutes": 45,
            }}},
            upsert=True,
        )

        try:
            first_scan_time = datetime(2026, 3, 25, 5, 5, 0)
            with patch.object(
                self.app_module,
                "get_active_schedule",
                return_value={"type": "regular", "special_condition": "", "schedule": self.app_module.get_default_schedule()},
            ):
                status_hint = self.app_module.session_info_for_time(datetime(2026, 3, 25, 5, 16, 0))
                self.assertEqual(status_hint["status"], "Late")

                with patch.object(self.app_module, "now_local", return_value=first_scan_time):
                    first_response = client.post(f"/simulate-gate/{student_id}", headers=csrf_headers)
                self.assertEqual(first_response.status_code, 200)
                self.assertFalse(first_response.get_json()["duplicate"])

                with patch.object(self.app_module, "now_local", return_value=first_scan_time + timedelta(minutes=30)):
                    blocked_response = client.post(f"/simulate-gate/{student_id}", headers=csrf_headers)
                self.assertEqual(blocked_response.status_code, 200)
                self.assertTrue(blocked_response.get_json()["duplicate"])

                with patch.object(self.app_module, "now_local", return_value=first_scan_time + timedelta(minutes=46)):
                    out_response = client.post(f"/simulate-gate/{student_id}", headers=csrf_headers)
                self.assertEqual(out_response.status_code, 200)
                self.assertFalse(out_response.get_json()["duplicate"])
                self.assertEqual(out_response.get_json()["action"], "OUT")
        finally:
            attendance_collection.delete_many({"student_id": student_id})
            self.app_module.students.delete_many({"student_id": student_id})
            self.restore_default_schedule_doc(original_doc)

    def test_start_scan_capture_resets_stale_events_for_new_session(self):
        with self.app_module.scan_lock:
            self.app_module.scan_state["active"] = False
            self.app_module.scan_state["events"] = [{"id": 42, "type": "verified"}]
            self.app_module.scan_state["event_counter"] = 42

        try:
            with patch.object(self.app_module, "refresh_face_index_async"):
                ok, _message = self.app_module.start_scan_capture()
            self.assertTrue(ok)
            with self.app_module.scan_lock:
                self.assertEqual(self.app_module.scan_state["events"], [])
                self.assertEqual(self.app_module.scan_state["event_counter"], 0)
        finally:
            self.app_module.stop_scan_capture()

    def test_dashboard_and_log_pages_render_12_hour_time_labels(self):
        client = self.make_client()
        marker = f"TIMEFMT-{uuid.uuid4().hex[:8]}"
        school_year_label = self.app_module.get_current_school_year_label()
        attendance_collection, _, _ = self.app_module.get_attendance_logs_storage(school_year_label)
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)

        gate_doc = {
            "student_id": marker,
            "student_name": "Time Format Student",
            "school_year": school_year_label,
            "status": "Present",
            "session": "Live IN 11:53 PM",
            "source": "test",
            "timestamp": "2026-03-26T23:53:00",
            "date": "2026-03-26",
            "time": "23:53:00",
            "gate_action": "IN",
            "verification_label": "Welcome",
            "tracking_mode": "auto",
        }
        sms_doc = {
            "student_id": marker,
            "name": "Time Format Student",
            "parent_contact": "09123456789",
            "message": "Test message",
            "status": "sent",
            "sid": "TEST-SID",
            "error": "",
            "date": "2026-03-26",
            "time": "23:53:00",
            "timestamp": "2026-03-26T23:53:00",
            "school_year": school_year_label,
        }

        attendance_collection.insert_one(gate_doc)
        sms_collection.insert_one(sms_doc)
        try:
            dashboard_html = client.get(f"/dashboard?q={marker}").get_data(as_text=True)
            gate_logs_html = client.get(f"/gate-logs?q={marker}").get_data(as_text=True)
            sms_logs_html = client.get(f"/sms-logs?q={marker}").get_data(as_text=True)

            self.assertIn("11:53 PM", dashboard_html)
            self.assertIn("11:53 PM", gate_logs_html)
            self.assertIn("11:53:00 PM", sms_logs_html)
        finally:
            attendance_collection.delete_many({"student_id": marker})
            sms_collection.delete_many({"student_id": marker})

    def test_process_pending_sms_retries_updates_due_failed_log_in_place(self):
        school_year_label = self.app_module.get_current_school_year_label()
        sms_collection, _, _ = self.app_module.get_sms_logs_storage(school_year_label)
        student_id = f"SMSRETRY-{uuid.uuid4().hex[:8]}"
        log_id = sms_collection.insert_one({
            "to": "+639171234567",
            "message": "Retry this message",
            "type": "transactional",
            "status": "failed",
            "provider": "PHILSMS",
            "providerMessageId": "",
            "providerResponse": {"phase": "provider_send", "meta": {"context": "attendance_gate_scan"}},
            "error": "Temporary gateway failure",
            "httpStatus": 503,
            "errorCode": "PROVIDER_ERROR",
            "errorMessage": "Temporary gateway failure",
            "createdAt": "2026-04-10T08:00:00",
            "updatedAt": "2026-04-10T08:00:00",
            "school_year": school_year_label,
            "student_id": student_id,
            "name": "Retry Student",
            "parent_contact": "09171234567",
            "parent_contact_raw": "09171234567",
            "retryEligible": True,
            "retryCount": 0,
            "retryMaxAttempts": 3,
            "nextRetryAt": "2026-04-10T08:00:00",
            "lastRetryError": "Temporary gateway failure",
            "sid": "",
            "timestamp": "2026-04-10T08:00:00",
            "date": "2026-04-10",
            "time": "08:00:00",
        }).inserted_id

        try:
            original_count = sms_collection.count_documents({"student_id": student_id})
            with patch.object(self.app_module, "send_sms", return_value={
                "status": "sent",
                "sid": "RETRY-SID-001",
                "provider_message_id": "RETRY-SID-001",
                "provider_response": {"status": "success", "message_id": "RETRY-SID-001"},
                "error": "",
                "http_status": 200,
                "error_code": "",
                "error_message": "",
                "to": "+639171234567",
                "log_id": "",
            }):
                result = self.app_module.process_pending_sms_retries(max_logs=5)

            self.assertEqual(result["processed"], 1)
            self.assertEqual(result["sent"], 1)
            self.assertEqual(sms_collection.count_documents({"student_id": student_id}), original_count)

            saved = sms_collection.find_one({"_id": log_id})
            self.assertEqual(saved.get("status"), "sent")
            self.assertEqual(saved.get("providerMessageId"), "RETRY-SID-001")
            self.assertFalse(saved.get("retryEligible"))
            self.assertEqual(saved.get("retryCount"), 1)
            self.assertIsNone(saved.get("nextRetryAt"))
            self.assertIsNone(saved.get("lastRetryError"))
        finally:
            sms_collection.delete_many({"student_id": student_id})

    def test_face_registration_requires_ten_guided_captures_for_standard_mode(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_doc = {
            "student_id": f"FACE-{uuid.uuid4().hex[:8]}",
            "name": "Face Validation Student",
            "parent_contact": "",
            "status": "Active",
        }
        inserted_id = self.app_module.students.insert_one(student_doc).inserted_id

        try:
            payload = {
                "capture_profile": "standard",
                "faces": [f"data:image/jpeg;base64,shot-{index}" for index in range(9)],
            }
            response = client.post(
                f"/api/students/{inserted_id}/face/register",
                json=payload,
                headers=csrf_headers,
            )
            self.assertEqual(response.status_code, 400)
            body = response.get_json()
            self.assertIn("10", body["message"])
        finally:
            self.app_module.students.delete_many({"_id": inserted_id})

    def test_face_registration_reports_invalid_capture_indexes_for_partial_retake(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_doc = {
            "student_id": f"FACE-{uuid.uuid4().hex[:8]}",
            "name": "Face Retake Student",
            "parent_contact": "",
            "status": "Active",
        }
        inserted_id = self.app_module.students.insert_one(student_doc).inserted_id
        validated_rows = [
            {
                "encoding": self.app_module.np.array([index * 0.01] * 128, dtype=self.app_module.np.float64),
                "brightness": 120.0,
                "contrast": 40.0,
                "sharpness": 80.0,
            }
            for index in range(10)
        ]
        validated_rows[3] = None
        payload = {
            "capture_profile": "standard",
            "faces": [f"data:image/jpeg;base64,shot-{index}" for index in range(10)],
            "capture_meta": [
                {"step_key": f"step_{index}", "label": f"Capture {index + 1}", "instruction": "Hold still."}
                for index in range(10)
            ],
        }

        try:
            with patch.object(self.app_module, "validate_face_capture_image", side_effect=validated_rows):
                response = client.post(
                    f"/api/students/{inserted_id}/face/register",
                    json=payload,
                    headers=csrf_headers,
                )

            self.assertEqual(response.status_code, 400)
            body = response.get_json()
            self.assertEqual(body["field"], "faces")
            self.assertEqual(body["required_count"], 10)
            self.assertEqual(body["valid_capture_count"], 9)
            self.assertEqual(body["invalid_capture_indices"], [3])
            self.assertEqual(body["invalid_captures"][0]["reason"], "validation_failed")
            self.assertEqual(body["invalid_captures"][0]["label"], "Capture 4")
        finally:
            self.app_module.students.delete_many({"_id": inserted_id})

    def test_face_registration_accepts_twenty_capture_similar_faces_mode(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_doc = {
            "student_id": f"FACE-{uuid.uuid4().hex[:8]}",
            "name": "Similar Faces Student",
            "parent_contact": "",
            "status": "Active",
        }
        inserted_id = self.app_module.students.insert_one(student_doc).inserted_id
        validated_rows = [
            {
                "encoding": self.app_module.np.array([index * 0.01] * 128, dtype=self.app_module.np.float64),
                "brightness": 120.0,
                "contrast": 40.0,
                "sharpness": 80.0,
            }
            for index in range(20)
        ]
        payload = {
            "capture_profile": "similar_faces",
            "faces": [f"data:image/jpeg;base64,shot-{index}" for index in range(20)],
            "capture_meta": [
                {"step_key": f"step_{index}", "label": f"Capture {index + 1}", "instruction": "Hold still."}
                for index in range(20)
            ],
        }

        try:
            with patch.object(self.app_module, "validate_face_capture_image", side_effect=validated_rows), \
                    patch.object(self.app_module, "refresh_scan_face_index_if_active"), \
                    patch.object(self.app_module, "sync_student_base_fields_to_enrollments"):
                response = client.post(
                    f"/api/students/{inserted_id}/face/register",
                    json=payload,
                    headers=csrf_headers,
                )

            self.assertEqual(response.status_code, 200)
            body = response.get_json()
            self.assertEqual(body["status"], "ok")

            saved = self.app_module.students.find_one({"_id": inserted_id})
            self.assertTrue(saved.get("face_registered"))
            self.assertEqual(saved.get("face_capture_profile"), "similar_faces")
            self.assertEqual(saved.get("face_capture_count"), 20)
            self.assertEqual(len(saved.get("face_encodings", [])), 20)
            self.assertEqual(len(saved.get("face_capture_meta", [])), 20)
        finally:
            self.app_module.students.delete_many({"_id": inserted_id})

    def test_face_registration_rejects_repetitive_guided_pose_sequence(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_doc = {
            "student_id": f"FACE-{uuid.uuid4().hex[:8]}",
            "name": "Pose Diversity Student",
            "parent_contact": "",
            "status": "Active",
        }
        inserted_id = self.app_module.students.insert_one(student_doc).inserted_id
        validated_rows = [
            {
                "ok": True,
                "encoding": self.app_module.np.array([0.15 + (index * 0.004)] * 128, dtype=self.app_module.np.float64),
                "brightness": 120.0,
                "contrast": 40.0,
                "sharpness": 80.0,
            }
            for index in range(10)
        ]
        payload = {
            "capture_profile": "standard",
            "faces": [f"data:image/jpeg;base64,shot-{index}" for index in range(10)],
            "capture_meta": [
                {
                    "step_key": f"step_{index}",
                    "label": f"Capture {index + 1}",
                    "instruction": "Hold still.",
                    "yaw": 0.01,
                    "pitch": 0.01,
                }
                for index in range(10)
            ],
        }

        try:
            with patch.object(self.app_module, "validate_face_capture_image", side_effect=validated_rows):
                response = client.post(
                    f"/api/students/{inserted_id}/face/register",
                    json=payload,
                    headers=csrf_headers,
                )

            self.assertEqual(response.status_code, 400)
            body = response.get_json()
            self.assertEqual(body["field"], "faces")
            self.assertIn("face-angle variety", body["message"])
            self.assertEqual(body["pose_bucket_count"], 1)
            self.assertEqual(body["pose_sample_count"], 10)
        finally:
            self.app_module.students.delete_many({"_id": inserted_id})

    def test_encode_live_face_locations_retries_per_face_after_incomplete_batch(self):
        frame = self.app_module.np.full((96, 96, 3), 32, dtype=self.app_module.np.uint8)
        locations = [(10, 40, 40, 10), (20, 72, 52, 42)]
        first_encoding = self.app_module.np.array([0.11] * 128, dtype=self.app_module.np.float64)
        second_encoding = self.app_module.np.array([0.27] * 128, dtype=self.app_module.np.float64)

        def fake_face_encodings(_image, known_face_locations=None, num_jitters=1, model="small"):
            known_face_locations = list(known_face_locations or [])
            if len(known_face_locations) == 2:
                return [first_encoding]
            if known_face_locations and tuple(known_face_locations[0]) == locations[0]:
                return [first_encoding]
            if known_face_locations and tuple(known_face_locations[0]) == locations[1]:
                return [second_encoding]
            return []

        with patch.object(self.app_module.face_recognition, "face_encodings", side_effect=fake_face_encodings):
            encoded_rows = self.app_module.encode_live_face_locations(
                frame,
                locations,
                num_jitters=1,
                low_light_hint=True,
            )

        self.assertEqual(len(encoded_rows), 2)
        self.assertTrue(self.app_module.np.allclose(encoded_rows[0], first_encoding))
        self.assertTrue(self.app_module.np.allclose(encoded_rows[1], second_encoding))

    def test_face_registration_ignores_orphan_face_profiles_without_current_enrollment(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        current_school_year = self.app_module.get_current_school_year_label()
        target_student = {
            "student_id": f"FACE-{uuid.uuid4().hex[:8]}",
            "name": "Current Enrolled Student",
            "parent_contact": "",
            "status": "Active",
        }
        orphan_student = {
            "student_id": f"ORPHAN-{uuid.uuid4().hex[:8]}",
            "name": "Orphan Face Student",
            "parent_contact": "",
            "status": "Active",
            "face_registered": True,
            "face_data": ["data:image/jpeg;base64,orphan-shot"],
            "face_encodings": [[0.22] * 128, [0.225] * 128, [0.23] * 128],
            "face_embeddings": [[0.22] * 128, [0.225] * 128, [0.23] * 128],
            "profile_photo": "data:image/jpeg;base64,orphan-shot",
        }
        target_inserted_id = self.app_module.students.insert_one(target_student).inserted_id
        orphan_inserted_id = self.app_module.students.insert_one(orphan_student).inserted_id
        stored_target = self.app_module.students.find_one({"_id": target_inserted_id})
        self.app_module.upsert_student_enrollment(
            stored_target,
            current_school_year,
            grade_level="Grade 12",
            section="BSINT",
            status="Active",
            update_existing=True,
        )

        payload = {
            "capture_profile": "standard",
            "faces": [f"data:image/jpeg;base64,shot-{index}" for index in range(10)],
            "capture_meta": [
                {"step_key": f"step_{index}", "label": f"Capture {index + 1}", "instruction": "Hold still."}
                for index in range(10)
            ],
        }
        validated_rows = [
            {
                "encoding": self.app_module.np.array([0.22 + (index * 0.003)] * 128, dtype=self.app_module.np.float64),
                "brightness": 120.0,
                "contrast": 40.0,
                "sharpness": 80.0,
            }
            for index in range(10)
        ]

        try:
            with patch.object(self.app_module, "validate_face_capture_image", side_effect=validated_rows), \
                    patch.object(self.app_module, "refresh_scan_face_index_if_active"), \
                    patch.object(self.app_module, "sync_student_base_fields_to_enrollments"):
                response = client.post(
                    f"/api/students/{target_inserted_id}/face/register",
                    json=payload,
                    headers=csrf_headers,
                )

            self.assertEqual(response.status_code, 200)
            body = response.get_json()
            self.assertEqual(body["status"], "ok")
            saved = self.app_module.students.find_one({"_id": target_inserted_id})
            self.assertTrue(saved.get("face_registered"))
            self.assertEqual(len(saved.get("face_encodings", [])), 10)
        finally:
            self.app_module.students.delete_many({"_id": target_inserted_id})
            self.app_module.students.delete_many({"_id": orphan_inserted_id})
            self.app_module.get_school_year_enrollment_collection(current_school_year).delete_many({"student_id": target_student["student_id"]})

    def test_delete_student_api_removes_profile_face_data_and_runtime_references(self):
        client = self.make_client()
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}
        student_id = f"DEL-{uuid.uuid4().hex[:8]}"
        current_school_year = self.app_module.get_current_school_year_label()
        other_school_year = "2098-2099"
        student_doc = {
            "student_id": student_id,
            "name": "Delete Face Student",
            "parent_contact": "",
            "status": "Active",
            "face_registered": True,
            "face_data": ["data:image/jpeg;base64,profile-shot"],
            "face_encodings": [[0.12] * 128],
            "face_encoding_count": 1,
            "profile_photo": "data:image/jpeg;base64,profile-shot",
        }
        inserted_id = self.app_module.students.insert_one(student_doc).inserted_id
        stored_student = self.app_module.students.find_one({"_id": inserted_id})
        current_enrollment = self.app_module.upsert_student_enrollment(
            stored_student,
            current_school_year,
            grade_level="Grade 7",
            section="AVILA",
            status="Active",
            update_existing=True,
        )
        self.app_module.upsert_student_enrollment(
            stored_student,
            other_school_year,
            grade_level="Grade 8",
            section="ELNAR",
            status="Active",
            update_existing=True,
        )
        self.app_module.attendance_logs.insert_one({
            "student_id": student_id,
            "student_name": "Delete Face Student",
            "school_year": current_school_year,
            "timestamp": "2026-03-28T08:00:00",
            "date": "2026-03-28",
            "time": "08:00:00",
            "gate_action": "IN",
            "status": "Present",
        })
        self.app_module.sms_logs.insert_one({
            "student_id": student_id,
            "name": "Delete Face Student",
            "school_year": current_school_year,
            "timestamp": "2026-03-28T08:00:00",
            "date": "2026-03-28",
            "time": "08:00:00",
            "status": "sent",
            "message": "Test",
        })
        self.app_module.failed_scans.insert_one({
            "student_id": student_id,
            "reason": "unknown_face",
            "date": "2026-03-28",
            "timestamp": "2026-03-28T08:05:00",
        })

        with self.app_module.scan_lock:
            original_active = self.app_module.scan_state.get("active")
            original_known_encodings = self.app_module.scan_state.get("known_encodings")
            original_known_students = self.app_module.scan_state.get("known_students")
            original_model_status = self.app_module.scan_state.get("model_status")
            original_events = list(self.app_module.scan_state.get("events") or [])
            self.app_module.scan_state["active"] = False
            self.app_module.scan_state["known_encodings"] = self.app_module.np.array(
                [
                    [0.12] * 128,
                    [0.91] * 128,
                ],
                dtype=self.app_module.np.float64,
            )
            self.app_module.scan_state["known_students"] = [
                {"student_id": student_id, "name": "Delete Face Student", "encodings": self.app_module.np.array([[0.12] * 128], dtype=self.app_module.np.float64)},
                {"student_id": "KEEP-001", "name": "Keep Student", "encodings": self.app_module.np.array([[0.91] * 128], dtype=self.app_module.np.float64)},
            ]
            self.app_module.scan_state["model_status"] = "ready"
            self.app_module.scan_state["events"] = [
                {"id": 1, "type": "verified", "student_id": student_id},
                {"id": 2, "type": "verified", "student_id": "KEEP-001"},
            ]

        self.app_module.last_scanned[student_id] = 12345.0
        self.app_module.scan_presence_locks[student_id] = {"last_seen_ts": 12345.0}

        try:
            with patch.object(self.app_module, "refresh_loaded_face_processors") as refresh_mock:
                response = client.delete(
                    f"/api/students/{current_enrollment['_id']}",
                    headers=csrf_headers,
                )

            self.assertEqual(response.status_code, 200)
            body = response.get_json()
            self.assertEqual(body["status"], "ok")
            self.assertTrue(body["validation"]["completed"])
            self.assertEqual(body["validation"]["remaining_profiles"], 0)
            self.assertEqual(body["validation"]["remaining_enrollment_records"], 0)
            self.assertGreaterEqual(body["counts"]["student_profiles_deleted"], 1)
            self.assertGreaterEqual(body["counts"]["enrollment_records_deleted"], 2)
            self.assertEqual(body["counts"]["attendance_logs_deleted"], 1)
            self.assertEqual(body["counts"]["sms_logs_deleted"], 1)
            self.assertEqual(body["counts"]["failed_scans_deleted"], 1)
            refresh_mock.assert_called_once()

            self.assertIsNone(self.app_module.students.find_one({"_id": inserted_id}))
            self.assertIsNone(self.app_module.students.find_one({"student_id": student_id}))
            self.assertEqual(
                self.app_module.get_school_year_enrollment_collection(current_school_year).count_documents({"student_id": student_id}),
                0,
            )
            self.assertEqual(
                self.app_module.get_school_year_enrollment_collection(other_school_year).count_documents({"student_id": student_id}),
                0,
            )
            self.assertEqual(self.app_module.attendance_logs.count_documents({"student_id": student_id}), 0)
            self.assertEqual(self.app_module.sms_logs.count_documents({"student_id": student_id}), 0)
            self.assertEqual(self.app_module.failed_scans.count_documents({"student_id": student_id}), 0)
            self.assertNotIn(student_id, self.app_module.last_scanned)
            self.assertNotIn(student_id, self.app_module.scan_presence_locks)
            with self.app_module.scan_lock:
                self.assertFalse(any(row.get("student_id") == student_id for row in self.app_module.scan_state.get("known_students", [])))
                self.assertFalse(any(row.get("student_id") == student_id for row in self.app_module.scan_state.get("events", [])))
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_known_encodings
                self.app_module.scan_state["known_students"] = original_known_students
                self.app_module.scan_state["model_status"] = original_model_status
                self.app_module.scan_state["events"] = original_events
            self.app_module.last_scanned.pop(student_id, None)
            self.app_module.scan_presence_locks.pop(student_id, None)
            self.app_module.students.delete_many({"_id": inserted_id})
            self.app_module.students.delete_many({"student_id": student_id})
            self.app_module.get_school_year_enrollment_collection(current_school_year).delete_many({"student_id": student_id})
            self.app_module.get_school_year_enrollment_collection(other_school_year).delete_many({"student_id": student_id})
            self.app_module.attendance_logs.delete_many({"student_id": student_id})
            self.app_module.sms_logs.delete_many({"student_id": student_id})
            self.app_module.failed_scans.delete_many({"student_id": student_id})
            self.drop_enrollment_collection_if_orphaned(other_school_year)
