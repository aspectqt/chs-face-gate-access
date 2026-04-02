import importlib
import unittest
import uuid
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

    def test_full_admin_pages_render(self):
        client = self.make_client()

        for route in ("/dashboard", "/analytics", "/students", "/gate-logs", "/sms-logs"):
            with self.subTest(route=route):
                response = client.get(route)
                self.assertEqual(response.status_code, 200)
                self.assertIn("text/html", response.content_type)

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
        self.assertIn(b"Prepared by", payload)
        self.assertIn(b"Approved by", payload)

        inline_response = client.get("/students/export_pdf?disposition=inline")
        self.assertEqual(inline_response.status_code, 200)
        self.assertIn("inline", inline_response.headers.get("Content-Disposition", "").lower())

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

        cached = self.app_module.get_cached_live_track_encoding(stable_track, now_ts=50.1)
        stale = self.app_module.get_cached_live_track_encoding(stable_track, now_ts=50.5)
        moving = self.app_module.get_cached_live_track_encoding(moving_track, now_ts=50.1)

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
                stack.enter_context(patch.multiple(
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
                stack.enter_context(patch.multiple(
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
