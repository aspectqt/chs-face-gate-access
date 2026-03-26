import importlib
import unittest
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch


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
        self.assertNotIn("Manual IN", html)
        self.assertNotIn("Manual OUT", html)
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
            "scan_cooldown_minutes": 45,
        }
        csrf_headers = {self.app_module.CSRF_HEADER_NAME: "test-csrf-token"}

        try:
            response = client.post("/api/schedule/default", json=payload, headers=csrf_headers)
            self.assertEqual(response.status_code, 200)
            body = response.get_json()
            self.assertEqual(body["status"], "ok")
            self.assertEqual(body["schedule"]["late_threshold_minutes"], 20)
            self.assertEqual(body["schedule"]["scan_cooldown_minutes"], 45)
            self.assertEqual(body["schedule"]["morning_late"], "05:20")
            self.assertEqual(body["schedule"]["afternoon_late"], "13:20")

            fetched = client.get("/api/schedule/default")
            self.assertEqual(fetched.status_code, 200)
            fetched_body = fetched.get_json()
            self.assertEqual(fetched_body["schedule"]["scan_cooldown_minutes"], 45)
            self.assertEqual(fetched_body["schedule"]["late_threshold_minutes"], 20)
        finally:
            self.restore_default_schedule_doc(original_doc)

    def test_pdf_action_menu_is_present_on_export_pages(self):
        client = self.make_client()

        for route in ("/students", "/gate-logs", "/sms-logs"):
            with self.subTest(route=route):
                response = client.get(route)
                self.assertEqual(response.status_code, 200)
                html = response.get_data(as_text=True)
                self.assertIn("Download PDF", html)
                self.assertIn("Print PDF", html)

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

        payload = pdf_response.get_data()
        self.assertTrue(payload.startswith(b"%PDF-"))
        self.assertIn(b"Official Student Records Report", payload)

        inline_response = client.get("/students/export_pdf?disposition=inline")
        self.assertEqual(inline_response.status_code, 200)
        self.assertIn("inline", inline_response.headers.get("Content-Disposition", "").lower())

    def test_students_pdf_export_supports_grade_section_and_individual_scopes(self):
        collection = self.app_module.get_school_year_enrollment_collection(self.app_module.get_current_school_year_label())
        sample_student = collection.find_one({}, {"student_id": 1, "grade_level": 1, "section": 1})
        if not sample_student:
            self.skipTest("No student data available for scoped PDF export checks.")

        client = self.make_client()
        grade_value = str(sample_student.get("grade_level") or "")
        section_value = str(sample_student.get("section") or "")
        student_id = str(sample_student.get("student_id") or "")

        checks = [
            (f"/students/export_pdf?grade={grade_value}", "grade"),
            (f"/students/export_pdf?section={section_value}", "section"),
            (f"/students/export_pdf?q={student_id}", "individual"),
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
                self.assertTrue(response.get_data().startswith(b"%PDF-"))

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
            with patch.object(self.app_module.cv2, "imdecode", return_value=frame), \
                    patch.object(self.app_module.face_recognition, "face_locations", return_value=[(10, 110, 110, 10), (40, 250, 160, 140)]), \
                    patch.object(self.app_module.face_recognition, "face_encodings", return_value=encodings), \
                    patch.object(
                        self.app_module.face_recognition,
                        "face_distance",
                        side_effect=[
                            self.app_module.np.array([0.21, 0.74], dtype=self.app_module.np.float64),
                            self.app_module.np.array([0.72, 0.19], dtype=self.app_module.np.float64),
                        ],
                    ), \
                    patch.object(self.app_module, "calculate_match_confidence", side_effect=[99.2, 98.6]), \
                    patch.object(
                        self.app_module,
                        "handle_verified_student",
                        side_effect=[
                            {"student_id": "MF-001", "gate_action": "IN"},
                            {"student_id": "MF-002", "gate_action": "IN"},
                        ],
                    ) as verified_mock, \
                    patch.object(self.app_module, "push_not_registered_event") as not_registered_mock, \
                    patch.object(self.app_module, "push_multi_face_event") as multi_face_mock:
                success, message = self.app_module.process_client_frame(b"frame")

            self.assertTrue(success)
            self.assertIn("Verified 2 students", message)
            self.assertIn("Multi Face One", message)
            self.assertIn("Multi Face Two", message)
            self.assertEqual(verified_mock.call_count, 2)
            not_registered_mock.assert_not_called()
            multi_face_mock.assert_not_called()
        finally:
            with self.app_module.scan_lock:
                self.app_module.scan_state["active"] = original_active
                self.app_module.scan_state["known_encodings"] = original_encodings
                self.app_module.scan_state["known_students"] = original_students
                self.app_module.scan_state["model_status"] = original_model_status

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
