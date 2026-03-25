# =====================================
# MONGODB CONNECTION
# =====================================
from pymongo import MongoClient, ASCENDING, DESCENDING
import os
from urllib.parse import urlparse
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Connect to local MongoDB (MongoDB Compass)
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
uri_path_db = urlparse(MONGO_URI).path.lstrip("/") if MONGO_URI else ""
DB_NAME = os.getenv("MONGODB_DB_NAME", uri_path_db or "face_gate_db")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
STUDENT_ENROLLMENT_COLLECTION_PREFIX = "student_"
LEGACY_STUDENT_ENROLLMENTS_COLLECTION_NAME = "student_enrollments"
ATTENDANCE_LOGS_ARCHIVE_COLLECTION_NAME = "attendance_logs_archive"
SMS_LOGS_ARCHIVE_COLLECTION_NAME = "sms_logs_archive"
ALERTS_ARCHIVE_COLLECTION_NAME = "alerts_archive"
ATTENDANCE_CORRECTIONS_ARCHIVE_COLLECTION_NAME = "attendance_corrections_archive"
EARLY_TIMEOUT_REQUESTS_ARCHIVE_COLLECTION_NAME = "early_timeout_requests_archive"
CALENDAR_EVENTS_ARCHIVE_COLLECTION_NAME = "calendar_events_archive"

# Core collections
students = db["students"]
# New canonical attendance collection
attendance_logs = db["attendance_logs"]
attendance_logs_archive = db[ATTENDANCE_LOGS_ARCHIVE_COLLECTION_NAME]
# Legacy collection retained for migration/reference
Attendance = db["Attendance"]
sms_logs = db["sms_logs"]
sms_logs_archive = db[SMS_LOGS_ARCHIVE_COLLECTION_NAME]
otp_requests = db["otp_requests"]
users = db["users"]
alerts = db["alerts"]
alerts_archive = db[ALERTS_ARCHIVE_COLLECTION_NAME]
login_history = db["login_history"]
failed_scans = db["failed_scans"]
sections = db["sections"]
school_years = db["school_years"]
student_enrollments = db[LEGACY_STUDENT_ENROLLMENTS_COLLECTION_NAME]
audit_logs = db["audit_logs"]
login_attempts = db["login_attempts"]
attendance_corrections = db["attendance_corrections"]
attendance_corrections_archive = db[ATTENDANCE_CORRECTIONS_ARCHIVE_COLLECTION_NAME]
scheduled_reports = db["scheduled_reports"]
scheduled_report_runs = db["scheduled_report_runs"]
anomaly_rules = db["anomaly_rules"]
anomaly_events = db["anomaly_events"]
system_settings = db["system_settings"]
calendar_events = db["calendar_events"]
calendar_events_archive = db[CALENDAR_EVENTS_ARCHIVE_COLLECTION_NAME]
early_timeout_requests = db["early_timeout_requests"]
early_timeout_requests_archive = db[EARLY_TIMEOUT_REQUESTS_ARCHIVE_COLLECTION_NAME]


def _normalize_index_keys(keys):
    normalized = []
    for item in keys:
        if isinstance(item, str):
            normalized.append((item, ASCENDING))
            continue
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            normalized.append((item[0], item[1]))
            continue
        raise ValueError(f"Unsupported index key specification: {item!r}")
    return tuple(normalized)


def _find_existing_index(collection, normalized_keys, requested_name=""):
    index_info = collection.index_information()
    if requested_name:
        existing = index_info.get(requested_name)
        if existing:
            return requested_name, existing

    for name, spec in index_info.items():
        if tuple(spec.get("key", [])) == normalized_keys:
            return name, spec
    return "", None


def _safe_create_index(collection, keys, **kwargs):
    normalized_keys = _normalize_index_keys(keys)
    requested_name = str(kwargs.get("name") or "").strip()
    existing_name, existing_spec = _find_existing_index(collection, normalized_keys, requested_name)
    if existing_spec is not None:
        return existing_name or requested_name or None

    try:
        return collection.create_index(list(normalized_keys), **kwargs)
    except Exception as exc:
        existing_name, existing_spec = _find_existing_index(collection, normalized_keys, requested_name)
        if existing_spec is not None:
            return existing_name or requested_name or None
        print(f"[WARNING] Could not create index on {collection.name}: {exc}")
        return None


def student_enrollment_collection_name(school_year):
    label = str(school_year or "").strip()
    if not label:
        return STUDENT_ENROLLMENT_COLLECTION_PREFIX.rstrip("_")
    return f"{STUDENT_ENROLLMENT_COLLECTION_PREFIX}{label}"


def is_student_enrollment_collection_name(name):
    raw = str(name or "").strip()
    return raw.startswith(STUDENT_ENROLLMENT_COLLECTION_PREFIX) and raw != "students"


def list_student_enrollment_collection_names():
    return sorted(
        name
        for name in db.list_collection_names()
        if is_student_enrollment_collection_name(name)
    )


def ensure_student_enrollment_collection_indexes(collection):
    _safe_create_index(collection, [("student_id", ASCENDING)], unique=True, sparse=True)
    _safe_create_index(collection, [("grade_level", ASCENDING), ("section", ASCENDING)])
    _safe_create_index(collection, [("status", ASCENDING)])
    _safe_create_index(collection, [("face_registered", ASCENDING)])
    _safe_create_index(collection, [("name", ASCENDING)])
    _safe_create_index(collection, [("student_ref_id", ASCENDING)])
    _safe_create_index(collection, [("section", ASCENDING)])
    _safe_create_index(collection, [("created_at", DESCENDING)])


def ensure_attendance_logs_indexes(collection):
    _safe_create_index(collection, [("school_year", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("timestamp", DESCENDING)])
    _safe_create_index(collection, [("date", ASCENDING)])
    _safe_create_index(collection, [("student_id", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(collection, [("student_id", ASCENDING), ("status", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("status", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(collection, [("grade_level", ASCENDING), ("section", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("grade", ASCENDING), ("section", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("gate_action", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("session", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("legacy_id", ASCENDING)], unique=True, sparse=True)
    if collection.name == "attendance_logs":
        _safe_create_index(collection, [("student_id", ASCENDING), ("date", ASCENDING), ("session", ASCENDING)], unique=True, sparse=True)


def ensure_sms_logs_indexes(collection):
    _safe_create_index(collection, [("school_year", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(collection, [("timestamp", DESCENDING)])
    _safe_create_index(collection, [("createdAt", DESCENDING)])
    _safe_create_index(collection, [("updatedAt", DESCENDING)])
    _safe_create_index(collection, [("status", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(collection, [("student_id", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(collection, [("to", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(collection, [("type", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(collection, [("provider", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(collection, [("providerMessageId", ASCENDING)], sparse=True)
    _safe_create_index(collection, [("httpStatus", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(collection, [("errorCode", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(collection, [("parent_contact", ASCENDING), ("date", DESCENDING)])


def ensure_alerts_indexes(collection):
    _safe_create_index(collection, [("school_year", ASCENDING), ("timestamp", DESCENDING)])
    _safe_create_index(collection, [("is_read", ASCENDING), ("created_at", DESCENDING)])
    _safe_create_index(collection, [("category", ASCENDING), ("created_at", DESCENDING)])
    _safe_create_index(collection, [("status", ASCENDING), ("timestamp", DESCENDING)])
    _safe_create_index(collection, [("type", ASCENDING), ("timestamp", DESCENDING)])


def ensure_attendance_corrections_indexes(collection):
    _safe_create_index(collection, [("school_year", ASCENDING), ("requestedAt", DESCENDING)])
    _safe_create_index(collection, [("status", ASCENDING), ("requestedAt", DESCENDING)])
    _safe_create_index(collection, [("attendance_log_id", ASCENDING), ("status", ASCENDING)])
    _safe_create_index(collection, [("requested_by", ASCENDING), ("requestedAt", DESCENDING)])
    _safe_create_index(collection, [("reviewed_by", ASCENDING), ("reviewedAt", DESCENDING)])


def get_student_enrollment_collection(school_year):
    collection = db[student_enrollment_collection_name(school_year)]
    ensure_student_enrollment_collection_indexes(collection)
    return collection


def ensure_indexes():
    _safe_create_index(students, [("lrn", ASCENDING)], unique=True, sparse=True)
    _safe_create_index(students, [("student_id", ASCENDING)], unique=True, sparse=True)
    _safe_create_index(students, [("name", ASCENDING)])
    _safe_create_index(students, [("section", ASCENDING)])
    _safe_create_index(students, [("grade_level", ASCENDING)])
    _safe_create_index(students, [("grade", ASCENDING), ("section", ASCENDING)])
    _safe_create_index(students, [("grade_level", ASCENDING), ("section", ASCENDING)])
    _safe_create_index(students, [("face_registered", ASCENDING)])
    _safe_create_index(students, [("gender", ASCENDING)])
    _safe_create_index(students, [("status", ASCENDING)])
    _safe_create_index(students, [("created_at", DESCENDING)])
    _safe_create_index(students, [("grade", ASCENDING), ("section", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)])
    _safe_create_index(students, [("gender", ASCENDING), ("grade_level", ASCENDING), ("section", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)])

    ensure_attendance_logs_indexes(attendance_logs)
    ensure_attendance_logs_indexes(attendance_logs_archive)

    ensure_sms_logs_indexes(sms_logs)
    ensure_sms_logs_indexes(sms_logs_archive)

    _safe_create_index(otp_requests, [("phone", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(otp_requests, [("phone", ASCENDING), ("status", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(otp_requests, [("expiresAt", ASCENDING)])
    _safe_create_index(otp_requests, [("status", ASCENDING), ("verifiedAt", DESCENDING)])

    _safe_create_index(users, [("username", ASCENDING)], unique=True)
    _safe_create_index(users, [("email", ASCENDING)], sparse=True)
    _safe_create_index(users, [("fullName", ASCENDING)])
    _safe_create_index(users, [("role", ASCENDING)])
    _safe_create_index(users, [("twoFactorEnabled", ASCENDING)])
    _safe_create_index(users, [("updatedAt", DESCENDING)])

    ensure_alerts_indexes(alerts)
    ensure_alerts_indexes(alerts_archive)

    _safe_create_index(login_history, [("username", ASCENDING), ("timestamp", DESCENDING)])
    _safe_create_index(login_attempts, [("username_lower", ASCENDING), ("ip", ASCENDING)], unique=True)
    _safe_create_index(login_attempts, [("lockout_until", ASCENDING)])
    _safe_create_index(login_attempts, [("last_attempt_at", DESCENDING)])
    _safe_create_index(audit_logs, [("createdAt", DESCENDING)])
    _safe_create_index(audit_logs, [("action", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(audit_logs, [("actor.username", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(audit_logs, [("target_type", ASCENDING), ("target_id", ASCENDING), ("createdAt", DESCENDING)])
    ensure_attendance_corrections_indexes(attendance_corrections)
    ensure_attendance_corrections_indexes(attendance_corrections_archive)
    _safe_create_index(scheduled_reports, [("enabled", ASCENDING), ("next_run_at", ASCENDING)])
    _safe_create_index(scheduled_reports, [("updated_at", DESCENDING)])
    _safe_create_index(scheduled_reports, [("name", ASCENDING)], unique=True)
    _safe_create_index(scheduled_report_runs, [("report_id", ASCENDING), ("started_at", DESCENDING)])
    _safe_create_index(scheduled_report_runs, [("status", ASCENDING), ("started_at", DESCENDING)])
    _safe_create_index(anomaly_rules, [("enabled", ASCENDING), ("updated_at", DESCENDING)])
    _safe_create_index(anomaly_rules, [("name", ASCENDING)], unique=True)
    _safe_create_index(anomaly_events, [("rule_id", ASCENDING), ("triggered_at", DESCENDING)])
    _safe_create_index(anomaly_events, [("triggered_at", DESCENDING)])
    _safe_create_index(failed_scans, [("timestamp", DESCENDING)])
    _safe_create_index(failed_scans, [("date", ASCENDING), ("reason", ASCENDING)])
    _safe_create_index(failed_scans, [("student_id", ASCENDING), ("reason", ASCENDING), ("date", DESCENDING)])

    _safe_create_index(system_settings, [("key", ASCENDING)], unique=True)
    _safe_create_index(calendar_events, [("date", ASCENDING)], unique=True)
    _safe_create_index(calendar_events, [("school_year", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(early_timeout_requests, [("student_id", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(early_timeout_requests, [("status", ASCENDING), ("requested_at", DESCENDING)])
    _safe_create_index(early_timeout_requests, [("school_year", ASCENDING), ("requested_at", DESCENDING)])

    try:
        sections.drop_index("grade_key_1_section_normalized_1")
    except Exception:
        pass
    _safe_create_index(sections, [("school_year", ASCENDING), ("grade_key", ASCENDING), ("section_normalized", ASCENDING)], unique=True)
    _safe_create_index(sections, [("school_year", ASCENDING), ("grade_key", ASCENDING), ("section", ASCENDING)])
    _safe_create_index(sections, [("school_year", ASCENDING), ("updated_at", DESCENDING)])

    _safe_create_index(school_years, [("label", ASCENDING)], unique=True)
    _safe_create_index(school_years, [("is_current", ASCENDING), ("updated_at", DESCENDING)])
    _safe_create_index(school_years, [("start_year", DESCENDING)])

    if LEGACY_STUDENT_ENROLLMENTS_COLLECTION_NAME in db.list_collection_names():
        _safe_create_index(student_enrollments, [("school_year", ASCENDING), ("student_id", ASCENDING)], unique=True)
        _safe_create_index(student_enrollments, [("school_year", ASCENDING), ("grade_level", ASCENDING), ("section", ASCENDING)])
        _safe_create_index(student_enrollments, [("school_year", ASCENDING), ("status", ASCENDING)])
        _safe_create_index(student_enrollments, [("school_year", ASCENDING), ("face_registered", ASCENDING)])
        _safe_create_index(student_enrollments, [("school_year", ASCENDING), ("name", ASCENDING)])
        _safe_create_index(student_enrollments, [("student_ref_id", ASCENDING)])
        _safe_create_index(student_enrollments, [("created_at", DESCENDING)])
    for collection_name in list_student_enrollment_collection_names():
        ensure_student_enrollment_collection_indexes(db[collection_name])


ensure_indexes()
