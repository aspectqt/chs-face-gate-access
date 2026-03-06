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

# Core collections
students = db["students"]
# New canonical attendance collection
attendance_logs = db["attendance_logs"]
# Legacy collection retained for migration/reference
Attendance = db["Attendance"]
sms_logs = db["sms_logs"]
otp_requests = db["otp_requests"]
users = db["users"]
alerts = db["alerts"]
login_history = db["login_history"]
failed_scans = db["failed_scans"]
sections = db["sections"]


def _safe_create_index(collection, keys, **kwargs):
    try:
        collection.create_index(keys, **kwargs)
    except Exception as exc:
        print(f"[WARNING] Could not create index on {collection.name}: {exc}")


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

    _safe_create_index(attendance_logs, [("timestamp", DESCENDING)])
    _safe_create_index(attendance_logs, [("date", ASCENDING)])
    _safe_create_index(attendance_logs, [("student_id", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(attendance_logs, [("student_id", ASCENDING), ("status", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(attendance_logs, [("status", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(attendance_logs, [("grade_level", ASCENDING), ("section", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(attendance_logs, [("grade", ASCENDING), ("section", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(attendance_logs, [("gate_action", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(attendance_logs, [("session", ASCENDING), ("date", DESCENDING)])
    _safe_create_index(attendance_logs, [("student_id", ASCENDING), ("date", ASCENDING), ("session", ASCENDING)], unique=True, sparse=True)
    _safe_create_index(attendance_logs, [("legacy_id", ASCENDING)], unique=True, sparse=True)

    _safe_create_index(sms_logs, [("timestamp", DESCENDING)])
    _safe_create_index(sms_logs, [("createdAt", DESCENDING)])
    _safe_create_index(sms_logs, [("updatedAt", DESCENDING)])
    _safe_create_index(sms_logs, [("status", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(sms_logs, [("student_id", ASCENDING), ("date", ASCENDING)])
    _safe_create_index(sms_logs, [("to", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(sms_logs, [("type", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(sms_logs, [("provider", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(sms_logs, [("providerMessageId", ASCENDING)], sparse=True)
    _safe_create_index(sms_logs, [("httpStatus", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(sms_logs, [("errorCode", ASCENDING), ("createdAt", DESCENDING)])
    _safe_create_index(sms_logs, [("parent_contact", ASCENDING), ("date", DESCENDING)])

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

    _safe_create_index(alerts, [("is_read", ASCENDING), ("created_at", DESCENDING)])
    _safe_create_index(alerts, [("category", ASCENDING), ("created_at", DESCENDING)])

    _safe_create_index(login_history, [("username", ASCENDING), ("timestamp", DESCENDING)])
    _safe_create_index(failed_scans, [("timestamp", DESCENDING)])
    _safe_create_index(failed_scans, [("date", ASCENDING), ("reason", ASCENDING)])
    _safe_create_index(failed_scans, [("student_id", ASCENDING), ("reason", ASCENDING), ("date", DESCENDING)])

    _safe_create_index(sections, [("grade_key", ASCENDING), ("section_normalized", ASCENDING)], unique=True)
    _safe_create_index(sections, [("grade_key", ASCENDING), ("section", ASCENDING)])
    _safe_create_index(sections, [("updated_at", DESCENDING)])


ensure_indexes()
