from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response, stream_with_context
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta, time as dtime
from decimal import Decimal, InvalidOperation
import csv
import os
import cv2
import face_recognition
import numpy as np
from dotenv import load_dotenv
from config import DB_NAME, students, attendance_logs, sms_logs, otp_requests, users, alerts, login_history, failed_scans, sections
import json
from io import BytesIO, StringIO
from PIL import Image
import base64
import threading
import time
import unicodedata
from functools import wraps
from urllib.parse import urlencode
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import re
import uuid
import requests
import traceback
from services.sms_provider import SmsProvider, create_sms_provider_from_env
from services.otp_service import generate_otp_code, hash_otp_code, verify_otp_code
from services.ai_analytics import (
    SUPPORTED_CHANGE_MODES,
    SUPPORTED_INSIGHT_RANGES,
    SUPPORTED_RISK_TARGETS,
    get_ai_insights,
    get_change_explanations,
    get_next_best_actions,
    get_risk_predictions,
    run_nlq_query,
)
try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Alignment, Border, Font, Side
    from openpyxl.utils import get_column_letter
except Exception:
    Workbook = None
    load_workbook = None
    Alignment = None
    Border = None
    Font = None
    Side = None
    get_column_letter = None

# =====================================
# ENVIRONMENT
# =====================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


def env_int(name, default, minimum=None, maximum=None):
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        print(f"[WARNING] Invalid integer for {name}: {raw!r}. Using default={default}.")
        value = int(default)

    if minimum is not None and value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def env_bool(name, default=False):
    raw = os.getenv(name, str(int(bool(default))))
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

# =====================================
# FLASK SETUP
# =====================================
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "super_secret_key_change_this")
if app.secret_key == "super_secret_key_change_this":
    print("[WARNING] FLASK_SECRET_KEY is not set. Using insecure default key.")
app.permanent_session_lifetime = timedelta(days=14)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = env_bool("SESSION_COOKIE_SECURE", False)
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024
AVATAR_UPLOAD_DIR = os.path.join(app.root_path, "static", "avatars")
os.makedirs(AVATAR_UPLOAD_DIR, exist_ok=True)

# =====================================
# CONFIGURATION
# =====================================
SCAN_COOLDOWN_SECONDS = env_int("SCAN_COOLDOWN_SECONDS", 8, minimum=5, maximum=30)
UNKNOWN_ALERT_COOLDOWN_SECONDS = 30
UNREGISTERED_EVENT_COOLDOWN_SECONDS = 2
RECOGNITION_TOLERANCE = 0.43
MIN_RECOGNITION_CONFIDENCE = 57.0
PASSWORD_HASH_METHOD = "pbkdf2:sha256:600000"
ALLOWED_AVATAR_EXTENSIONS = {"jpg", "jpeg", "png", "webp"}
MAX_AVATAR_SIZE_BYTES = 5 * 1024 * 1024
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128
MORNING_START = dtime(hour=5, minute=0)
NOON_START = dtime(hour=12, minute=0)
AFTERNOON_START = dtime(hour=13, minute=0)
AFTERNOON_END_START = dtime(hour=17, minute=0)
MORNING_LATE_THRESHOLD = dtime(hour=8, minute=15)
AFTERNOON_LATE_THRESHOLD = dtime(hour=13, minute=15)
GRADE_LEVEL_OPTIONS = ["Grade 7", "Grade 8", "Grade 9", "Grade 10", "Grade 11", "Grade 12"]
STUDENT_IMPORT_ALLOWED_EXTENSIONS = {"xlsx"}
STUDENT_IMPORT_MAX_ROWS = env_int("STUDENT_IMPORT_MAX_ROWS", 0, minimum=0, maximum=200000)
REQUIRED_STUDENT_IMPORT_FIELDS = {"lrn", "name", "gender"}
STUDENT_IMPORT_HEADER_ALIASES = {
    "lrn": {"lrn", "learnerreferencenumber", "learnerreference", "studentid", "studentnumber"},
    "name": {"name", "studentname", "fullname", "full_name"},
    "grade_level": {"gradelevel", "grade", "gradelevelsection", "yearlevel"},
    "gender": {"gender", "sex", "sexgender", "sexorgender"},
    "section": {"section", "advisory", "class", "homeroom"},
}
PREDEFINED_SECTIONS_BY_GRADE = {
    "Grade 7": ["AVILA", "CALINGACION", "GUIRON", "VILLASAN"],
    "Grade 8": ["ELNAR", "FERRATER", "FLORES", "SARNE", "TRACES"],
    "Grade 9": ["NUIQUE", "PALENCIA", "RUBIO"],
    "Grade 10": ["BORROMEO", "FEROLINO", "PONSICA", "SY"],
}
PREDEFINED_SECTION_LOOKUP = {
    section.lower(): {"grade_level": grade_level, "section": section}
    for grade_level, section_values in PREDEFINED_SECTIONS_BY_GRADE.items()
    for section in section_values
}
STUDENT_IMPORT_TEMPLATE_HEADERS = ["LRN", "", "NAME", "", "", "", "Sex / Gender", "Section", "Grade Level"]
STUDENT_IMPORT_TEMPLATE_SAMPLE_ROWS = [
    ["120526180006", "ARADAN,LOUIS MIGUEL, SITOY", "M", "AVILA", "Grade 7"],
    ["120507180005", "AUJERO,IYAN, ARDIENTE", "M", "AVILA", "Grade 7"],
    ["120508130014", "BALIGASA,RICKY, AURILIO", "M", "AVILA", "Grade 7"],
    ["120507180026", "ALFONSO,CHADITH, GAUDIA", "F", "AVILA", "Grade 7"],
    ["120526180025", "BANDICO,REXCYN MAE, QUILAT", "F", "AVILA", "Grade 7"],
]
OTP_CODE_LENGTH = env_int("OTP_CODE_LENGTH", 6, minimum=4, maximum=10)
OTP_EXPIRES_MINUTES = env_int("OTP_EXPIRES_MINUTES", 5, minimum=1, maximum=30)
OTP_MAX_ATTEMPTS = env_int("OTP_MAX_ATTEMPTS", 5, minimum=1, maximum=10)
OTP_THROTTLE_SECONDS = env_int("OTP_THROTTLE_SECONDS", 60, minimum=0, maximum=3600)
OTP_MAX_PER_HOUR = env_int("OTP_MAX_PER_HOUR", 5, minimum=1, maximum=100)
AI_NLQ_LLM_ENABLED = os.getenv("AI_NLQ_LLM_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_SECURITY_HEADERS = env_bool("ENABLE_SECURITY_HEADERS", True)
CSP_ENFORCE = env_bool("CSP_ENFORCE", False)
VALID_SCAN_SESSION_MODES = {"auto", "manual_in", "manual_out"}
CONTENT_SECURITY_POLICY = "; ".join([
    "default-src 'self'",
    "base-uri 'self'",
    "object-src 'none'",
    "frame-ancestors 'none'",
    "form-action 'self'",
    "img-src 'self' data: blob:",
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval' https://cdn.tailwindcss.com https://cdn.jsdelivr.net",
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
    "font-src 'self' data: https://cdn.jsdelivr.net",
    "connect-src 'self' https://cdn.jsdelivr.net",
    "media-src 'self' blob:",
    "worker-src 'self' blob:",
])

sms_provider = create_sms_provider_from_env()
sms_provider_startup_status = sms_provider.validate_configuration(raise_on_error=False)
print(f"[INFO] MongoDB database selected: {DB_NAME}")
if DB_NAME != "face_gate_db":
    print(f"[WARNING] MONGODB_DB_NAME is '{DB_NAME}'. Expected 'face_gate_db'.")
if sms_provider_startup_status.get("status") != "ok":
    print(f"[WARNING] SMS provider not ready at startup: {sms_provider_startup_status.get('message')}")

# =====================================
# GLOBAL STATE
# =====================================
last_scanned = {}

scan_lock = threading.Lock()
scan_state = {
    "active": False,
    "capture": None,
    "events": [],
    "event_counter": 0,
    "last_unknown_alert_ts": 0.0,
    "last_not_registered_ts": 0.0,
    "last_multi_face_ts": 0.0,
    "model_status": "idle",
    "known_encodings": [],
    "known_students": [],
    "session_mode": "auto",
}

alert_lock = threading.Lock()
alert_revision = 0
data_change_lock = threading.Lock()
data_change_revision = 0
data_change_domains = {
    "students": 0,
    "sections": 0,
    "gate_logs": 0,
    "sms_logs": 0,
}

ROLE_PERMISSIONS = {
    "Full Admin": {"dashboard", "scan", "students_read", "students_write", "logs", "analytics", "users_manage", "alerts_manage"},
    "Limited Access": {"dashboard", "scan", "students_read", "logs", "analytics", "alerts_manage"},
}


# =====================================
# HELPER FUNCTIONS
# =====================================
def login_required():
    return "admin" in session


def hash_password(password):
    return generate_password_hash(password, method=PASSWORD_HASH_METHOD)


def post_login_redirect(role):
    # Role-based redirect map can be extended when distinct staff pages exist.
    role_routes = {
        "Full Admin": "dashboard",
        "Limited Access": "dashboard",
    }
    return url_for(role_routes.get(role, "dashboard"))


def validate_email_format(value):
    if not value:
        return False
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value) is not None


def validate_phone_format(value):
    if not value:
        return True
    return re.match(r"^[0-9+\-\s()]{7,20}$", value) is not None


def normalize_parent_contact_value(value):
    raw = str(value or "").strip()
    if not raw:
        return ""

    compact = re.sub(r"[\s\-\(\)]", "", raw)
    if compact in {"+63", "63"}:
        return ""

    digits = ""
    if compact.startswith("+63"):
        digits = compact[3:]
    elif compact.startswith("63"):
        digits = compact[2:]
    elif compact.startswith("09"):
        digits = compact[1:]
    elif compact.startswith("9"):
        digits = compact
    else:
        raise ValueError("Parent contact must be a Philippine mobile number (+639XXXXXXXXX).")

    if not digits.isdigit():
        raise ValueError("Parent contact must contain numbers only.")

    normalized = f"+63{digits}"
    if re.match(r"^\+639\d{9}$", normalized) is None:
        raise ValueError("Parent contact must be in +639XXXXXXXXX format.")
    return normalized


def normalize_section_value(value):
    cleaned = re.sub(r"\s+", " ", str(value or "")).strip()
    return cleaned[:64]


def normalize_student_import_header(value):
    normalized = normalize_text_value(value).lower()
    return re.sub(r"[^a-z0-9]+", "", normalized)


def map_student_import_columns(header_row):
    mapping = {}
    for idx, raw_header in enumerate(header_row or []):
        header_key = normalize_student_import_header(raw_header)
        if not header_key:
            continue
        for field_name, aliases in STUDENT_IMPORT_HEADER_ALIASES.items():
            if field_name in mapping:
                continue
            if header_key in aliases:
                mapping[field_name] = idx
                break
    return mapping


def is_student_import_summary_row(row_payload):
    if not isinstance(row_payload, dict):
        return False
    lrn_text = normalize_text_value(row_payload.get("lrn", "")).lower()
    name_text = normalize_text_value(row_payload.get("name", "")).lower()
    combined = f"{lrn_text} {name_text}".strip()
    if not combined:
        return False
    if "total male" in combined or "total female" in combined:
        return True
    if "<==" in combined and "total" in combined:
        return True
    return False


def parse_student_import_workbook(file_bytes):
    if load_workbook is None:
        raise ValueError("Excel import dependency is not installed on the server.")
    if not file_bytes:
        raise ValueError("Uploaded Excel file is empty.")

    workbook = None
    try:
        workbook = load_workbook(filename=BytesIO(file_bytes), data_only=True, read_only=False)
        sheet = None
        header_row_number = 0
        column_mapping = {}

        for candidate_sheet in workbook.worksheets:
            for row_index, row_values in enumerate(candidate_sheet.iter_rows(values_only=True), start=1):
                if all(normalize_text_value(value) == "" for value in row_values or []):
                    continue
                candidate_mapping = map_student_import_columns(row_values)
                if REQUIRED_STUDENT_IMPORT_FIELDS.issubset(set(candidate_mapping.keys())):
                    sheet = candidate_sheet
                    header_row_number = row_index
                    column_mapping = candidate_mapping
                    break
            if column_mapping:
                break

        if not column_mapping:
            expected = "LRN, NAME, Sex / Gender (required). GRADE LEVEL and SECTION are optional if provided as defaults."
            raise ValueError(f"Excel template is invalid. Required columns: {expected}.")

        max_rows_allowed = STUDENT_IMPORT_MAX_ROWS if STUDENT_IMPORT_MAX_ROWS and STUDENT_IMPORT_MAX_ROWS > 0 else 0
        parsed_rows = []
        for row_index, row_values in enumerate(
            sheet.iter_rows(min_row=header_row_number + 1, values_only=True),
            start=header_row_number + 1,
        ):
            if all(normalize_text_value(value) == "" for value in row_values or []):
                continue

            if max_rows_allowed and len(parsed_rows) >= max_rows_allowed:
                raise ValueError(f"Import limit reached. Maximum allowed rows is {max_rows_allowed}.")

            row_payload = {"row_number": row_index}
            for field_name, column_index in column_mapping.items():
                row_payload[field_name] = row_values[column_index] if column_index < len(row_values) else ""
            parsed_rows.append(row_payload)

        if not parsed_rows:
            raise ValueError("No student rows found in the uploaded Excel file.")
        return parsed_rows
    except ValueError:
        raise
    except Exception:
        raise ValueError("Unable to read Excel file. Please upload a valid .xlsx template.")
    finally:
        if workbook is not None:
            workbook.close()


def build_student_import_template_bytes():
    if Workbook is None:
        raise ValueError("Excel import dependency is not installed on the server.")

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Sheet1"

    sheet.append(STUDENT_IMPORT_TEMPLATE_HEADERS)
    sheet.append([""] * len(STUDENT_IMPORT_TEMPLATE_HEADERS))
    for lrn, name, gender, section, grade_level in STUDENT_IMPORT_TEMPLATE_SAMPLE_ROWS:
        sheet.append([lrn, "", name, "", "", "", gender, section, grade_level])

    border = Border(
        left=Side(style="thin", color="000000"),
        right=Side(style="thin", color="000000"),
        top=Side(style="thin", color="000000"),
        bottom=Side(style="thin", color="000000"),
    ) if Border and Side else None

    header_font = Font(bold=True) if Font else None
    center_alignment = Alignment(horizontal="center", vertical="center") if Alignment else None
    left_alignment = Alignment(horizontal="left", vertical="center") if Alignment else None

    total_rows = 2 + len(STUDENT_IMPORT_TEMPLATE_SAMPLE_ROWS)
    total_cols = len(STUDENT_IMPORT_TEMPLATE_HEADERS)
    for row_idx in range(1, total_rows + 1):
        for col_idx in range(1, total_cols + 1):
            cell = sheet.cell(row=row_idx, column=col_idx)
            if border:
                cell.border = border
            if row_idx == 1 and col_idx in (1, 3, 7, 8, 9):
                if header_font:
                    cell.font = header_font
                if center_alignment:
                    cell.alignment = center_alignment
            elif row_idx >= 3:
                if col_idx in (1, 3) and left_alignment:
                    cell.alignment = left_alignment
                elif col_idx in (7, 8, 9) and center_alignment:
                    cell.alignment = center_alignment

    sheet.merge_cells("A1:B2")
    sheet.merge_cells("C1:F2")
    sheet.merge_cells("G1:G2")
    sheet.merge_cells("H1:H2")
    sheet.merge_cells("I1:I2")
    for row_idx in range(3, total_rows + 1):
        sheet.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=2)
        sheet.merge_cells(start_row=row_idx, start_column=3, end_row=row_idx, end_column=6)

    column_widths = {
        1: 13,
        2: 13,
        3: 13,
        4: 13,
        5: 13,
        6: 13,
        7: 11.42578125,
        8: 9.140625,
        9: 13.28515625,
    }
    for col_idx, width_value in column_widths.items():
        if get_column_letter:
            sheet.column_dimensions[get_column_letter(col_idx)].width = width_value

    output = BytesIO()
    workbook.save(output)
    workbook.close()
    output.seek(0)
    return output.getvalue()


def normalize_lrn_value(value):
    if value is None:
        return ""

    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            return ""
        text = str(value.quantize(Decimal(1))) if value == value.to_integral_value() else format(value.normalize(), "f")
    elif isinstance(value, (int, np.integer)):
        text = str(int(value))
    elif isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return ""
        if float(value).is_integer():
            text = str(int(value))
        else:
            try:
                dec = Decimal(str(value))
                text = format(dec.normalize(), "f")
            except InvalidOperation:
                text = str(value)
    else:
        text = str(value)

    text = unicodedata.normalize("NFKC", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) not in {"Cf", "Cc"})
    text = text.replace("\ufeff", "").replace("\u200b", "").replace("\xa0", " ")
    text = text.strip()

    # Unwrap formula-like values such as ="120526180006" before strict validation.
    if text.startswith("="):
        formula_text = text[1:].strip()
        quoted_match = re.fullmatch(r"""['"](.+)['"]""", formula_text)
        if quoted_match:
            text = quoted_match.group(1).strip()
        else:
            text = formula_text

    text = re.sub(r"^[`'\"\u2018\u2019\u201c\u201d]+", "", text).strip()
    text = re.sub(r"[`'\"\u2018\u2019\u201c\u201d]+$", "", text).strip()
    text = re.sub(r"[\u2010-\u2015\u2212]", "-", text)
    text = text.replace(",", "")
    normalized = re.sub(r"\s+", "", text)

    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?[eE][+-]?\d+", normalized):
        try:
            sci = Decimal(normalized)
            if sci == sci.to_integral_value():
                normalized = str(sci.quantize(Decimal(1)))
            else:
                normalized = format(sci.normalize(), "f").rstrip("0").rstrip(".")
        except InvalidOperation:
            pass

    if re.fullmatch(r"\d+\.0+", normalized):
        normalized = normalized.split(".", 1)[0]
    return normalized[:32]


def validate_lrn_value(value):
    lrn = normalize_lrn_value(value)
    if not lrn:
        return "", "LRN is required."
    if re.match(r"^[A-Za-z0-9_-]+$", lrn) is None:
        return "", "LRN may contain only letters, numbers, dashes, and underscores."
    return lrn, ""


def sanitize_profile_text(value, max_length, allow_newlines=False):
    raw = str(value or "")
    if allow_newlines:
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", raw)
        cleaned = re.sub(r"\r\n?", "\n", cleaned)
        cleaned = "\n".join(line.strip() for line in cleaned.split("\n"))
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    else:
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", raw)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_length]


def normalize_theme_value(value, default="light"):
    normalized = (value or "").strip().lower()
    if normalized in ("light", "dark"):
        return normalized
    return default


def normalize_profile_user_doc(user_doc):
    if not user_doc:
        return None

    username = (user_doc.get("username") or "").strip()
    email = (user_doc.get("email") or "").strip()
    if not email:
        email = f"{username}@chs.local" if username else ""

    full_name = (user_doc.get("fullName") or "").strip() or username
    avatar_url = (user_doc.get("avatarUrl") or "").strip()
    updated_at = (user_doc.get("updatedAt") or user_doc.get("updated_at") or user_doc.get("created_at") or "").strip()

    return {
        "username": username,
        "role": user_doc.get("role", "Limited Access"),
        "fullName": full_name,
        "email": email,
        "phone": (user_doc.get("phone") or "").strip(),
        "address": (user_doc.get("address") or "").strip(),
        "bio": (user_doc.get("bio") or "").strip(),
        "avatarUrl": avatar_url,
        "twoFactorEnabled": bool(user_doc.get("twoFactorEnabled", False)),
        "updatedAt": updated_at,
        "theme": normalize_theme_value(user_doc.get("theme")),
    }


def current_user_profile():
    username = session.get("admin", "").strip()
    if not username:
        return None, None
    user_doc = users.find_one({"username": username})
    if not user_doc:
        return None, None
    return user_doc, normalize_profile_user_doc(user_doc)


@app.context_processor
def inject_global_theme():
    theme = normalize_theme_value(session.get("theme"), default="")
    if theme:
        return {"current_theme": theme}

    if not session.get("admin"):
        return {"current_theme": ""}

    try:
        user_doc, profile = current_user_profile()
        if user_doc and profile:
            theme = normalize_theme_value(profile.get("theme"))
            session["theme"] = theme
            return {"current_theme": theme}
    except Exception:
        pass
    return {"current_theme": ""}


def current_role():
    return session.get("role", "Limited Access")


def has_permission(permission):
    perms = ROLE_PERMISSIONS.get(current_role(), set())
    return permission in perms or current_role() == "Full Admin"


def require_permission(permission, api=False):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not login_required():
                if api:
                    return jsonify({"status": "error", "message": "Unauthorized"}), 401
                return redirect(url_for("login"))

            if permission and not has_permission(permission):
                create_alert(
                    "warning",
                    f"Unauthorized permission attempt by {session.get('admin', 'unknown')}: {permission}",
                    "security",
                    {"permission": permission},
                )
                if api:
                    return jsonify({"status": "error", "message": "Forbidden"}), 403
                return redirect(url_for("dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


@app.after_request
def apply_security_headers(response):
    if not ENABLE_SECURITY_HEADERS:
        return response

    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("X-Permitted-Cross-Domain-Policies", "none")
    response.headers.setdefault("Permissions-Policy", "camera=(self), microphone=(), geolocation=(), payment=(), usb=()")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")

    csp_header_name = "Content-Security-Policy" if CSP_ENFORCE else "Content-Security-Policy-Report-Only"
    response.headers.setdefault(csp_header_name, CONTENT_SECURITY_POLICY)

    if request.is_secure:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

    if login_required() and request.endpoint != "static":
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")
        response.headers.setdefault("Expires", "0")

    return response


def now_local():
    return datetime.now()


def now_iso():
    return now_local().isoformat(timespec="seconds")


def normalize_timestamp_value(value):
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if value is None:
        return ""
    return str(value)


def parse_date_or_none(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def contains_regex_filter(value):
    term = (value or "").strip()
    if not term:
        return None
    return {"$regex": re.escape(term), "$options": "i"}


def build_daily_count_series(collection, start_date, end_date, extra_match=None):
    labels = []
    cursor = start_date
    while cursor <= end_date:
        labels.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)

    match_stage = {
        "date": {
            "$gte": start_date.strftime("%Y-%m-%d"),
            "$lte": end_date.strftime("%Y-%m-%d"),
        }
    }
    if isinstance(extra_match, dict):
        match_stage.update(extra_match)

    counts = {
        row.get("_id"): int(row.get("count", 0) or 0)
        for row in collection.aggregate([
            {"$match": match_stage},
            {"$group": {"_id": "$date", "count": {"$sum": 1}}},
        ])
    }
    series = [counts.get(day, 0) for day in labels]
    return labels, series


def normalize_student_doc(student_doc):
    if not student_doc:
        return {}
    doc = dict(student_doc)
    doc["_id"] = str(doc.get("_id", ""))
    lrn_value = normalize_lrn_value(doc.get("lrn") or doc.get("student_id"))
    doc["lrn"] = lrn_value
    doc["student_id"] = lrn_value
    doc["status"] = doc.get("status", "Active") or "Active"
    doc["gender"] = normalize_gender_value(doc.get("gender") or doc.get("sex"))
    doc["grade_level"] = normalize_grade_level(doc.get("grade_level") or doc.get("grade"))
    doc["grade"] = doc["grade_level"]
    doc["sex"] = doc["gender"]
    doc["created_at"] = normalize_timestamp_value(doc.get("created_at"))
    doc["updated_at"] = normalize_timestamp_value(doc.get("updated_at"))
    created_at_text = doc.get("created_at", "")
    doc["created_date"] = created_at_text[:10] if created_at_text else ""
    faces = doc.get("face_data", doc.get("faces"))
    if not isinstance(faces, list):
        faces = []
    doc["faces"] = faces[:5]
    doc["face_data"] = doc["faces"]
    doc["profile_photo"] = doc.get("profile_photo") or (doc["faces"][0] if doc["faces"] else "")
    has_face_payload = bool(doc.get("faces")) or bool(doc.get("face_encodings")) or bool(doc.get("face_embeddings"))
    doc["face_registered"] = bool(doc.get("face_registered")) or has_face_payload
    doc["face_updated_at"] = normalize_timestamp_value(doc.get("face_updated_at"))
    return doc


def normalize_gender_value(value):
    v = normalize_text_value(value).lower()
    if v in {"male", "m"}:
        return "Male"
    if v in {"female", "f"}:
        return "Female"
    return ""


def normalize_grade_level(value):
    if value is None:
        return ""

    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            return ""
        if value == value.to_integral_value():
            value = str(value.quantize(Decimal(1)))
        else:
            value = format(value.normalize(), "f")
    elif isinstance(value, (int, np.integer)):
        value = str(int(value))
    elif isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return ""
        if float(value).is_integer():
            value = str(int(value))
        else:
            try:
                value = format(Decimal(str(value)).normalize(), "f")
            except InvalidOperation:
                value = str(value)

    v = normalize_text_value(value)
    if not v:
        return ""
    lower = v.lower()
    if lower in {"nan", "none", "null", "n/a", "na", "-"}:
        return ""

    grade_number_match = re.fullmatch(r"(7|8|9|10|11|12)(?:\.0+)?", lower)
    if grade_number_match:
        return f"Grade {grade_number_match.group(1)}"

    explicit_grade_match = re.search(r"\bgrade\s*(7|8|9|10|11|12)\b", lower, re.IGNORECASE)
    if explicit_grade_match:
        return f"Grade {explicit_grade_match.group(1)}"

    shorthand_grade_match = re.search(r"\bg\s*(7|8|9|10|11|12)\b", lower, re.IGNORECASE)
    if shorthand_grade_match:
        return f"Grade {shorthand_grade_match.group(1)}"

    combined_grade_match = re.search(r"(?<!\d)(7|8|9|10|11|12)(?!\d)\s*[-/]\s*[A-Za-z]", v)
    if combined_grade_match:
        return f"Grade {combined_grade_match.group(1)}"

    for grade_label in GRADE_LEVEL_OPTIONS:
        if grade_label.lower() == lower:
            return grade_label

    return v


def normalize_text_value(value):
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = "".join(
        (" " if unicodedata.category(ch) == "Zs" else ch)
        for ch in text
        if unicodedata.category(ch) not in {"Cf", "Cc"}
    )
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def build_pagination_payload(page, per_page, total, filters_payload, endpoint):
    total_pages = max((total + per_page - 1) // per_page, 1)
    page = min(max(page, 1), total_pages)

    def build_page_link(page_number):
        params = {**filters_payload, "page": page_number}
        params = {k: v for k, v in params.items() if v not in ("", None)}
        return f"{url_for(endpoint)}?{urlencode(params)}" if params else url_for(endpoint)

    page_start = max(1, page - 2)
    page_end = min(total_pages, page + 2)
    page_numbers = list(range(page_start, page_end + 1))

    return {
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_url": build_page_link(page - 1) if page > 1 else "",
        "next_url": build_page_link(page + 1) if page < total_pages else "",
        "page_numbers": page_numbers,
        "page_links": {p: build_page_link(p) for p in page_numbers},
    }


def session_info_for_time(dt):
    t = dt.time()

    if MORNING_START <= t < NOON_START:
        is_late = t >= MORNING_LATE_THRESHOLD
        return {
            "session": "Morning In",
            "gate_action": "IN",
            "verification_label": "Verified In",
            "status": "Late" if is_late else "Present",
            "display_message": "Verified In - You are Late" if is_late else "Verified In",
            "voice_message": "Verified In, but you are late" if is_late else "Verified In",
        }

    if NOON_START <= t < AFTERNOON_START:
        return {
            "session": "Noon Out",
            "gate_action": "OUT",
            "verification_label": "Verified Out",
            "status": "Present",
            "display_message": "Verified Out",
            "voice_message": "Verified Out",
        }

    if AFTERNOON_START <= t < AFTERNOON_END_START:
        is_late = t >= AFTERNOON_LATE_THRESHOLD
        return {
            "session": "Afternoon In",
            "gate_action": "IN",
            "verification_label": "Verified In",
            "status": "Late" if is_late else "Present",
            "display_message": "Verified In - You are Late" if is_late else "Verified In",
            "voice_message": "Verified In, but you are late" if is_late else "Verified In",
        }

    if t >= AFTERNOON_END_START:
        return {
            "session": "Afternoon Out",
            "gate_action": "OUT",
            "verification_label": "Verified Out",
            "status": "Present",
            "display_message": "Verified Out",
            "voice_message": "Verified Out",
        }

    # Before 5:00 AM fallback
    return {
        "session": "Morning In",
        "gate_action": "IN",
        "verification_label": "Verified In",
        "status": "Present",
        "display_message": "Verified In",
        "voice_message": "Verified In",
    }


def normalize_scan_session_mode(value, default="auto"):
    mode = str(value or "").strip().lower().replace("-", "_")
    if not mode:
        return default
    if mode not in VALID_SCAN_SESSION_MODES:
        raise ValueError("Invalid session mode. Allowed values: auto, manual_in, manual_out.")
    return mode


def scan_session_mode_label(mode):
    normalized = normalize_scan_session_mode(mode)
    labels = {
        "auto": "Automatic (Time-Based)",
        "manual_in": "Manual IN",
        "manual_out": "Manual OUT",
    }
    return labels.get(normalized, "Automatic (Time-Based)")


def get_scan_session_mode():
    with scan_lock:
        return normalize_scan_session_mode(scan_state.get("session_mode", "auto"), default="auto")


def set_scan_session_mode(mode):
    normalized = normalize_scan_session_mode(mode)
    with scan_lock:
        scan_state["session_mode"] = normalized
    return normalized


def session_info_for_mode(dt, mode):
    normalized_mode = normalize_scan_session_mode(mode, default="auto")
    if normalized_mode == "manual_in":
        return {
            "session": "Manual In",
            "gate_action": "IN",
            "verification_label": "Verified In",
            "status": "Present",
            "display_message": "Verified In (Manual)",
            "voice_message": "Verified In",
        }
    if normalized_mode == "manual_out":
        return {
            "session": "Manual Out",
            "gate_action": "OUT",
            "verification_label": "Verified Out",
            "status": "Present",
            "display_message": "Verified Out (Manual)",
            "voice_message": "Verified Out",
        }
    return session_info_for_time(dt)


def resolve_gate_session(dt=None):
    current_dt = dt or now_local()
    mode = get_scan_session_mode()
    session_info = session_info_for_mode(current_dt, mode)
    return {
        **session_info,
        "mode": mode,
        "mode_label": scan_session_mode_label(mode),
    }


def push_scan_event(event_type, payload):
    with scan_lock:
        scan_state["event_counter"] += 1
        event = {
            "id": scan_state["event_counter"],
            "type": event_type,
            "timestamp": now_iso(),
            **payload,
        }
        scan_state["events"].append(event)
        if len(scan_state["events"]) > 300:
            scan_state["events"] = scan_state["events"][-300:]


def create_alert(level, message, category="system", meta=None):
    global alert_revision
    payload = {
        "level": level,
        "message": message,
        "category": category,
        "meta": meta or {},
        "is_read": False,
        "created_at": now_iso(),
    }
    try:
        alerts.insert_one(payload)
        with alert_lock:
            alert_revision += 1
    except Exception as exc:
        print(f"[ERROR] Failed to insert alert: {exc}")


def data_change_snapshot():
    with data_change_lock:
        return {
            "revision": int(data_change_revision),
            "students": int(data_change_domains.get("students", 0)),
            "sections": int(data_change_domains.get("sections", 0)),
            "gate_logs": int(data_change_domains.get("gate_logs", 0)),
            "sms_logs": int(data_change_domains.get("sms_logs", 0)),
        }


def signal_data_change(*domains):
    global data_change_revision
    valid_domains = [domain for domain in domains if domain in data_change_domains]
    if not valid_domains:
        return data_change_snapshot()

    with data_change_lock:
        data_change_revision += 1
        for domain in set(valid_domains):
            data_change_domains[domain] = int(data_change_domains.get(domain, 0)) + 1
        return {
            "revision": int(data_change_revision),
            "students": int(data_change_domains.get("students", 0)),
            "sections": int(data_change_domains.get("sections", 0)),
            "gate_logs": int(data_change_domains.get("gate_logs", 0)),
            "sms_logs": int(data_change_domains.get("sms_logs", 0)),
        }


def sidebar_context(current_page):
    unread = 0
    theme = "light"
    try:
        unread = alerts.count_documents({"is_read": False})
    except Exception:
        unread = 0

    display_user = session.get("admin", "Admin")
    try:
        user_doc, profile = current_user_profile()
        if user_doc and profile:
            display_user = profile.get("fullName") or display_user
            theme = normalize_theme_value(profile.get("theme"))
    except Exception:
        pass

    return {
        "current_page": current_page,
        "current_user": display_user,
        "current_role": session.get("role", "Limited Access"),
        "alerts_unread": unread,
        "current_theme": theme,
    }


def calculate_match_confidence(distance):
    try:
        return round(max(0.0, min(100.0, (1.0 - float(distance)) * 100.0)), 2)
    except Exception:
        return 0.0

def _extract_encodings_from_student(student_doc):
    encs = []
    stored = student_doc.get("face_encodings", student_doc.get("face_embeddings", []))
    if isinstance(stored, list):
        for row in stored:
            if isinstance(row, list) and len(row) == 128:
                try:
                    encs.append(np.array(row, dtype=np.float64))
                except Exception:
                    pass

    if encs:
        return encs

    # Backward compatibility for legacy docs with only image data.
    faces = student_doc.get("face_data", student_doc.get("faces", []))
    if isinstance(faces, list):
        for raw in faces[:5]:
            if not raw or "," not in raw:
                continue
            try:
                img_b64 = raw.split(",", 1)[1]
                img = Image.open(BytesIO(base64.b64decode(img_b64))).convert("RGB")
                np_img = np.array(img)
                face_enc = face_recognition.face_encodings(np_img)
                if face_enc:
                    encs.append(face_enc[0])
            except Exception:
                continue
    return encs


def load_face_index_from_db():
    known_db_encodings = []
    known_db_students = []

    rows = list(students.find({
        "$or": [
            {"status": "Active"},
            {"status": {"$exists": False}},
            {"status": ""},
        ]
    }, {
        "student_id": 1,
        "name": 1,
        "parent_contact": 1,
        "status": 1,
        "face_encodings": 1,
        "face_embeddings": 1,
        "face_data": 1,
        "faces": 1,
    }))

    for row in rows:
        sid = (row.get("student_id") or "").strip()
        name = (row.get("name") or "").strip()
        if not sid or not name:
            continue

        encs = _extract_encodings_from_student(row)
        for enc in encs:
            known_db_encodings.append(enc)
            known_db_students.append({
                "student_id": sid,
                "name": name,
                "parent_contact": row.get("parent_contact", ""),
            })

    return known_db_encodings, known_db_students


def record_login(username, role):
    try:
        login_history.insert_one({
            "username": username,
            "role": role,
            "timestamp": now_iso(),
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
        })
    except Exception as exc:
        print(f"[ERROR] Failed to write login history: {exc}")


def sms_status_filter_values(*statuses):
    values = []
    for status in statuses:
        norm = (status or "").strip()
        if not norm:
            continue
        lower = norm.lower()
        upper = lower.upper()
        if lower not in values:
            values.append(lower)
        if upper not in values:
            values.append(upper)
    return values


def sms_status_mongo_filter(*statuses):
    values = sms_status_filter_values(*statuses)
    if not values:
        return {}
    return {"$in": values}


def log_skipped_sms(student_id="", student_name="", parent_contact="", message="", reason="skipped", sms_type="transactional", metadata=None):
    now = now_local()
    timestamp = now_iso()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    raw_to = str(parent_contact or "").strip()
    normalized_to = ""
    if raw_to:
        try:
            normalized_to = SmsProvider.normalize_phone_number(raw_to)
        except Exception:
            normalized_to = raw_to

    doc = {
        "to": normalized_to or raw_to,
        "message": str(message or "").strip(),
        "type": (sms_type or "transactional").strip().lower() or "transactional",
        "status": "skipped",
        "provider": "PHILSMS",
        "providerMessageId": "",
        "providerResponse": {
            "phase": "skipped",
            "reason": str(reason or "skipped"),
            "meta": metadata if isinstance(metadata, dict) else {},
        },
        "error": "",
        "httpStatus": None,
        "errorCode": "SKIPPED",
        "errorMessage": str(reason or "skipped"),
        "createdAt": timestamp,
        "updatedAt": timestamp,
        # Legacy compatibility fields
        "student_id": (student_id or "").strip(),
        "name": (student_name or "").strip(),
        "parent_contact": raw_to,
        "parent_contact_raw": raw_to,
        "retryEligible": False,
        "retryCount": 0,
        "retryMaxAttempts": 0,
        "nextRetryAt": None,
        "lastRetryError": None,
        "sid": "",
        "timestamp": timestamp,
        "date": date_str,
        "time": time_str,
    }
    try:
        sms_logs.insert_one(doc)
        signal_data_change("sms_logs")
    except Exception as exc:
        print(f"[ERROR] Failed to persist skipped SMS log: {exc}")


def send_sms(to_number, message, sms_type="transactional", metadata=None, student_id="", student_name="", parent_contact="", persist=True):
    now = now_local()
    timestamp = now_iso()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")

    normalized_to = ""
    queued_doc_id = None
    provider = "PHILSMS"
    msg = str(message or "").strip()
    raw_to = str(to_number or "").strip()
    metadata_payload = metadata if isinstance(metadata, dict) else {}
    try:
        retry_count = max(int(metadata_payload.get("retry_count", 0) or 0), 0)
    except Exception:
        retry_count = 0
    try:
        retry_delay_seconds = max(int(os.getenv("SMS_RETRY_DELAY_SECONDS", "300")), 60)
    except Exception:
        retry_delay_seconds = 300
    try:
        retry_max_attempts = max(int(os.getenv("SMS_RETRY_MAX_ATTEMPTS", "3")), 1)
    except Exception:
        retry_max_attempts = 3

    def persist_failed_input(error_message):
        if not persist:
            return
        try:
            sms_logs.insert_one({
                "to": raw_to,
                "message": msg,
                "type": (sms_type or "transactional").strip().lower() or "transactional",
                "status": "failed",
                "provider": provider,
                "providerMessageId": "",
                "providerResponse": {"error": error_message, "phase": "input_validation"},
                "error": error_message,
                "httpStatus": None,
                "errorCode": "INPUT_VALIDATION",
                "errorMessage": error_message,
                "createdAt": timestamp,
                "updatedAt": timestamp,
                # Legacy compatibility fields
                "student_id": (student_id or "").strip(),
                "name": (student_name or "").strip(),
                "parent_contact": (parent_contact or raw_to).strip(),
                "parent_contact_raw": (parent_contact or raw_to).strip(),
                "retryEligible": False,
                "retryCount": retry_count,
                "retryMaxAttempts": retry_max_attempts,
                "nextRetryAt": None,
                "lastRetryError": error_message,
                "sid": "",
                "timestamp": timestamp,
                "date": date_str,
                "time": time_str,
            })
            signal_data_change("sms_logs")
        except Exception as exc:
            print(f"[ERROR] Failed to persist invalid SMS attempt: {exc}")

    try:
        normalized_to = SmsProvider.normalize_phone_number(to_number)
    except Exception as exc:
        err = str(exc)
        persist_failed_input(err)
        return {"status": "failed", "error": err, "sid": "", "provider_message_id": "", "provider_response": {"error": err}}

    if not msg:
        err = "Message is required."
        persist_failed_input(err)
        return {"status": "failed", "error": err, "sid": "", "provider_message_id": "", "provider_response": {"error": err}}

    if persist:
        queued_doc = {
            "to": normalized_to,
            "message": msg,
            "type": (sms_type or "transactional").strip().lower() or "transactional",
            "status": "sending",
            "provider": provider,
            "providerMessageId": "",
            "providerResponse": {"phase": "sending"},
            "error": "",
            "httpStatus": None,
            "errorCode": None,
            "errorMessage": None,
            "createdAt": timestamp,
            "updatedAt": timestamp,
            # Legacy compatibility fields
            "student_id": (student_id or "").strip(),
            "name": (student_name or "").strip(),
            "parent_contact": (parent_contact or raw_to or normalized_to).strip(),
            "parent_contact_raw": (parent_contact or raw_to).strip(),
            "retryEligible": False,
            "retryCount": retry_count,
            "retryMaxAttempts": retry_max_attempts,
            "nextRetryAt": None,
            "lastRetryError": None,
            "sid": "",
            "timestamp": timestamp,
            "date": date_str,
            "time": time_str,
        }
        try:
            queued_doc_id = sms_logs.insert_one(queued_doc).inserted_id
        except Exception as exc:
            print(f"[ERROR] Failed to persist sending SMS log: {exc}")

    try:
        result = sms_provider.send_sms(
            to_number=normalized_to,
            message=msg,
            sms_type=(sms_type or "transactional").strip().lower() or "transactional",
            metadata=metadata_payload,
        )
    except Exception as exc:
        result = {
            "status": "failed",
            "provider": provider,
            "provider_message_id": "",
            "provider_response": {"exception": str(exc), "phase": "provider_send"},
            "http_status": None,
            "to": normalized_to,
            "error": str(exc),
            "error_code": "PROVIDER_CONFIGURATION",
            "error_message": str(exc),
        }

    log_fields = SmsProvider.map_result_to_log_fields(result)
    delivery_status = log_fields["status"]
    provider_message_id = (log_fields.get("providerMessageId") or "").strip()
    provider_response = log_fields.get("providerResponse") or {}
    error = (result.get("error") or "").strip() or (log_fields.get("errorMessage") or "")

    if persist and queued_doc_id:
        update_doc = {
            **log_fields,
            "updatedAt": now_iso(),
        }
        if delivery_status == "failed":
            can_retry = retry_count < retry_max_attempts
            update_doc.update({
                "retryEligible": can_retry,
                "retryCount": retry_count,
                "retryMaxAttempts": retry_max_attempts,
                "nextRetryAt": (now_local() + timedelta(seconds=retry_delay_seconds)).isoformat() if can_retry else None,
                "lastRetryError": error or (log_fields.get("errorMessage") or None),
            })
        else:
            update_doc.update({
                "retryEligible": False,
                "retryCount": retry_count,
                "retryMaxAttempts": retry_max_attempts,
                "nextRetryAt": None,
                "lastRetryError": None,
            })
        try:
            sms_logs.update_one({"_id": queued_doc_id}, {"$set": update_doc})
            signal_data_change("sms_logs")
        except Exception as exc:
            print(f"[ERROR] Failed to update SMS log status: {exc}")

    return {
        "status": delivery_status,
        "sid": provider_message_id,
        "provider_message_id": provider_message_id,
        "provider_response": provider_response,
        "error": error,
        "http_status": result.get("http_status"),
        "error_code": result.get("error_code", ""),
        "error_message": result.get("error_message", ""),
        "to": normalized_to,
        "log_id": str(queued_doc_id) if queued_doc_id else "",
    }


def ensure_default_admin_user():
    try:
        admin = users.find_one({"username": "admin"})
        if not admin:
            created = now_iso()
            users.insert_one({
                "username": "admin",
                "password_hash": hash_password("admin123"),
                "role": "Full Admin",
                "fullName": "System Administrator",
                "email": "admin@chs.local",
                "phone": "",
                "address": "",
                "bio": "",
                "avatarUrl": "",
                "twoFactorEnabled": False,
                "theme": "light",
                "created_at": created,
                "updated_at": created,
                "updatedAt": created,
            })
        else:
            updates = {}
            if "password_hash" not in admin:
                legacy_pwd = admin.get("password", "admin123")
                updates["password_hash"] = hash_password(legacy_pwd)
            if "role" not in admin:
                updates["role"] = "Full Admin"
            if "fullName" not in admin:
                updates["fullName"] = "System Administrator"
            if "email" not in admin:
                updates["email"] = "admin@chs.local"
            if "phone" not in admin:
                updates["phone"] = ""
            if "address" not in admin:
                updates["address"] = ""
            if "bio" not in admin:
                updates["bio"] = ""
            if "avatarUrl" not in admin:
                updates["avatarUrl"] = ""
            if "twoFactorEnabled" not in admin:
                updates["twoFactorEnabled"] = False
            if normalize_theme_value(admin.get("theme"), default="") == "":
                updates["theme"] = "light"
            if "updatedAt" not in admin:
                updates["updatedAt"] = (admin.get("updated_at") or admin.get("created_at") or now_iso())
            if updates:
                users.update_one({"_id": admin["_id"]}, {"$set": updates})
    except Exception as exc:
        print(f"[ERROR] Failed ensuring default admin user: {exc}")


def migrate_plaintext_user_passwords():
    try:
        cursor = users.find({"password": {"$exists": True}})
        for user in cursor:
            if user.get("password_hash"):
                continue
            plain = user.get("password")
            if not plain:
                continue
            users.update_one(
                {"_id": user["_id"]},
                {
                    "$set": {"password_hash": hash_password(plain)},
                    "$unset": {"password": ""},
                },
            )
    except Exception as exc:
        print(f"[ERROR] Failed migrating plaintext passwords: {exc}")


def ensure_user_theme_defaults():
    try:
        users.update_many(
            {
                "$or": [
                    {"theme": {"$exists": False}},
                    {"theme": ""},
                    {"theme": {"$nin": ["light", "dark"]}},
                ]
            },
            {"$set": {"theme": "light"}},
        )
    except Exception as exc:
        print(f"[ERROR] Failed ensuring user theme defaults: {exc}")


def ensure_user_profile_defaults():
    try:
        for doc in users.find({}):
            username = (doc.get("username") or "").strip()
            fallback_name = username or "User"
            fallback_email = f"{username}@chs.local" if username else ""
            updated = (doc.get("updatedAt") or doc.get("updated_at") or doc.get("created_at") or now_iso())

            patch = {}
            if not (doc.get("fullName") or "").strip():
                patch["fullName"] = fallback_name
            if not (doc.get("email") or "").strip():
                patch["email"] = fallback_email
            if "phone" not in doc:
                patch["phone"] = ""
            if "address" not in doc:
                patch["address"] = ""
            if "bio" not in doc:
                patch["bio"] = ""
            if "avatarUrl" not in doc:
                patch["avatarUrl"] = ""
            if "twoFactorEnabled" not in doc:
                patch["twoFactorEnabled"] = False
            elif not isinstance(doc.get("twoFactorEnabled"), bool):
                patch["twoFactorEnabled"] = bool(doc.get("twoFactorEnabled"))
            if "updatedAt" not in doc:
                patch["updatedAt"] = updated
            if patch:
                users.update_one({"_id": doc["_id"]}, {"$set": patch})
    except Exception as exc:
        print(f"[ERROR] Failed ensuring user profile defaults: {exc}")


def maybe_create_absence_alerts():
    today = now_local().date()
    school_days = []
    cursor = today
    while len(school_days) < 7:
        if cursor.weekday() < 5:
            school_days.append(cursor.strftime("%Y-%m-%d"))
        cursor = cursor - timedelta(days=1)

    attendance_map = {}
    for row in attendance_logs.aggregate([
        {"$match": {"date": {"$in": school_days}, "student_id": {"$nin": ["", None]}}},
        {"$group": {"_id": "$student_id", "present_days": {"$addToSet": "$date"}}},
    ]):
        sid = (row.get("_id") or "").strip()
        if sid:
            attendance_map[sid] = set(row.get("present_days") or [])

    for student in students.find({}, {"student_id": 1, "name": 1}):
        sid = (student.get("student_id") or "").strip()
        if not sid:
            continue

        present_days = attendance_map.get(sid, set())

        absences = len(school_days) - len(present_days)
        if absences >= 3:
            alert_key = f"absence-{sid}-{today.isoformat()}"
            exists = alerts.count_documents({"meta.alert_key": alert_key})
            if exists == 0:
                create_alert(
                    level="warning",
                    message=f"{student.get('name', sid)} reached {absences} absences in the last 7 school days.",
                    category="attendance",
                    meta={"student_id": sid, "alert_key": alert_key, "absences": absences},
                )


def _active_student_query(student_id):
    return {
        "student_id": student_id,
        "$or": [
            {"status": "Active"},
            {"status": {"$exists": False}},
            {"status": ""},
        ],
    }


def log_attendance_and_sms(student):
    now = now_local()
    timestamp = now_iso()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    session_info = resolve_gate_session(now)
    status = session_info["status"]
    gate_action = session_info["gate_action"]
    verification_label = session_info["verification_label"]
    session_name = session_info["session"]

    student_id = (student.get("student_id") or "").strip()
    student_name = (student.get("name") or "").strip()
    parent_contact = (student.get("parent_contact") or "").strip()
    if not student_id or not student_name:
        return None

    dedupe_query = {"student_id": student_id, "date": date_str, "session": session_name}

    existing_record = attendance_logs.find_one(dedupe_query)
    if existing_record:
        existing_status = existing_record.get("status", status)
        existing_gate_action = existing_record.get("gate_action", gate_action)
        existing_session = existing_record.get("session", session_name)
        return {
            "student_id": student_id,
            "status": existing_status,
            "sms_status": "skipped",
            "gate_action": existing_gate_action,
            "verification_label": existing_record.get("verification_label", verification_label),
            "session": existing_session,
            "display_message": "Done!",
            "voice_message": "Done!",
            "duplicate": True,
        }

    attendance_doc = {
        "student_id": student_id,
        "student_name": student_name,
        "status": status,
        "session": session_name,
        "source": "gate_scan",
        "timestamp": timestamp,
        "date": date_str,
        "time": time_str,
        "gate_action": gate_action,
        "verification_label": verification_label,
    }
    try:
        attendance_logs.insert_one(attendance_doc)
        signal_data_change("gate_logs")
    except DuplicateKeyError:
        existing_record = attendance_logs.find_one(dedupe_query) or attendance_doc
        return {
            "student_id": student_id,
            "status": existing_record.get("status", status),
            "sms_status": "skipped",
            "gate_action": existing_record.get("gate_action", gate_action),
            "verification_label": existing_record.get("verification_label", verification_label),
            "session": existing_record.get("session", session_name),
            "display_message": "Done!",
            "voice_message": "Done!",
            "duplicate": True,
        }

    sms_status = "skipped"
    sms_error = ""

    if parent_contact:
        movement_text = "entered" if gate_action == "IN" else "exited"
        msg_text = f"CHS Gate Access: {student_name} {movement_text} the gate ({status}) at {time_str} on {date_str}."
        sms_result = send_sms(
            parent_contact,
            msg_text,
            sms_type="transactional",
            metadata={"context": "attendance_gate_scan", "session": session_name},
            student_id=student_id,
            student_name=student_name,
            parent_contact=parent_contact,
        )
        sms_status = "sent" if sms_result.get("status") == "sent" else "failed"
        sms_error = sms_result.get("error", "")

        if sms_status == "failed":
            create_alert(
                level="high",
                message=f"Failed SMS notification for {student_name}.",
                category="sms",
                meta={"student_id": student_id, "error": sms_error},
            )
    else:
        log_skipped_sms(
            student_id=student_id,
            student_name=student_name,
            parent_contact=parent_contact,
            message=f"No parent contact configured for {student_name}. SMS not sent.",
            reason="missing_parent_contact",
            sms_type="transactional",
            metadata={"context": "attendance_gate_scan", "session": session_name},
        )

    return {
        "student_id": student_id,
        "status": status,
        "sms_status": sms_status,
        "gate_action": gate_action,
        "verification_label": verification_label,
        "session": session_name,
        "display_message": session_info["display_message"],
        "voice_message": session_info["voice_message"],
        "duplicate": False,
    }


def handle_verified_student(student, confidence=0.0):
    now_ts = time.time()
    student_id = (student.get("student_id") or "").strip()
    student_name = (student.get("name") or "").strip()
    if not student_id or not student_name:
        return None

    with scan_lock:
        last_ts = float(last_scanned.get(student_id, 0) or 0)
        if now_ts - last_ts < SCAN_COOLDOWN_SECONDS:
            return None
        last_scanned[student_id] = now_ts
    result = log_attendance_and_sms(student)
    if not result:
        return None

    push_scan_event("verified", {
        "name": student_name,
        "verified": True,
        "attendance_status": result["status"],
        "sms_status": result["sms_status"],
        "gate_action": result["gate_action"],
        "verification_label": result["verification_label"],
        "session": result["session"],
        "display_message": result["display_message"],
        "voice_message": result["voice_message"],
        "confidence": confidence,
        "confidence_pct": confidence,
        "duplicate": result["duplicate"],
    })
    return result


def push_not_registered_event(reason="no_match", confidence=0.0):
    now_ts = time.time()
    with scan_lock:
        if now_ts - scan_state["last_not_registered_ts"] < UNREGISTERED_EVENT_COOLDOWN_SECONDS:
            return
        scan_state["last_not_registered_ts"] = now_ts
    push_scan_event("not_registered", {
        "verified": False,
        "message": "Not Registered!",
        "reason": reason,
        "confidence": confidence,
        "confidence_pct": confidence,
    })


def push_multi_face_event(face_count):
    now_ts = time.time()
    with scan_lock:
        if now_ts - scan_state["last_multi_face_ts"] < UNREGISTERED_EVENT_COOLDOWN_SECONDS:
            return
        scan_state["last_multi_face_ts"] = now_ts
    push_scan_event("scan_warning", {
        "verified": False,
        "message": "Multiple faces detected. Please scan one person at a time.",
        "reason": "multiple_faces",
        "face_count": int(face_count or 0),
    })


def start_scan_capture():
    try:
        db_encodings, db_students = load_face_index_from_db()
        model_status = "ready" if db_encodings else "no_registered_students"
    except Exception as exc:
        print(f"[ERROR] Failed loading face index from MongoDB: {exc}")
        db_encodings = []
        db_students = []
        model_status = "model_not_ready"

    with scan_lock:
        if scan_state["active"]:
            return True, "Scan already running"

        capture = cv2.VideoCapture(0)
        if not capture.isOpened():
            create_alert(
                level="critical",
                message="Gate system offline: webcam is unavailable.",
                category="system",
            )
            return False, "Webcam could not be opened"

        last_scanned.clear()
        scan_state["capture"] = capture
        scan_state["active"] = True
        scan_state["known_encodings"] = db_encodings
        scan_state["known_students"] = db_students
        scan_state["model_status"] = model_status
        scan_state["last_not_registered_ts"] = 0.0
        scan_state["last_multi_face_ts"] = 0.0
        return True, "Scan started"


def stop_scan_capture():
    with scan_lock:
        scan_state["active"] = False
        capture = scan_state.get("capture")
        scan_state["capture"] = None
        scan_state["known_encodings"] = []
        scan_state["known_students"] = []
        scan_state["model_status"] = "idle"

    if capture is not None:
        try:
            capture.release()
        except Exception as exc:
            print(f"[WARNING] Failed to release capture cleanly: {exc}")


def generate_frames():
    banner = ""
    banner_until = 0.0

    while True:
        with scan_lock:
            active = scan_state["active"]
            capture = scan_state.get("capture")
            db_encodings = scan_state.get("known_encodings", [])
            db_students = scan_state.get("known_students", [])
            model_status = scan_state.get("model_status", "idle")

        if not active or capture is None:
            break

        ok, frame = capture.read()
        if not ok:
            create_alert(
                level="critical",
                message="Gate system offline: video feed interrupted.",
                category="system",
            )
            break

        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        face_locations = face_recognition.face_locations(rgb_frame)
        if len(face_locations) > 1:
            for (top, right, bottom, left) in face_locations:
                cv2.rectangle(frame, (left, top), (right, bottom), (0, 165, 255), 2)
            push_multi_face_event(len(face_locations))
            banner = "Multiple faces detected. One person at a time."
            banner_until = time.time() + 2.0
        elif len(face_locations) == 1:
            face_encs = face_recognition.face_encodings(rgb_frame, face_locations)
            top, right, bottom, left = face_locations[0]
            label = "Not Registered!"
            color = (0, 64, 255)
            confidence_pct = 0.0

            if not face_encs:
                push_not_registered_event("face_not_encoded", 0.0)
            elif model_status != "ready" or not db_encodings:
                reason = "model_not_ready" if model_status == "model_not_ready" else "no_registered_students"
                push_not_registered_event(reason, 0.0)
            else:
                enc = face_encs[0]
                distances = face_recognition.face_distance(db_encodings, enc)
                if len(distances) > 0:
                    best_idx = int(np.argmin(distances))
                    best_distance = float(distances[best_idx])
                    confidence_pct = calculate_match_confidence(best_distance)
                    is_match = best_distance <= RECOGNITION_TOLERANCE and confidence_pct >= MIN_RECOGNITION_CONFIDENCE

                    if is_match and best_idx < len(db_students):
                        candidate = db_students[best_idx]
                        student_doc = students.find_one(_active_student_query(candidate["student_id"]), {
                            "student_id": 1,
                            "name": 1,
                            "parent_contact": 1,
                        })
                        if student_doc:
                            verification = handle_verified_student(student_doc, confidence_pct)
                            if verification:
                                label = student_doc.get("name", "Verified")
                                color = (46, 204, 113)
                                banner = f"{student_doc.get('name', '')}  |  {verification['display_message']}"
                                banner_until = time.time() + 2.5
                        else:
                            push_not_registered_event("student_not_found", confidence_pct)
                    else:
                        push_not_registered_event("low_confidence", confidence_pct)
                else:
                    push_not_registered_event("no_face_index", 0.0)

            cv2.rectangle(frame, (left, top), (right, bottom), color, 2)
            if label == "Not Registered!" and confidence_pct > 0:
                label_text = f"{label} ({confidence_pct:.1f}%)"
            else:
                label_text = label
            cv2.putText(frame, label_text, (left, max(22, top - 10)), cv2.FONT_HERSHEY_SIMPLEX, 0.62, color, 2)

        if time.time() < banner_until and banner:
            h, w = frame.shape[:2]
            cv2.rectangle(frame, (0, h - 46), (w, h), (16, 124, 85), -1)
            cv2.putText(frame, banner, (14, h - 16), cv2.FONT_HERSHEY_SIMPLEX, 0.72, (255, 255, 255), 2)

        ret, buffer = cv2.imencode(".jpg", frame)
        if not ret:
            continue

        yield (
            b"--frame\r\n"
            b"Content-Type: image/jpeg\r\n\r\n" + buffer.tobytes() + b"\r\n"
        )

    stop_scan_capture()


def compute_dashboard_data(args):
    today_date = now_local().date()
    today = today_date.strftime("%Y-%m-%d")
    total_students = students.count_documents({})
    total_male_students = students.count_documents({"$or": [{"gender": "Male"}, {"sex": "Male"}]})
    total_female_students = students.count_documents({"$or": [{"gender": "Female"}, {"sex": "Female"}]})
    present_today_ids = attendance_logs.distinct("student_id", {"date": today})
    present_today = len([sid for sid in present_today_ids if sid])
    sms_sent_today = sms_logs.count_documents({"date": today, "status": sms_status_mongo_filter("sent")})
    late_today = attendance_logs.count_documents({"date": today, "status": "Late"})
    session_counts = {"Morning In": 0, "Noon Out": 0, "Afternoon In": 0, "Afternoon Out": 0}
    for row in attendance_logs.aggregate([
        {"$match": {"date": today}},
        {"$group": {"_id": "$session", "count": {"$sum": 1}}},
    ]):
        session_name = row.get("_id")
        if session_name in session_counts:
            session_counts[session_name] = row.get("count", 0)

    start_date = today_date - timedelta(days=29)
    labels, gate_series = build_daily_count_series(attendance_logs, start_date, today_date)
    _sms_labels, sms_series = build_daily_count_series(
        sms_logs,
        start_date,
        today_date,
        {"status": sms_status_mongo_filter("sent")},
    )

    attendance_distribution = {
        "present": present_today,
        "absent": max(total_students - present_today, 0),
        "late": late_today,
    }

    unread_alerts = alerts.count_documents({"is_read": False})
    alert_docs = list(alerts.find().sort("created_at", -1).limit(25))
    for a in alert_docs:
        a["_id"] = str(a["_id"])

    users_list = list(users.find({}, {"password": 0, "password_hash": 0}).sort("username", 1))
    for u in users_list:
        u["_id"] = str(u["_id"])

    login_rows = list(login_history.find().sort("timestamp", -1).limit(20))
    for r in login_rows:
        r["_id"] = str(r["_id"])

    q = args.get("q", "").strip()
    log_type = args.get("log_type", "all")
    status_filter = args.get("status", "")
    date_filter = args.get("date", "")
    class_filter = args.get("student_class", "").strip()
    q_regex = contains_regex_filter(q)
    class_regex = contains_regex_filter(class_filter)

    student_filters = []
    if q_regex:
        student_filters.append({
            "$or": [
                {"name": q_regex},
                {"student_id": q_regex},
            ]
        })
    if class_regex:
        student_filters.append({
            "$or": [
                {"grade_level": class_regex},
                {"grade": class_regex},
            ]
        })
    student_query = {"$and": student_filters} if student_filters else {}

    students_result = [normalize_student_doc(s) for s in students.find(student_query).limit(15)]

    gate_query = {}
    if q_regex:
        gate_query["$or"] = [
            {"student_name": q_regex},
            {"student_id": q_regex},
        ]
    if date_filter:
        gate_query["date"] = date_filter
    if status_filter:
        gate_query["status"] = status_filter

    sms_query = {}
    if q_regex:
        sms_query["$or"] = [
            {"name": q_regex},
            {"student_id": q_regex},
        ]
    if date_filter:
        sms_query["date"] = date_filter
    if status_filter:
        sms_query["status"] = sms_status_mongo_filter(status_filter)

    gate_results = []
    sms_results = []
    if log_type in ("all", "gate"):
        gate_results = list(attendance_logs.find(gate_query).sort("timestamp", -1).limit(20))
        for g in gate_results:
            g["_id"] = str(g["_id"])
    if log_type in ("all", "sms"):
        sms_results = list(sms_logs.find(sms_query).sort("timestamp", -1).limit(20))
        for s in sms_results:
            s["_id"] = str(s["_id"])

    return {
        "total_students": total_students,
        "total_male_students": total_male_students,
        "total_female_students": total_female_students,
        "present_today": present_today,
        "sms_sent_today": sms_sent_today,
        "late_today": late_today,
        "session_counts": session_counts,
        "chart_labels": labels,
        "gate_series": gate_series,
        "sms_series": sms_series,
        "attendance_distribution": attendance_distribution,
        "alerts_unread": unread_alerts,
        "alert_rows": alert_docs,
        "users_list": users_list,
        "login_rows": login_rows,
        "search_students": students_result,
        "search_gate_logs": gate_results,
        "search_sms_logs": sms_results,
        "filters": {
            "q": q,
            "log_type": log_type,
            "status": status_filter,
            "date": date_filter,
            "student_class": class_filter,
        },
    }


ensure_default_admin_user()
migrate_plaintext_user_passwords()
ensure_user_theme_defaults()
ensure_user_profile_defaults()


# =====================================
# ROUTES
# =====================================
@app.route("/")
def home():
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remember_me = (request.form.get("remember_me") or "").strip().lower() in {"on", "true", "1", "yes"}

        if not username or not password:
            return render_template(
                "login.html",
                current_year=datetime.now().year,
                error="Username and password are required.",
                entered_username=username,
                remember_me=remember_me,
            )

        user = users.find_one({"username": username})
        if user:
            password_ok = False
            stored_hash = user.get("password_hash")
            if stored_hash:
                password_ok = check_password_hash(stored_hash, password)
            else:
                legacy_plain = user.get("password")
                if legacy_plain and legacy_plain == password:
                    password_ok = True
                    users.update_one(
                        {"_id": user["_id"]},
                        {
                            "$set": {"password_hash": hash_password(password)},
                            "$unset": {"password": ""},
                        },
                    )

            if password_ok:
                role = user.get("role", "Limited Access")
                session.clear()
                session.permanent = remember_me
                session["admin"] = username
                session["role"] = role
                session["theme"] = normalize_theme_value(user.get("theme"))
                record_login(username, role)
                return redirect(post_login_redirect(role))

        return render_template(
            "login.html",
            current_year=datetime.now().year,
            error="Invalid credentials.",
            entered_username=username,
            remember_me=remember_me,
        )

    return render_template("login.html", current_year=datetime.now().year, entered_username="", remember_me=False)


@app.route("/logout")
def logout():
    session.clear()
    stop_scan_capture()
    return redirect(url_for("login"))


@app.route("/dashboard")
@require_permission("dashboard")
def dashboard():
    maybe_create_absence_alerts()
    payload = compute_dashboard_data(request.args)
    payload.update(sidebar_context("dashboard"))
    return render_template("dashboard.html", **payload)


@app.route("/developers")
@require_permission("dashboard")
def developers_page():
    developers = [
        {
            "name": "CORDOVA, APRIL BRYAN C.",
            "role": "Full Stack Developer",
            "contribution": "Worked across frontend and backend modules, integrating core system workflows and feature delivery.",
            "email": "aprilbryan.cordova@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/cordova-april-bryan.png"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-cordova.svg"),
            "links": [{"label": "Email", "url": "mailto:aprilbryan.cordova@chs-gate.local"}],
        },
        {
            "name": "PILONGO, RON ALLEN R.",
            "role": "Backend Developer",
            "contribution": "Implemented and maintained API services, database transactions, and backend integration for key modules.",
            "email": "ronallen.pilongo@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/pilongo-ron-allen.jpg"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-pilongo.svg"),
            "links": [{"label": "Email", "url": "mailto:ronallen.pilongo@chs-gate.local"}],
        },
        {
            "name": "ZAMORA, ANGEL V.",
            "role": "System Developer",
            "contribution": "Supported system design, architecture alignment, and module-level technical implementation.",
            "email": "angel.zamora@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/zamora-angel.jpg"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-zamora.svg"),
            "links": [{"label": "Email", "url": "mailto:angel.zamora@chs-gate.local"}],
        },
        {
            "name": "ANLAP, GIAN EUGENE R.",
            "role": "Project Contributor",
            "contribution": "Contributed to system implementation, testing support, and project development tasks.",
            "email": "gianeugene.anlap@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/anlap-gian-eugene.jpg"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-anlap.svg"),
            "links": [{"label": "Email", "url": "mailto:gianeugene.anlap@chs-gate.local"}],
        },
        {
            "name": "RAMIREZ, ELMER D.",
            "role": "Project Contributor",
            "contribution": "Contributed to system implementation, testing support, and project development tasks.",
            "email": "elmer.ramirez@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/ramirez-elmer.jpg"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-ramirez.svg"),
            "links": [{"label": "Email", "url": "mailto:elmer.ramirez@chs-gate.local"}],
        },
        {
            "name": "CLANZA, ROSME A.",
            "role": "Project Contributor",
            "contribution": "Contributed to system implementation, testing support, and project development tasks.",
            "email": "rosme.clanza@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/clanza-rosme.jpg"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-clanza.svg"),
            "links": [{"label": "Email", "url": "mailto:rosme.clanza@chs-gate.local"}],
        },
        {
            "name": "GELLA, BRENUS C.",
            "role": "Project Contributor",
            "contribution": "Contributed to project implementation and supported interface and usability refinements for the platform.",
            "email": "brenus.gella@chs-gate.local",
            "profile_photo": url_for("static", filename="developer_photos/real/gella-brenus.jpg"),
            "fallback_photo": url_for("static", filename="developer_photos/dev-gella.svg"),
            "links": [{"label": "Email", "url": "mailto:brenus.gella@chs-gate.local"}],
        },
    ]

    return render_template(
        "developers.html",
        developers=developers,
        system_info={
            "version": os.getenv("SYSTEM_VERSION", "v1.0.0"),
            "last_update": os.getenv("SYSTEM_LAST_UPDATE", now_local().strftime("%B %d, %Y")),
            "environment": os.getenv("FLASK_ENV", "production").strip() or "production",
        },
        **sidebar_context("developers"),
    )


@app.route("/api/dashboard/stats")
@require_permission("dashboard", api=True)
def dashboard_stats_api():
    today = now_local().strftime("%Y-%m-%d")
    total_students = students.count_documents({})
    total_male_students = students.count_documents({"$or": [{"gender": "Male"}, {"sex": "Male"}]})
    total_female_students = students.count_documents({"$or": [{"gender": "Female"}, {"sex": "Female"}]})
    present_today_ids = attendance_logs.distinct("student_id", {"date": today})
    present_today = len([sid for sid in present_today_ids if sid])
    sms_sent_today = sms_logs.count_documents({"date": today, "status": sms_status_mongo_filter("sent")})

    return jsonify({
        "status": "ok",
        "total_students": total_students,
        "present_today": present_today,
        "sms_sent_today": sms_sent_today,
        "total_male_students": total_male_students,
        "total_female_students": total_female_students,
    })


@app.route("/api/profile", methods=["GET"])
@require_permission("dashboard", api=True)
def profile_get_api():
    user_doc, profile = current_user_profile()
    if not user_doc or not profile:
        return jsonify({"status": "error", "message": "User session is invalid."}), 401
    return jsonify({"status": "ok", "profile": profile})


@app.route("/api/profile", methods=["PUT"])
@require_permission("dashboard", api=True)
def profile_update_api():
    user_doc, profile = current_user_profile()
    if not user_doc or not profile:
        return jsonify({"status": "error", "message": "User session is invalid."}), 401

    payload = request.get_json(silent=True) or {}
    full_name = sanitize_profile_text(payload.get("fullName"), 120)
    email = sanitize_profile_text(payload.get("email"), 160).lower()
    phone = sanitize_profile_text(payload.get("phone"), 32)
    address = sanitize_profile_text(payload.get("address"), 240)
    bio = sanitize_profile_text(payload.get("bio"), 800, allow_newlines=True)
    two_factor_value = payload.get("twoFactorEnabled", profile.get("twoFactorEnabled"))
    remove_avatar = payload.get("removeAvatar", False)

    if not full_name:
        return jsonify({"status": "error", "message": "Full Name is required.", "field": "fullName"}), 400
    if not email:
        return jsonify({"status": "error", "message": "Email is required.", "field": "email"}), 400
    if not validate_email_format(email):
        return jsonify({"status": "error", "message": "Invalid email format.", "field": "email"}), 400
    if not validate_phone_format(phone):
        return jsonify({"status": "error", "message": "Invalid phone number format.", "field": "phone"}), 400
    if not isinstance(two_factor_value, bool):
        return jsonify({"status": "error", "message": "Two-factor setting must be true or false.", "field": "twoFactorEnabled"}), 400
    if not isinstance(remove_avatar, bool):
        return jsonify({"status": "error", "message": "Avatar remove flag must be true or false.", "field": "removeAvatar"}), 400

    duplicate_email_user = users.find_one({
        "email": email,
        "_id": {"$ne": user_doc["_id"]},
    })
    if duplicate_email_user:
        return jsonify({"status": "error", "message": "Email is already in use.", "field": "email"}), 400

    updated = now_iso()
    update_payload = {
        "fullName": full_name,
        "email": email[:160],
        "phone": phone,
        "address": address,
        "bio": bio,
        "twoFactorEnabled": two_factor_value,
        "updatedAt": updated,
        "updated_at": updated,
    }
    if remove_avatar:
        old_avatar = (user_doc.get("avatarUrl") or "").strip()
        if old_avatar.startswith("/static/avatars/"):
            old_name = old_avatar.split("/static/avatars/", 1)[1]
            old_path = os.path.join(AVATAR_UPLOAD_DIR, old_name)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                except Exception:
                    pass
        update_payload["avatarUrl"] = ""

    users.update_one({"_id": user_doc["_id"]}, {"$set": update_payload})
    refreshed = users.find_one({"_id": user_doc["_id"]})
    return jsonify({"status": "ok", "profile": normalize_profile_user_doc(refreshed)})


@app.route("/api/profile/theme", methods=["GET"])
@require_permission("dashboard", api=True)
def profile_theme_get_api():
    _user_doc, profile = current_user_profile()
    if not profile:
        return jsonify({"status": "error", "message": "User session is invalid."}), 401
    return jsonify({"status": "ok", "theme": normalize_theme_value(profile.get("theme"))})


@app.route("/api/profile/theme", methods=["PUT"])
@require_permission("dashboard", api=True)
def profile_theme_update_api():
    user_doc, profile = current_user_profile()
    if not user_doc or not profile:
        return jsonify({"status": "error", "message": "User session is invalid."}), 401

    payload = request.get_json(silent=True) or {}
    theme = normalize_theme_value(payload.get("theme"), default="")
    if theme not in ("light", "dark"):
        return jsonify({"status": "error", "message": "Theme must be 'light' or 'dark'."}), 400

    updated = now_iso()
    users.update_one({"_id": user_doc["_id"]}, {"$set": {"theme": theme, "updatedAt": updated, "updated_at": updated}})
    session["theme"] = theme
    return jsonify({"status": "ok", "theme": theme})


@app.route("/api/profile/photo", methods=["POST"])
@require_permission("dashboard", api=True)
def profile_photo_upload_api():
    user_doc, profile = current_user_profile()
    if not user_doc or not profile:
        return jsonify({"status": "error", "message": "User session is invalid."}), 401

    file = request.files.get("avatar")
    if not file or not file.filename:
        return jsonify({"status": "error", "message": "No image selected.", "field": "avatar"}), 400

    filename = secure_filename(file.filename)
    if "." not in filename:
        return jsonify({"status": "error", "message": "Invalid file format.", "field": "avatar"}), 400
    ext = filename.rsplit(".", 1)[1].lower()
    if ext not in ALLOWED_AVATAR_EXTENSIONS:
        return jsonify({"status": "error", "message": "Only JPG, PNG, and WEBP are allowed.", "field": "avatar"}), 400

    file.seek(0, os.SEEK_END)
    size = file.tell()
    file.seek(0)
    if size > MAX_AVATAR_SIZE_BYTES:
        return jsonify({"status": "error", "message": "Image size exceeds 5MB limit.", "field": "avatar"}), 400

    # Verify the uploaded payload is a real image, not only extension-based.
    try:
        image_bytes = file.read()
        if not image_bytes:
            return jsonify({"status": "error", "message": "Uploaded image is empty.", "field": "avatar"}), 400
        with Image.open(BytesIO(image_bytes)) as img:
            img.verify()
        file.seek(0)
    except Exception:
        return jsonify({"status": "error", "message": "Uploaded file is not a valid image.", "field": "avatar"}), 400

    unique_name = f"{user_doc.get('username', 'user')}_{uuid.uuid4().hex[:12]}.{ext}"
    save_path = os.path.join(AVATAR_UPLOAD_DIR, unique_name)

    try:
        file.save(save_path)
    except Exception:
        return jsonify({"status": "error", "message": "Failed to save uploaded image.", "field": "avatar"}), 500

    old_avatar = (user_doc.get("avatarUrl") or "").strip()
    if old_avatar.startswith("/static/avatars/"):
        old_name = old_avatar.split("/static/avatars/", 1)[1]
        old_path = os.path.join(AVATAR_UPLOAD_DIR, old_name)
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
            except Exception:
                pass

    avatar_url = f"/static/avatars/{unique_name}"
    updated = now_iso()
    users.update_one(
        {"_id": user_doc["_id"]},
        {"$set": {"avatarUrl": avatar_url, "updatedAt": updated, "updated_at": updated}},
    )
    refreshed = users.find_one({"_id": user_doc["_id"]})

    return jsonify({
        "status": "ok",
        "avatarUrl": avatar_url,
        "profile": normalize_profile_user_doc(refreshed),
    })


@app.route("/api/profile/password", methods=["PUT"])
@require_permission("dashboard", api=True)
def profile_password_update_api():
    user_doc, profile = current_user_profile()
    if not user_doc or not profile:
        return jsonify({"status": "error", "message": "User session is invalid."}), 401

    payload = request.get_json(silent=True) or {}
    current_password = str(payload.get("currentPassword") or "")
    new_password = str(payload.get("newPassword") or "")
    confirm_password = str(payload.get("confirmPassword") or "")

    if not current_password:
        return jsonify({"status": "error", "message": "Current password is required.", "field": "currentPassword"}), 400
    if not new_password:
        return jsonify({"status": "error", "message": "New password is required.", "field": "newPassword"}), 400
    if not confirm_password:
        return jsonify({"status": "error", "message": "Please confirm your new password.", "field": "confirmPassword"}), 400
    if len(new_password) < MIN_PASSWORD_LENGTH or len(new_password) > MAX_PASSWORD_LENGTH:
        return jsonify({
            "status": "error",
            "message": f"Password must be between {MIN_PASSWORD_LENGTH} and {MAX_PASSWORD_LENGTH} characters.",
            "field": "newPassword",
        }), 400
    if new_password != confirm_password:
        return jsonify({"status": "error", "message": "Passwords do not match.", "field": "confirmPassword"}), 400

    checks = [
        bool(re.search(r"[A-Z]", new_password)),
        bool(re.search(r"[a-z]", new_password)),
        bool(re.search(r"[0-9]", new_password)),
        bool(re.search(r"[^A-Za-z0-9]", new_password)),
    ]
    if sum(checks) < 3:
        return jsonify({
            "status": "error",
            "message": "Use at least 3 of: uppercase, lowercase, number, special character.",
            "field": "newPassword",
        }), 400

    stored_hash = user_doc.get("password_hash")
    legacy_plain = user_doc.get("password")
    current_ok = False
    if stored_hash:
        current_ok = check_password_hash(stored_hash, current_password)
    elif legacy_plain and legacy_plain == current_password:
        current_ok = True

    if not current_ok:
        return jsonify({"status": "error", "message": "Current password is incorrect.", "field": "currentPassword"}), 400

    if stored_hash and check_password_hash(stored_hash, new_password):
        return jsonify({"status": "error", "message": "New password must be different from the current password.", "field": "newPassword"}), 400
    if legacy_plain and legacy_plain == new_password:
        return jsonify({"status": "error", "message": "New password must be different from the current password.", "field": "newPassword"}), 400

    updated = now_iso()
    users.update_one(
        {"_id": user_doc["_id"]},
        {
            "$set": {
                "password_hash": hash_password(new_password),
                "updatedAt": updated,
                "updated_at": updated,
            },
            "$unset": {"password": ""},
        },
    )
    refreshed = users.find_one({"_id": user_doc["_id"]})
    return jsonify({"status": "ok", "profile": normalize_profile_user_doc(refreshed)})


# =====================================
# SMS / OTP API
# =====================================
def get_client_ip():
    xff = request.headers.get("X-Forwarded-For", "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "").strip()


def parse_json_payload():
    if request.is_json:
        return request.get_json(silent=True) or {}
    return request.form.to_dict() if request.form else {}


def validate_message_type(value):
    normalized = (value or "transactional").strip().lower()
    if normalized not in {"transactional", "otp"}:
        raise ValueError("Invalid message type. Allowed values: transactional, otp.")
    return normalized


def otp_rate_limit_check(phone, client_ip):
    now = now_local()
    throttle_after = (now - timedelta(seconds=OTP_THROTTLE_SECONDS)).isoformat(timespec="seconds")
    hour_after = (now - timedelta(hours=1)).isoformat(timespec="seconds")

    recent_phone = otp_requests.count_documents({"phone": phone, "createdAt": {"$gte": throttle_after}})
    if recent_phone > 0:
        return False, f"Please wait {OTP_THROTTLE_SECONDS} seconds before requesting another OTP."

    recent_hourly = otp_requests.count_documents({"phone": phone, "createdAt": {"$gte": hour_after}})
    if recent_hourly >= OTP_MAX_PER_HOUR:
        return False, "Hourly OTP limit reached. Please try again later."

    if client_ip:
        recent_ip = otp_requests.count_documents({"requestIp": client_ip, "createdAt": {"$gte": hour_after}})
        if recent_ip >= (OTP_MAX_PER_HOUR * 3):
            return False, "Too many OTP requests from this IP. Please try again later."

    return True, ""


@app.route("/api/sms/send", methods=["POST"])
@require_permission("users_manage", api=True)
def api_sms_send():
    payload = parse_json_payload()
    to_raw = (payload.get("to") or "").strip()
    message = (payload.get("message") or "").strip()
    template = payload.get("template")
    variables = payload.get("variables") if isinstance(payload.get("variables"), dict) else {}

    try:
        sms_type = validate_message_type(payload.get("type", "transactional"))
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400

    if template:
        try:
            message = SmsProvider.render_template(template, variables)
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400

    if not to_raw:
        return jsonify({"status": "error", "message": "Recipient phone number is required."}), 400
    if not message:
        return jsonify({"status": "error", "message": "Message is required."}), 400

    sms_result = send_sms(
        to_raw,
        message,
        sms_type=sms_type,
        metadata={"context": "api_sms_send"},
        parent_contact=to_raw,
    )
    if sms_result.get("status") != "sent":
        return jsonify({
            "status": "error",
            "message": sms_result.get("error", "SMS failed to send."),
            "data": {
                "provider": "PHILSMS",
                "to": sms_result.get("to", ""),
                "providerMessageId": sms_result.get("provider_message_id", ""),
                "logId": sms_result.get("log_id", ""),
            },
        }), 502

    return jsonify({
        "status": "ok",
        "message": "SMS sent successfully.",
        "data": {
            "provider": "PHILSMS",
            "to": sms_result.get("to", ""),
            "providerMessageId": sms_result.get("provider_message_id", ""),
            "logId": sms_result.get("log_id", ""),
        },
    })


@app.route("/api/sms/health", methods=["GET"])
@require_permission("users_manage", api=True)
def api_sms_health():
    health = sms_provider.health_check()
    if health.get("status") == "ok":
        return jsonify({"status": "ok", "data": health})
    return jsonify({"status": "error", "message": health.get("message", "SMS provider unhealthy."), "data": health}), 503


@app.route("/api/sms/auth-check", methods=["GET"])
@require_permission("scan", api=True)
def api_sms_auth_check():
    checker = getattr(sms_provider, "auth_check", None)
    if callable(checker):
        result = checker()
    else:
        result = sms_provider.health_check()

    if result.get("status") == "ok":
        return jsonify({"status": "ok", "data": result})
    return jsonify({
        "status": "error",
        "message": result.get("message", "SMS auth check failed."),
        "data": result,
    }), 503


@app.route("/api/auth/otp/request", methods=["POST"])
def api_otp_request():
    payload = parse_json_payload()
    phone_raw = (payload.get("phone") or "").strip()
    if not phone_raw:
        return jsonify({"status": "error", "message": "Phone number is required."}), 400

    try:
        normalized_phone = SmsProvider.normalize_phone_number(phone_raw)
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400

    client_ip = get_client_ip()
    allowed, reason = otp_rate_limit_check(normalized_phone, client_ip)
    if not allowed:
        return jsonify({"status": "error", "message": reason}), 429

    now = now_local()
    created_at = now_iso()
    expires_at_dt = now + timedelta(minutes=OTP_EXPIRES_MINUTES)
    expires_at = expires_at_dt.isoformat(timespec="seconds")
    otp_code = generate_otp_code(OTP_CODE_LENGTH)
    otp_hash = hash_otp_code(otp_code)

    # Keep only one active OTP per phone.
    otp_requests.update_many(
        {"phone": normalized_phone, "status": "pending"},
        {"$set": {"status": "replaced", "updatedAt": created_at}},
    )

    otp_doc = {
        "phone": normalized_phone,
        "otpHash": otp_hash,
        "expiresAt": expires_at,
        "attempts": 0,
        "verifiedAt": None,
        "status": "pending",
        "requestIp": client_ip,
        "createdAt": created_at,
        "updatedAt": created_at,
        "type": "otp",
    }
    otp_insert = otp_requests.insert_one(otp_doc)
    otp_id = str(otp_insert.inserted_id)

    otp_template = os.getenv(
        "OTP_MESSAGE_TEMPLATE",
        "Your CHS Gate Access OTP is {code}. It expires in {minutes} minutes.",
    )
    message = SmsProvider.render_template(
        otp_template,
        {"code": otp_code, "minutes": OTP_EXPIRES_MINUTES},
    )

    sms_result = send_sms(
        normalized_phone,
        message,
        sms_type="otp",
        metadata={"context": "otp_request", "otpRequestId": otp_id},
        parent_contact=normalized_phone,
    )
    if sms_result.get("status") != "sent":
        otp_requests.update_one(
            {"_id": otp_insert.inserted_id},
            {"$set": {
                "status": "failed",
                "updatedAt": now_iso(),
                "error": sms_result.get("error", "Failed to dispatch OTP SMS."),
            }},
        )
        return jsonify({
            "status": "error",
            "message": "OTP dispatch failed.",
            "error": sms_result.get("error", "Failed to dispatch OTP SMS."),
            "data": {"phone": normalized_phone, "otpRequestId": otp_id},
        }), 502

    return jsonify({
        "status": "ok",
        "message": "OTP sent successfully.",
        "data": {
            "phone": normalized_phone,
            "otpRequestId": otp_id,
            "expiresAt": expires_at,
        },
    })


@app.route("/api/auth/otp/verify", methods=["POST"])
def api_otp_verify():
    payload = parse_json_payload()
    phone_raw = (payload.get("phone") or "").strip()
    otp_code = (payload.get("otp") or "").strip()
    if not phone_raw or not otp_code:
        return jsonify({"status": "error", "message": "Phone number and OTP are required."}), 400

    try:
        normalized_phone = SmsProvider.normalize_phone_number(phone_raw)
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc)}), 400

    otp_record = otp_requests.find_one(
        {"phone": normalized_phone, "status": "pending"},
        sort=[("createdAt", -1)],
    )
    if not otp_record:
        return jsonify({"status": "error", "message": "No active OTP request found."}), 404

    now_ts = now_iso()
    expires_at = (otp_record.get("expiresAt") or "").strip()
    if expires_at and now_ts > expires_at:
        otp_requests.update_one(
            {"_id": otp_record["_id"]},
            {"$set": {"status": "expired", "updatedAt": now_ts}},
        )
        return jsonify({"status": "error", "message": "OTP has expired."}), 400

    attempts = int(otp_record.get("attempts") or 0)
    if attempts >= OTP_MAX_ATTEMPTS:
        otp_requests.update_one(
            {"_id": otp_record["_id"]},
            {"$set": {"status": "locked", "updatedAt": now_ts}},
        )
        return jsonify({"status": "error", "message": "OTP attempts exceeded."}), 429

    if verify_otp_code(otp_record.get("otpHash", ""), otp_code):
        otp_requests.update_one(
            {"_id": otp_record["_id"]},
            {"$set": {"status": "verified", "verifiedAt": now_ts, "updatedAt": now_ts}},
        )
        return jsonify({
            "status": "ok",
            "message": "OTP verified successfully.",
            "data": {"phone": normalized_phone, "verifiedAt": now_ts},
        })

    attempts += 1
    updated_status = "locked" if attempts >= OTP_MAX_ATTEMPTS else "pending"
    otp_requests.update_one(
        {"_id": otp_record["_id"]},
        {"$set": {"attempts": attempts, "status": updated_status, "updatedAt": now_ts}},
    )
    return jsonify({
        "status": "error",
        "message": "Invalid OTP.",
        "data": {"attempts": attempts, "maxAttempts": OTP_MAX_ATTEMPTS},
    }), 400


# =====================================
# SCANNING ROUTES
# =====================================
@app.route("/start_scan", methods=["POST", "GET"])
@require_permission("scan", api=True)
def start_scan():
    requested_mode = None
    if request.method == "POST":
        payload = request_payload()
        requested_mode = payload.get("session_mode") or payload.get("mode")
    if requested_mode is not None:
        try:
            set_scan_session_mode(requested_mode)
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400

    validator = getattr(sms_provider, "validate_configuration", None)
    if callable(validator):
        sms_status = validator(raise_on_error=False)
    else:
        sms_status = sms_provider.health_check()
    sms_warning = ""
    if sms_status.get("status") != "ok":
        sms_warning = sms_status.get("message", "SMS provider is unavailable. Scanning will continue without SMS delivery.")
        print(f"[WARNING] SMS provider not ready during scan start: {sms_warning}")

    ok, message = start_scan_capture()
    if not ok:
        payload = {"status": "failed", "message": message, "sms_auth": sms_status}
        if sms_warning:
            payload["sms_warning"] = sms_warning
        return jsonify(payload), 500

    with scan_lock:
        model_status = scan_state.get("model_status", "idle")
        registered_faces = len(scan_state.get("known_encodings", []))
        session_mode = normalize_scan_session_mode(scan_state.get("session_mode", "auto"), default="auto")
    effective_session = resolve_gate_session(now_local())
    payload = {
        "status": "ok",
        "message": message,
        "model_status": model_status,
        "registered_faces": registered_faces,
        "sms_auth": sms_status,
        "scan_session_mode": session_mode,
        "session_mode_label": scan_session_mode_label(session_mode),
        "effective_session": {
            "session": effective_session.get("session", ""),
            "gate_action": effective_session.get("gate_action", ""),
            "verification_label": effective_session.get("verification_label", ""),
            "status": effective_session.get("status", ""),
            "display_message": effective_session.get("display_message", ""),
            "voice_message": effective_session.get("voice_message", ""),
        },
    }
    if sms_warning:
        payload["sms_warning"] = sms_warning
    return jsonify(payload)


@app.route("/stop_scan", methods=["POST", "GET"])
@require_permission("scan", api=True)
def stop_scan():
    stop_scan_capture()
    return jsonify({"status": "ok", "message": "Scan stopped"})


@app.route("/video_feed")
@require_permission("scan", api=True)
def video_feed():
    return Response(generate_frames(), mimetype="multipart/x-mixed-replace; boundary=frame")


@app.route("/scan_events")
@require_permission("scan", api=True)
def scan_events():
    try:
        since = max(int(request.args.get("since", "0")), 0)
    except (TypeError, ValueError):
        since = 0
    with scan_lock:
        events = [e for e in scan_state["events"] if e["id"] > since]
        active = scan_state["active"]
        session_mode = normalize_scan_session_mode(scan_state.get("session_mode", "auto"), default="auto")
    effective_session = resolve_gate_session(now_local())
    return jsonify({
        "events": events,
        "active": active,
        "scan_session_mode": session_mode,
        "session_mode_label": scan_session_mode_label(session_mode),
        "effective_session": {
            "session": effective_session.get("session", ""),
            "gate_action": effective_session.get("gate_action", ""),
            "verification_label": effective_session.get("verification_label", ""),
            "status": effective_session.get("status", ""),
        },
    })


@app.route("/api/scan/session-mode", methods=["GET", "POST"])
@require_permission("scan", api=True)
def api_scan_session_mode():
    if request.method == "POST":
        payload = request_payload()
        requested_mode = payload.get("mode") or payload.get("session_mode")
        try:
            mode = set_scan_session_mode(requested_mode)
        except ValueError as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400
    else:
        mode = get_scan_session_mode()

    with scan_lock:
        active = bool(scan_state.get("active"))

    effective_session = resolve_gate_session(now_local())
    return jsonify({
        "status": "ok",
        "mode": mode,
        "mode_label": scan_session_mode_label(mode),
        "active": active,
        "effective_session": {
            "session": effective_session.get("session", ""),
            "gate_action": effective_session.get("gate_action", ""),
            "verification_label": effective_session.get("verification_label", ""),
            "status": effective_session.get("status", ""),
            "display_message": effective_session.get("display_message", ""),
            "voice_message": effective_session.get("voice_message", ""),
        },
    })


# =====================================
# ALERT ROUTES
# =====================================
@app.route("/alerts/mark-read", methods=["POST"])
@require_permission("alerts_manage", api=True)
def mark_alerts_read():
    global alert_revision
    data = request.get_json(silent=True) or {}
    if data.get("all"):
        alerts.update_many({"is_read": False}, {"$set": {"is_read": True}})
        with alert_lock:
            alert_revision += 1
        return jsonify({"status": "ok"})

    ids = data.get("ids", [])
    object_ids = []
    for i in ids:
        try:
            object_ids.append(ObjectId(i))
        except Exception:
            pass

    if object_ids:
        alerts.update_many({"_id": {"$in": object_ids}}, {"$set": {"is_read": True}})
        with alert_lock:
            alert_revision += 1
    return jsonify({"status": "ok"})


@app.route("/alerts/unread-count")
@require_permission("alerts_manage", api=True)
def unread_alert_count():
    unread = alerts.count_documents({"is_read": False})
    return jsonify({"unread": unread})


@app.route("/alerts/stream")
@require_permission("alerts_manage", api=True)
def alerts_stream():
    def generate():
        last_seen = -1
        while True:
            try:
                with alert_lock:
                    current_rev = alert_revision

                if current_rev != last_seen:
                    unread = alerts.count_documents({"is_read": False})
                    payload = json.dumps({"revision": current_rev, "unread": unread})
                    yield f"event: alerts\ndata: {payload}\n\n"
                    last_seen = current_rev
                else:
                    # keep-alive for intermediaries/proxies
                    yield ": keep-alive\n\n"

                time.sleep(1.5)
            except GeneratorExit:
                break
            except Exception:
                time.sleep(2)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/changes/stream")
@require_permission("dashboard", api=True)
def data_changes_stream():
    def generate():
        last_seen = -1
        while True:
            try:
                snapshot = data_change_snapshot()
                current_rev = int(snapshot.get("revision", 0))
                if current_rev != last_seen:
                    payload = json.dumps({**snapshot, "server_time": now_iso()})
                    yield f"event: data_change\ndata: {payload}\n\n"
                    last_seen = current_rev
                else:
                    yield ": keep-alive\n\n"
                time.sleep(1.5)
            except GeneratorExit:
                break
            except Exception:
                time.sleep(2)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# =====================================
# USER MANAGEMENT
# =====================================
@app.route("/admin/users/add", methods=["POST"])
@require_permission("users_manage")
def add_user():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    role = request.form.get("role", "Limited Access")
    if role not in ROLE_PERMISSIONS:
        role = "Limited Access"

    if not username or not password:
        return redirect(url_for("dashboard"))

    if users.count_documents({"username": username}) > 0:
        create_alert("warning", f"User creation skipped: {username} already exists.", "system")
        return redirect(url_for("dashboard"))

    created = now_iso()
    users.insert_one({
        "username": username,
        "password_hash": hash_password(password),
        "role": role,
        "fullName": username,
        "email": f"{username}@chs.local",
        "phone": "",
        "address": "",
        "bio": "",
        "avatarUrl": "",
        "twoFactorEnabled": False,
        "theme": "light",
        "created_at": created,
        "updated_at": created,
        "updatedAt": created,
    })
    create_alert("info", f"New user '{username}' added with role {role}.", "system")
    return redirect(url_for("dashboard"))


# =====================================
# STUDENTS CRUD
# =====================================
def api_success(payload=None, status_code=200):
    body = {"status": "ok"}
    if isinstance(payload, dict):
        body.update(payload)
    return jsonify(body), status_code


def api_error(message, status_code=400, field=None):
    body = {"status": "error", "message": message}
    if field:
        body["field"] = field
    return jsonify(body), status_code


def request_payload():
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict(flat=True)
    return data if isinstance(data, dict) else {}


def parse_student_oid(raw_id):
    try:
        return ObjectId(raw_id)
    except Exception:
        return None


def extract_grade_number(value):
    grade_label = normalize_grade_level(value)
    if not grade_label:
        return ""
    lower = grade_label.lower()
    if lower.startswith("grade "):
        return grade_label.split(" ", 1)[1].strip()
    if grade_label.isdigit():
        return grade_label
    match = re.search(r"\d+", grade_label)
    return match.group(0) if match else grade_label


def resolve_student_grade_and_section(grade_value, section_value):
    section_clean = normalize_section_value(section_value)
    if not section_clean:
        return "", "", "Section is required.", "section"

    predefined = PREDEFINED_SECTION_LOOKUP.get(section_clean.lower())
    if predefined:
        return predefined["grade_level"], predefined["section"], "", ""

    grade_level = normalize_grade_level(grade_value)
    section_normalized = section_clean.lower()

    if not grade_level:
        matches = list(sections.find(
            {"section_normalized": section_normalized},
            {"grade_level": 1, "grade_key": 1, "section": 1},
        ).limit(2))
        if len(matches) == 1:
            inferred_grade = normalize_grade_level(matches[0].get("grade_level") or matches[0].get("grade_key"))
            if inferred_grade:
                grade_level = inferred_grade
                inferred_section = normalize_section_value(matches[0].get("section"))
                if inferred_section:
                    section_clean = inferred_section

    if not grade_level:
        return "", "", "Grade level is required.", "grade_level"

    grade_key = extract_grade_number(grade_level)
    if grade_key:
        existing = sections.find_one(
            {"grade_key": str(grade_key), "section_normalized": section_normalized},
            {"section": 1},
        )
        existing_section = normalize_section_value(existing.get("section")) if existing else ""
        if existing_section:
            section_clean = existing_section

    return grade_level, section_clean, "", ""


def build_grade_filter(grade_value):
    grade_level = normalize_grade_level(grade_value)
    if not grade_level:
        return None, ""

    grade_number = extract_grade_number(grade_level)
    grade_candidates = [grade_level]
    if grade_number:
        if grade_number not in grade_candidates:
            grade_candidates.append(grade_number)
        grade_prefixed = f"Grade {grade_number}"
        if grade_prefixed not in grade_candidates:
            grade_candidates.append(grade_prefixed)

    return {
        "$or": [
            {"grade_level": {"$in": grade_candidates}},
            {"grade": {"$in": grade_candidates}},
        ]
    }, grade_level


def build_students_query(q_value="", grade_value="", section_value=""):
    q_text = (q_value or "").strip()
    section_text = (section_value or "").strip()

    clauses = []
    if q_text:
        q_regex = contains_regex_filter(q_text)
        clauses.append({
            "$or": [
                {"name": q_regex},
                {"lrn": q_regex},
                {"student_id": q_regex},
                {"section": q_regex},
            ]
        })

    grade_clause, grade_level = build_grade_filter(grade_value)
    if grade_clause:
        clauses.append(grade_clause)
    else:
        grade_level = ""

    if section_text:
        clauses.append({"section": section_text})

    query = {"$and": clauses} if clauses else {}
    return query, q_text, grade_level, section_text


def grade_sort_key(raw_key):
    key = str(raw_key)
    return (0, int(key)) if key.isdigit() else (1, key.lower())


def build_sections_by_grade(grade_filter=""):
    grade_clause, _normalized_grade = build_grade_filter(grade_filter)
    query = grade_clause if grade_clause else {}

    sections_by_grade = {}
    projection = {"grade_level": 1, "grade": 1, "section": 1}
    for row in students.find(query, projection):
        grade_label = normalize_grade_level(row.get("grade_level") or row.get("grade"))
        grade_key = extract_grade_number(grade_label)
        section_value = normalize_section_value(row.get("section"))
        if not grade_key or not section_value:
            continue
        if grade_key not in sections_by_grade:
            sections_by_grade[grade_key] = set()
        sections_by_grade[grade_key].add(section_value)

    manual_query = {}
    manual_grade = extract_grade_number(grade_filter)
    if manual_grade:
        manual_query["grade_key"] = str(manual_grade)

    for row in sections.find(manual_query, {"grade_key": 1, "grade_level": 1, "section": 1}):
        grade_key = str(row.get("grade_key") or extract_grade_number(row.get("grade_level")))
        section_value = normalize_section_value(row.get("section"))
        if not grade_key or not section_value:
            continue
        if grade_key not in sections_by_grade:
            sections_by_grade[grade_key] = set()
        sections_by_grade[grade_key].add(section_value)

    ordered = {}
    for grade_key in sorted(sections_by_grade.keys(), key=grade_sort_key):
        ordered[grade_key] = sorted(sections_by_grade[grade_key], key=str.lower)
    return ordered


def upsert_manual_section(grade_value, section_value, created_by=""):
    grade_level = normalize_grade_level(grade_value)
    if not grade_level:
        raise ValueError("Grade level is required.")

    grade_key = extract_grade_number(grade_level)
    if not grade_key:
        raise ValueError("Invalid grade level.")

    section_clean = normalize_section_value(section_value)
    if not section_clean:
        raise ValueError("Section is required.")

    section_normalized = section_clean.lower()
    query = {"grade_key": str(grade_key), "section_normalized": section_normalized}
    existing = sections.find_one(query, {"_id": 1, "grade_level": 1, "section": 1})

    if existing:
        existing_section = normalize_section_value(existing.get("section"))
        existing_grade = str(existing.get("grade_level") or "").strip()
        if existing_section != section_clean or existing_grade != grade_level:
            sections.update_one(
                {"_id": existing["_id"]},
                {"$set": {"grade_level": grade_level, "section": section_clean, "updated_at": now_iso()}},
            )
            signal_data_change("sections")
        return {
            "grade_key": str(grade_key),
            "grade_level": grade_level,
            "section": section_clean,
        }

    now_value = now_iso()
    sections.insert_one({
        "grade_key": str(grade_key),
        "grade_level": grade_level,
        "section": section_clean,
        "section_normalized": section_normalized,
        "created_at": now_value,
        "updated_at": now_value,
        "created_by": (created_by or "").strip(),
    })
    signal_data_change("sections")
    return {
        "grade_key": str(grade_key),
        "grade_level": grade_level,
        "section": section_clean,
    }


def ensure_predefined_sections():
    for grade_level, section_values in PREDEFINED_SECTIONS_BY_GRADE.items():
        for section_name in section_values:
            try:
                upsert_manual_section(grade_level, section_name, created_by="system")
            except Exception as exc:
                print(f"[WARNING] Could not upsert predefined section {grade_level} - {section_name}: {exc}")


def build_lrn_duplicate_query(lrn_value, exclude_oid=None):
    query = {
        "$or": [
            {"lrn": lrn_value},
            {"student_id": lrn_value},
        ]
    }
    if exclude_oid is not None:
        query["_id"] = {"$ne": exclude_oid}
    return query


def normalize_student_name_value(value):
    cleaned = re.sub(r"\s+", " ", str(value or "")).strip()
    return cleaned[:120]


def normalize_student_status(value, default="Active"):
    status_value = str(value or default).strip()
    return "Inactive" if status_value == "Inactive" else "Active"


def sanitize_personal_student_payload(data, existing_doc=None):
    current = existing_doc or {}
    lrn_input = data.get("lrn", data.get("student_id", current.get("lrn", current.get("student_id", ""))))
    lrn, lrn_error = validate_lrn_value(lrn_input)
    student_name = normalize_student_name_value(data.get("name", current.get("name", "")))
    resolved_grade, resolved_section, grade_section_error, grade_section_field = resolve_student_grade_and_section(
        data.get("grade_level") or data.get("grade") or current.get("grade_level") or current.get("grade"),
        data.get("section", current.get("section", "")),
    )
    parent_contact_raw = str(data.get("parent_contact", current.get("parent_contact", "")) or "").strip()
    gender = normalize_gender_value(data.get("gender") or data.get("sex") or current.get("gender") or current.get("sex"))
    status = normalize_student_status(data.get("status", current.get("status", "Active")), default=current.get("status", "Active"))

    if lrn_error:
        return None, lrn_error, "lrn"
    if not student_name:
        return None, "Name is required.", "name"
    if grade_section_error:
        return None, grade_section_error, grade_section_field
    if not gender:
        return None, "Sex/Gender is required.", "gender"
    try:
        parent_contact = normalize_parent_contact_value(parent_contact_raw)
    except ValueError as exc:
        return None, str(exc), "parent_contact"

    return {
        "lrn": lrn,
        "student_id": lrn,
        "name": student_name,
        "grade_level": resolved_grade,
        "grade": resolved_grade,
        "section": resolved_section,
        "parent_contact": parent_contact,
        "gender": gender,
        "sex": gender,
        "status": status,
    }, "", ""


def build_new_student_document(student_data):
    now_value = now_iso()
    payload = dict(student_data or {})
    payload.update({
        "face_registered": False,
        "face_updated_at": None,
        "face_data": [],
        "faces": [],
        "face_encodings": [],
        "face_embeddings": [],
        "profile_photo": "",
        "created_at": now_value,
        "updated_at": now_value,
    })
    return payload


def parse_faces_payload(data):
    raw_faces = data.get("faces", data.get("face_data", []))
    if isinstance(raw_faces, str):
        try:
            raw_faces = json.loads(raw_faces)
        except Exception:
            raw_faces = []

    if not isinstance(raw_faces, list):
        return None, None, "Face data must be an array.", "faces"

    faces_array = []
    for raw_face in raw_faces:
        if not isinstance(raw_face, str):
            continue
        raw = raw_face.strip()
        if raw and "," in raw:
            faces_array.append(raw)
        if len(faces_array) >= 5:
            break

    if not faces_array:
        return None, None, "Capture at least one face image.", "faces"
    if len(faces_array) < 5:
        return None, None, "Capture all required angles (Front, Left, Right, Slight Up, Slight Down).", "faces"

    face_encodings = []
    for raw_face in faces_array:
        try:
            img_b64 = raw_face.split(",", 1)[1]
            img_bytes = base64.b64decode(img_b64)
            img = Image.open(BytesIO(img_bytes)).convert("RGB")
            img_np = np.array(img)
            enc_rows = face_recognition.face_encodings(img_np)
            if enc_rows:
                face_encodings.append(enc_rows[0].tolist())
        except Exception as exc:
            print(f"[WARNING] Face encoding skipped: {exc}")

    if not face_encodings:
        return None, None, "No detectable face found in the uploaded captures.", "faces"

    return faces_array, face_encodings, "", ""


def refresh_scan_face_index_if_active():
    with scan_lock:
        is_active = bool(scan_state.get("active"))

    if not is_active:
        return

    try:
        db_encodings, db_students = load_face_index_from_db()
        model_status = "ready" if db_encodings else "no_registered_students"
    except Exception as exc:
        print(f"[WARNING] Could not refresh scan face index: {exc}")
        db_encodings = []
        db_students = []
        model_status = "model_not_ready"

    with scan_lock:
        scan_state["known_encodings"] = db_encodings
        scan_state["known_students"] = db_students
        scan_state["model_status"] = model_status


def ensure_student_lrn_defaults():
    try:
        cursor = students.find({}, {"lrn": 1, "student_id": 1})
        for row in cursor:
            normalized_lrn = normalize_lrn_value(row.get("lrn") or row.get("student_id"))
            if not normalized_lrn:
                continue

            current_lrn = normalize_lrn_value(row.get("lrn"))
            current_student_id = normalize_lrn_value(row.get("student_id"))
            patch = {}
            if current_lrn != normalized_lrn:
                patch["lrn"] = normalized_lrn
            if current_student_id != normalized_lrn:
                patch["student_id"] = normalized_lrn
            if patch:
                try:
                    students.update_one({"_id": row["_id"]}, {"$set": patch})
                except DuplicateKeyError:
                    print(f"[WARNING] Skipped duplicate LRN during startup backfill: {normalized_lrn}")
    except Exception as exc:
        print(f"[WARNING] Could not backfill student LRN values: {exc}")


def ensure_student_face_defaults():
    try:
        students.update_many(
            {
                "face_registered": {"$exists": False},
                "$or": [
                    {"face_data.0": {"$exists": True}},
                    {"faces.0": {"$exists": True}},
                    {"face_encodings.0": {"$exists": True}},
                    {"face_embeddings.0": {"$exists": True}},
                ],
            },
            {"$set": {"face_registered": True}},
        )
        students.update_many(
            {"face_registered": {"$exists": False}},
            {"$set": {"face_registered": False}},
        )
    except Exception as exc:
        print(f"[WARNING] Could not ensure face_registered defaults: {exc}")


ensure_predefined_sections()
ensure_student_lrn_defaults()
ensure_student_face_defaults()


def build_students_stats_payload():
    active_count = students.count_documents({
        "$or": [
            {"status": "Active"},
            {"status": {"$exists": False}},
            {"status": ""},
        ]
    })
    inactive_count = students.count_documents({"status": "Inactive"})
    today_prefix = now_local().strftime("%Y-%m-%d")
    new_today_count = students.count_documents({
        "created_at": {
            "$gte": f"{today_prefix}T00:00:00",
            "$lte": f"{today_prefix}T23:59:59",
        }
    })
    return {
        "total": students.count_documents({}),
        "active": active_count,
        "inactive": inactive_count,
        "new_today": new_today_count,
    }


@app.route("/students", methods=["GET"])
@require_permission("students_read")
def students_page():
    stats_payload = build_students_stats_payload()

    return render_template(
        "students.html",
        message=request.args.get("message", "").strip(),
        message_type=request.args.get("message_type", "success").strip() or "success",
        stats=stats_payload,
        grade_options=list(GRADE_LEVEL_OPTIONS),
        **sidebar_context("students"),
    )


@app.route("/api/students/stats", methods=["GET"])
@require_permission("students_read", api=True)
def api_students_stats():
    return api_success({"stats": build_students_stats_payload()})


@app.route("/api/students", methods=["GET", "POST"])
@require_permission("students_read", api=True)
def api_students_collection():
    if request.method == "GET":
        query, q_value, grade_level, section_value = build_students_query(
            request.args.get("q", ""),
            request.args.get("grade", "") or request.args.get("grade_level", ""),
            request.args.get("section", ""),
        )
        try:
            limit = int(request.args.get("limit", "10"))
        except (TypeError, ValueError):
            limit = 10
        try:
            page = int(request.args.get("page", "1"))
        except (TypeError, ValueError):
            page = 1

        limit = min(max(limit, 1), 100)
        page = max(page, 1)
        skip = (page - 1) * limit

        total = students.count_documents(query)
        rows = students.find(query).sort([("created_at", -1), ("name", 1)]).skip(skip).limit(limit)
        payload = {
            "students": [normalize_student_doc(row) for row in rows],
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit if total else 1,
            "filters": {
                "q": q_value,
                "grade": grade_level,
                "section": section_value,
            },
        }
        return api_success(payload)

    if not has_permission("students_write"):
        return api_error("Forbidden", 403)

    payload = request_payload()
    student_data, err_message, err_field = sanitize_personal_student_payload(payload)
    if err_message:
        return api_error(err_message, 400, err_field)

    if students.count_documents(build_lrn_duplicate_query(student_data["lrn"])) > 0:
        return api_error("LRN already exists.", 400, "lrn")

    try:
        upsert_manual_section(
            student_data.get("grade_level", ""),
            student_data.get("section", ""),
            created_by=session.get("admin", ""),
        )
    except ValueError as exc:
        return api_error(str(exc), 400, "section")

    student_data = build_new_student_document(student_data)

    try:
        inserted = students.insert_one(student_data)
    except DuplicateKeyError:
        return api_error("LRN already exists.", 400, "lrn")
    signal_data_change("students")
    saved = students.find_one({"_id": inserted.inserted_id})
    return api_success({
        "message": "Student created successfully.",
        "student": normalize_student_doc(saved),
    }, 201)


@app.route("/api/students/import", methods=["POST"])
@require_permission("students_write", api=True)
def api_students_import():
    upload = request.files.get("file")
    if upload is None or not str(upload.filename or "").strip():
        return api_error("Excel file is required.", 400, "file")

    filename = secure_filename(upload.filename or "")
    extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if extension not in STUDENT_IMPORT_ALLOWED_EXTENSIONS:
        return api_error("Only .xlsx files are supported.", 400, "file")

    file_bytes = upload.read()
    try:
        parsed_rows = parse_student_import_workbook(file_bytes)
    except ValueError as exc:
        return api_error(str(exc), 400, "file")

    total_rows_read = len(parsed_rows)
    default_grade_level = normalize_grade_level(request.form.get("default_grade_level", ""))
    default_section = normalize_section_value(request.form.get("default_section", ""))

    validation_errors = []
    pending_rows = []
    summary_skipped_count = 0
    for row in parsed_rows:
        if is_student_import_summary_row(row):
            summary_skipped_count += 1
            continue

        row_grade_level = row.get("grade_level", "") or default_grade_level
        row_section = row.get("section", "") or default_section
        student_data, err_message, _err_field = sanitize_personal_student_payload({
            "lrn": row.get("lrn", ""),
            "name": row.get("name", ""),
            "grade_level": row_grade_level,
            "gender": row.get("gender", ""),
            "section": row_section,
            "status": "Active",
        })
        if err_message:
            validation_errors.append(f"Row {row.get('row_number', '?')}: {err_message}")
            continue
        pending_rows.append({
            "row_number": row.get("row_number", ""),
            "student_data": student_data,
        })

    candidate_lrns = sorted({item["student_data"]["lrn"] for item in pending_rows if item.get("student_data")})
    existing_lrns = set()
    if candidate_lrns:
        for row in students.find(
            {"$or": [{"lrn": {"$in": candidate_lrns}}, {"student_id": {"$in": candidate_lrns}}]},
            {"lrn": 1, "student_id": 1},
        ):
            existing_lrn = normalize_lrn_value(row.get("lrn") or row.get("student_id"))
            if existing_lrn:
                existing_lrns.add(existing_lrn)

    imported_count = 0
    duplicate_count = 0
    in_file_seen_lrns = set()

    for item in pending_rows:
        row_number = item.get("row_number", "")
        student_data = dict(item.get("student_data") or {})
        lrn_value = student_data.get("lrn", "")
        if not lrn_value:
            validation_errors.append(f"Row {row_number}: LRN is required.")
            continue
        if lrn_value in in_file_seen_lrns:
            duplicate_count += 1
            validation_errors.append(f"Row {row_number}: Duplicate LRN in uploaded file ({lrn_value}).")
            continue
        if lrn_value in existing_lrns:
            duplicate_count += 1
            validation_errors.append(f"Row {row_number}: LRN already exists ({lrn_value}).")
            continue

        try:
            upsert_manual_section(
                student_data.get("grade_level", ""),
                student_data.get("section", ""),
                created_by=session.get("admin", ""),
            )
        except ValueError as exc:
            validation_errors.append(f"Row {row_number}: {exc}")
            continue

        student_doc = build_new_student_document(student_data)
        try:
            students.insert_one(student_doc)
        except DuplicateKeyError:
            duplicate_count += 1
            validation_errors.append(f"Row {row_number}: LRN already exists ({lrn_value}).")
            continue

        imported_count += 1
        in_file_seen_lrns.add(lrn_value)

    if imported_count > 0:
        signal_data_change("students")

    error_count = len(validation_errors)
    invalid_count = max(error_count - duplicate_count, 0)
    skipped_count = max(total_rows_read - imported_count, 0)
    message = (
        f"Import completed. Rows read: {total_rows_read}. "
        f"Imported: {imported_count}. "
        f"Skipped/failed: {skipped_count}."
    )
    if skipped_count > 0:
        message = (
            f"{message} Duplicates: {duplicate_count}. "
            f"Invalid: {invalid_count}. "
            f"Summary rows skipped: {summary_skipped_count}."
        )

    response_payload = {
        "message": message,
        "total_rows_read": total_rows_read,
        "imported_count": imported_count,
        "skipped_count": skipped_count,
        "duplicate_count": duplicate_count,
        "invalid_count": invalid_count,
        "summary_skipped_count": summary_skipped_count,
        "error_count": error_count,
        "errors": validation_errors[:100],
    }

    if imported_count == 0:
        return api_success(response_payload, 200)
    return api_success(response_payload, 201)


@app.route("/api/students/import/template", methods=["GET"])
@require_permission("students_read", api=True)
def api_students_import_template():
    try:
        template_bytes = build_student_import_template_bytes()
    except ValueError as exc:
        return api_error(str(exc), 500, "file")

    filename = f"student_import_template_{now_local().strftime('%Y%m%d')}.xlsx"
    return Response(
        template_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/api/students/<id>", methods=["GET", "PUT", "DELETE"])
@require_permission("students_read", api=True)
def api_students_item(id):
    student_oid = parse_student_oid(id)
    if not student_oid:
        return api_error("Invalid student id.", 400, "id")

    if request.method == "GET":
        student_doc = students.find_one({"_id": student_oid})
        if not student_doc:
            return api_error("Student not found.", 404)
        return api_success({"student": normalize_student_doc(student_doc)})

    if not has_permission("students_write"):
        return api_error("Forbidden", 403)

    if request.method == "DELETE":
        result = students.delete_one({"_id": student_oid})
        if result.deleted_count == 0:
            return api_error("Student not found.", 404)
        signal_data_change("students", "sections")
        return api_success({"message": "Student deleted successfully."})

    existing_doc = students.find_one({"_id": student_oid})
    if not existing_doc:
        return api_error("Student not found.", 404)

    payload = request_payload()
    student_data, err_message, err_field = sanitize_personal_student_payload(payload, existing_doc=existing_doc)
    if err_message:
        return api_error(err_message, 400, err_field)

    if students.count_documents(build_lrn_duplicate_query(student_data["lrn"], exclude_oid=student_oid)) > 0:
        return api_error("LRN already exists.", 400, "lrn")

    try:
        upsert_manual_section(
            student_data.get("grade_level", ""),
            student_data.get("section", ""),
            created_by=session.get("admin", ""),
        )
    except ValueError as exc:
        return api_error(str(exc), 400, "section")

    student_data["updated_at"] = now_iso()
    try:
        students.update_one({"_id": student_oid}, {"$set": student_data})
    except DuplicateKeyError:
        return api_error("LRN already exists.", 400, "lrn")
    signal_data_change("students")
    updated_doc = students.find_one({"_id": student_oid})
    return api_success({
        "message": "Student updated successfully.",
        "student": normalize_student_doc(updated_doc),
    })


@app.route("/api/sections", methods=["GET", "POST"])
@require_permission("students_read", api=True)
def api_sections():
    if request.method == "GET":
        grade_filter = request.args.get("grade", "").strip() or request.args.get("grade_level", "").strip()
        return api_success({
            "sections_by_grade": build_sections_by_grade(grade_filter),
        })

    if not has_permission("students_write"):
        return api_error("Forbidden", 403)

    payload = request_payload()
    grade_value = str(payload.get("grade", payload.get("grade_level", "")) or "").strip()
    section_value = payload.get("section", "")
    try:
        section_doc = upsert_manual_section(grade_value, section_value, created_by=session.get("admin", ""))
    except ValueError as exc:
        message = str(exc)
        field = "grade" if "grade" in message.lower() else "section"
        return api_error(message, 400, field)

    return api_success({
        "message": "Section saved successfully.",
        "section": section_doc,
        "sections_by_grade": build_sections_by_grade(),
    })


@app.route("/api/sections/stats", methods=["GET"])
@require_permission("students_read", api=True)
def api_sections_stats():
    grade_value = request.args.get("grade", "").strip() or request.args.get("grade_level", "").strip()
    section_value = normalize_section_value(request.args.get("section", ""))

    if not grade_value:
        return api_error("Grade level is required.", 400, "grade")
    if not section_value:
        return api_error("Section is required.", 400, "section")

    grade_clause, grade_level = build_grade_filter(grade_value)
    if not grade_clause:
        return api_error("Invalid grade level.", 400, "grade")

    section_clause = {"section": section_value}
    base_conditions = [grade_clause, section_clause]
    base_query = {"$and": base_conditions}

    male_values = ["Male", "male", "M", "m"]
    female_values = ["Female", "female", "F", "f"]
    male_query = {
        "$and": base_conditions + [{
            "$or": [
                {"gender": {"$in": male_values}},
                {"sex": {"$in": male_values}},
            ]
        }]
    }
    female_query = {
        "$and": base_conditions + [{
            "$or": [
                {"gender": {"$in": female_values}},
                {"sex": {"$in": female_values}},
            ]
        }]
    }

    return api_success({
        "grade_level": grade_level,
        "section": section_value,
        "stats": {
            "total": students.count_documents(base_query),
            "male": students.count_documents(male_query),
            "female": students.count_documents(female_query),
        },
    })


@app.route("/api/sections/clear-students", methods=["POST"])
@require_permission("students_write", api=True)
def api_sections_clear_students():
    payload = request_payload()
    grade_value = str(payload.get("grade", payload.get("grade_level", "")) or "").strip()
    section_value = normalize_section_value(payload.get("section", ""))

    if not grade_value:
        return api_error("Grade level is required.", 400, "grade")
    if not section_value:
        return api_error("Section is required.", 400, "section")

    grade_clause, grade_level = build_grade_filter(grade_value)
    if not grade_clause:
        return api_error("Invalid grade level.", 400, "grade")

    query = {"$and": [grade_clause, {"section": section_value}]}
    update_result = students.update_many(
        query,
        {
            "$set": {
                "section": "",
                "updated_at": now_iso(),
            }
        },
    )
    if update_result.modified_count > 0:
        signal_data_change("students", "sections")

    return api_success({
        "message": f"Removed {update_result.modified_count} student(s) from {grade_level} - {section_value}.",
        "removed_count": int(update_result.modified_count),
        "grade_level": grade_level,
        "section": section_value,
    })


def save_face_registration(student_id, is_update=False):
    student_oid = parse_student_oid(student_id)
    if not student_oid:
        return api_error("Invalid student id.", 400, "id")

    student_doc = students.find_one({"_id": student_oid})
    if not student_doc:
        return api_error("Student not found.", 404)

    payload = request_payload()
    faces_array, face_encodings, err_message, err_field = parse_faces_payload(payload)
    if err_message:
        return api_error(err_message, 400, err_field)

    update_doc = {
        "face_data": faces_array,
        "faces": faces_array,
        "face_encodings": face_encodings,
        "face_embeddings": face_encodings,
        "profile_photo": faces_array[0] if faces_array else "",
        "face_registered": True,
        "face_updated_at": now_local(),
        "updated_at": now_iso(),
    }
    students.update_one({"_id": student_oid}, {"$set": update_doc})
    refresh_scan_face_index_if_active()
    signal_data_change("students")

    saved_doc = students.find_one({"_id": student_oid})
    message = "Face registration updated successfully." if is_update else "Face registered successfully."
    return api_success({"message": message, "student": normalize_student_doc(saved_doc)})


@app.route("/api/students/<id>/face/register", methods=["POST"])
@require_permission("students_write", api=True)
def api_student_face_register(id):
    return save_face_registration(id, is_update=False)


@app.route("/api/students/<id>/face/update", methods=["PUT"])
@require_permission("students_write", api=True)
def api_student_face_update(id):
    return save_face_registration(id, is_update=True)


@app.route("/students/delete/<id>", methods=["POST", "GET"])
@require_permission("students_write")
def delete_student(id):
    try:
        result = students.delete_one({"_id": ObjectId(id)})
        if result.deleted_count:
            signal_data_change("students", "sections")
        return redirect(url_for("students_page", message="Student deleted successfully.", message_type="success"))
    except Exception:
        return redirect(url_for("students_page", message="Failed to delete student record.", message_type="error"))


# =====================================
# LOG ROUTES
# =====================================
def build_gate_logs_query(args):
    q = args.get("q", "").strip()
    start_date = args.get("start_date", "").strip()
    end_date = args.get("end_date", "").strip()
    status_filter = args.get("status", "").strip()
    session_filter = args.get("session", "").strip().upper()
    sort_by = args.get("sort", "newest").strip()

    query = {}
    q_regex = contains_regex_filter(q)
    if q_regex:
        query["$or"] = [
            {"student_name": q_regex},
            {"student_id": q_regex},
            {"date": q_regex},
        ]

    if start_date or end_date:
        date_query = {}
        if start_date:
            date_query["$gte"] = start_date
        if end_date:
            date_query["$lte"] = end_date
        query["date"] = date_query

    if status_filter:
        query["status"] = status_filter

    if session_filter in ("IN", "OUT"):
        query["gate_action"] = session_filter

    sort_spec = [("timestamp", -1)] if sort_by != "oldest" else [("timestamp", 1)]

    filters_payload = {
        "q": q,
        "start_date": start_date,
        "end_date": end_date,
        "status": status_filter,
        "session": session_filter,
        "sort": sort_by,
    }
    return query, sort_spec, filters_payload


def build_student_photo_map(student_ids):
    normalized_ids = sorted({
        str(student_id or "").strip()
        for student_id in student_ids
        if str(student_id or "").strip()
    })
    if not normalized_ids:
        return {}

    photo_map = {}
    projection = {"student_id": 1, "profile_photo": 1, "face_data": 1, "faces": 1}
    for row in students.find({"student_id": {"$in": normalized_ids}}, projection):
        student_id = str(row.get("student_id", "")).strip()
        if not student_id or student_id in photo_map:
            continue
        normalized = normalize_student_doc(row)
        photo_map[student_id] = normalized.get("profile_photo", "")
    return photo_map


def build_sms_logs_query(args):
    q = args.get("q", "").strip()
    start_date = args.get("start_date", "").strip()
    end_date = args.get("end_date", "").strip()
    status_filter = args.get("status", "").strip()
    sort_by = args.get("sort", "newest").strip()

    query = {}
    q_regex = contains_regex_filter(q)
    if q_regex:
        query["$or"] = [
            {"name": q_regex},
            {"student_id": q_regex},
            {"parent_contact": q_regex},
        ]

    if start_date or end_date:
        date_query = {}
        if start_date:
            date_query["$gte"] = start_date
        if end_date:
            date_query["$lte"] = end_date
        query["date"] = date_query

    if status_filter.strip().lower() in ("sent", "failed", "queued", "sending", "skipped"):
        query["status"] = sms_status_mongo_filter(status_filter.strip().lower())

    sort_spec = [("timestamp", -1)] if sort_by != "oldest" else [("timestamp", 1)]

    filters_payload = {
        "q": q,
        "start_date": start_date,
        "end_date": end_date,
        "status": status_filter,
        "sort": sort_by,
    }
    return query, sort_spec, filters_payload


@app.route("/gate-logs")
@require_permission("logs")
def gate_logs_page():
    query, sort_spec, filters_payload = build_gate_logs_query(request.args)
    try:
        page = max(int(request.args.get("page", "1")), 1)
    except ValueError:
        page = 1

    per_page = 15
    total_filtered = attendance_logs.count_documents(query)
    pagination = build_pagination_payload(page, per_page, total_filtered, filters_payload, "gate_logs_page")
    skip = (pagination["page"] - 1) * per_page

    rows = list(attendance_logs.find(query).sort(sort_spec).skip(skip).limit(per_page))
    photo_map = build_student_photo_map([row.get("student_id", "") for row in rows])
    logs = []
    for row in rows:
        student_id = row.get("student_id", "")
        logs.append({
            "_id": str(row.get("_id")),
            "student_id": student_id,
            "name": row.get("student_name", ""),
            "action": row.get("gate_action", "IN"),
            "status": row.get("status", "Present"),
            "session": row.get("session", ""),
            "verification_label": row.get("verification_label", ""),
            "date": row.get("date", ""),
            "time": row.get("time", ""),
            "timestamp": row.get("timestamp", ""),
            "profile_photo": photo_map.get(str(student_id).strip(), ""),
        })

    stats = {
        "total_entries": total_filtered,
        "total_in": attendance_logs.count_documents({**query, "gate_action": "IN"}),
        "total_out": attendance_logs.count_documents({**query, "gate_action": "OUT"}),
        "late_count": attendance_logs.count_documents({**query, "status": "Late"}),
    }

    return render_template(
        "gate_logs.html",
        logs=logs,
        stats=stats,
        filters=filters_payload,
        pagination=pagination,
        export_query=urlencode({k: v for k, v in filters_payload.items() if v not in ("", None)}),
        **sidebar_context("gate_logs"),
    )


@app.route("/gate-logs/export")
@require_permission("logs")
def gate_logs_export():
    query, sort_spec, _filters_payload = build_gate_logs_query(request.args)
    rows = list(attendance_logs.find(query).sort(sort_spec).limit(5000))

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Student ID", "Name", "Date", "Time", "Action", "Session", "Status", "Verification Label", "Timestamp"])
    for row in rows:
        writer.writerow([
            row.get("student_id", ""),
            row.get("student_name", ""),
            row.get("date", ""),
            row.get("time", ""),
            row.get("gate_action", ""),
            row.get("session", ""),
            row.get("status", ""),
            row.get("verification_label", ""),
            row.get("timestamp", ""),
        ])

    filename = f"gate_logs_{now_local().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/gate-logs/latest")
@require_permission("logs", api=True)
def gate_logs_latest():
    since_id = request.args.get("since_id", "").strip()
    query = {}
    if since_id:
        try:
            query["_id"] = {"$gt": ObjectId(since_id)}
        except Exception:
            pass

    rows = list(attendance_logs.find(query).sort("_id", -1).limit(10))
    rows.reverse()
    photo_map = build_student_photo_map([row.get("student_id", "") for row in rows])

    payload = []
    for row in rows:
        student_id = row.get("student_id", "")
        payload.append({
            "_id": str(row.get("_id")),
            "student_id": student_id,
            "name": row.get("student_name", ""),
            "action": row.get("gate_action", "IN"),
            "status": row.get("status", "Present"),
            "session": row.get("session", ""),
            "verification_label": row.get("verification_label", ""),
            "date": row.get("date", ""),
            "time": row.get("time", ""),
            "timestamp": row.get("timestamp", ""),
            "profile_photo": photo_map.get(str(student_id).strip(), ""),
        })

    return jsonify({"status": "ok", "logs": payload})


@app.route("/gate-logs/delete/<id>", methods=["POST", "DELETE"])
@require_permission("logs", api=True)
def gate_logs_delete(id):
    if current_role() != "Full Admin":
        return jsonify({"status": "error", "message": "Only Full Admin can delete gate logs."}), 403

    try:
        result = attendance_logs.delete_one({"_id": ObjectId(id)})
        if result.deleted_count == 0:
            return jsonify({"status": "error", "message": "Gate log not found."}), 404
        signal_data_change("gate_logs")
        return jsonify({"status": "ok", "message": "Gate log deleted."})
    except Exception:
        return jsonify({"status": "error", "message": "Failed to delete gate log."}), 400


@app.route("/simulate-gate/<student_id>")
@require_permission("scan", api=True)
def simulate_gate(student_id):
    student = students.find_one({"student_id": student_id})
    if not student:
        return jsonify({"status": "FAILED", "error": "Student not found"}), 404

    now = now_local()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    session_info = resolve_gate_session(now)
    status = session_info["status"]
    gate_action = session_info["gate_action"]
    verification_label = session_info["verification_label"]
    session_name = session_info["session"]

    dedupe_query = {
        "student_id": student.get("student_id", ""),
        "date": date_str,
        "session": session_name,
    }
    existing_record = attendance_logs.find_one(dedupe_query)
    duplicate = existing_record is not None

    if not duplicate:
        attendance_logs.insert_one({
            "student_id": student.get("student_id", ""),
            "student_name": student.get("name", ""),
            "status": status,
            "session": session_name,
            "source": "manual_simulation",
            "timestamp": now_iso(),
            "date": date_str,
            "time": time_str,
            "gate_action": gate_action,
            "verification_label": verification_label,
        })
        signal_data_change("gate_logs")

    push_scan_event("verified", {
        "name": student.get("name", ""),
        "verified": True,
        "attendance_status": status,
        "sms_status": "SKIPPED",
        "gate_action": gate_action,
        "verification_label": verification_label,
        "session": session_name,
        "display_message": "Done!" if duplicate else session_info["display_message"],
        "voice_message": "Done!" if duplicate else session_info["voice_message"],
        "duplicate": duplicate,
    })
    return jsonify({"status": "SUCCESS", "name": student.get("name", ""), "action": gate_action})


@app.route("/sms-logs")
@require_permission("logs")
def sms_logs_page():
    query, sort_spec, filters_payload = build_sms_logs_query(request.args)
    try:
        page = max(int(request.args.get("page", "1")), 1)
    except ValueError:
        page = 1

    per_page = 15
    total_filtered = sms_logs.count_documents(query)
    pagination = build_pagination_payload(page, per_page, total_filtered, filters_payload, "sms_logs_page")
    skip = (pagination["page"] - 1) * per_page

    rows = list(sms_logs.find(query).sort(sort_spec).skip(skip).limit(per_page))
    logs = []
    for row in rows:
        status_value = str(row.get("status", "") or "")
        logs.append({
            "_id": str(row.get("_id")),
            "student_id": row.get("student_id", ""),
            "name": row.get("name", ""),
            "parent_contact": row.get("parent_contact", ""),
            "message": row.get("message", ""),
            "status": status_value.upper() if status_value else "",
            "sid": row.get("sid", ""),
            "error": row.get("error", ""),
            "date": row.get("date", ""),
            "time": row.get("time", ""),
            "timestamp": row.get("timestamp", ""),
        })

    stats = {
        "total_logs": total_filtered,
        "sent_count": sms_logs.count_documents({**query, "status": sms_status_mongo_filter("sent")}),
        "failed_count": sms_logs.count_documents({**query, "status": sms_status_mongo_filter("failed")}),
    }

    return render_template(
        "sms_logs.html",
        logs=logs,
        stats=stats,
        filters=filters_payload,
        pagination=pagination,
        export_query=urlencode({k: v for k, v in filters_payload.items() if v not in ("", None)}),
        **sidebar_context("sms_logs"),
    )


@app.route("/sms-logs/export")
@require_permission("logs")
def sms_logs_export():
    query, sort_spec, _filters_payload = build_sms_logs_query(request.args)
    rows = list(sms_logs.find(query).sort(sort_spec).limit(5000))

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Student ID", "Name", "Parent Contact", "Date", "Time", "Status", "Message", "SID", "Error", "Timestamp"])
    for row in rows:
        writer.writerow([
            row.get("student_id", ""),
            row.get("name", ""),
            row.get("parent_contact", ""),
            row.get("date", ""),
            row.get("time", ""),
            row.get("status", ""),
            row.get("message", ""),
            row.get("sid", ""),
            row.get("error", ""),
            row.get("timestamp", ""),
        ])

    filename = f"sms_logs_{now_local().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/sms-logs/resend/<id>", methods=["POST"])
@require_permission("logs", api=True)
def sms_logs_resend(id):
    try:
        sms_log_id = ObjectId(id)
    except Exception:
        return jsonify({"status": "error", "message": "Invalid SMS log id."}), 400

    original = sms_logs.find_one({"_id": sms_log_id})
    if not original:
        return jsonify({"status": "error", "message": "SMS log not found."}), 404

    parent_contact = original.get("parent_contact", "")
    message = original.get("message", "")
    if not parent_contact or not message:
        return jsonify({"status": "error", "message": "Missing recipient or message content."}), 400

    sms_result = send_sms(
        parent_contact,
        message,
        sms_type=original.get("type", "transactional"),
        metadata={"context": "sms_resend", "resent_from": str(original.get("_id"))},
        student_id=original.get("student_id", ""),
        student_name=original.get("name", ""),
        parent_contact=parent_contact,
    )
    sms_status = "sent" if sms_result.get("status") == "sent" else "failed"
    sms_sid = sms_result.get("sid", "")
    sms_error = sms_result.get("error", "")
    now = now_local()

    if sms_status == "failed":
        create_alert(
            level="high",
            message=f"Failed SMS resend for {original.get('name', original.get('student_id', 'Unknown'))}.",
            category="sms",
            meta={"student_id": original.get("student_id", ""), "error": sms_error},
        )

    return jsonify({
        "status": "ok",
        "message": "SMS resend queued." if sms_status == "sent" else "SMS resend failed.",
        "sms_status": sms_status.upper(),
        "provider_message_id": sms_sid,
        "error": sms_error,
        "timestamp": now_iso(),
    })


# =====================================
# ANALYTICS
# =====================================
@app.route("/analytics")
@require_permission("analytics")
def analytics():
    today = now_local().date()
    range_type = request.args.get("range", "month").strip().lower()

    if range_type == "week":
        start_date = today - timedelta(days=6)
        end_date = today
    elif range_type == "custom":
        start_date = parse_date_or_none(request.args.get("start_date")) or (today - timedelta(days=29))
        end_date = parse_date_or_none(request.args.get("end_date")) or today
        if start_date > end_date:
            start_date, end_date = end_date, start_date
    else:
        range_type = "month"
        start_date = today - timedelta(days=29)
        end_date = today

    labels = []
    cursor = start_date
    while cursor <= end_date:
        labels.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)

    attendance_pipeline = [
        {"$match": {"date": {"$gte": start_date.strftime("%Y-%m-%d"), "$lte": end_date.strftime("%Y-%m-%d")}}},
        {"$group": {"_id": "$date", "count": {"$sum": 1}}},
    ]
    sms_pipeline = [
        {"$match": {"date": {"$gte": start_date.strftime("%Y-%m-%d"), "$lte": end_date.strftime("%Y-%m-%d")}, "status": sms_status_mongo_filter("sent")}},
        {"$group": {"_id": "$date", "count": {"$sum": 1}}},
    ]

    attendance_map = {row["_id"]: row["count"] for row in attendance_logs.aggregate(attendance_pipeline)}
    sms_map = {row["_id"]: row["count"] for row in sms_logs.aggregate(sms_pipeline)}
    gate_series = [attendance_map.get(day, 0) for day in labels]
    sms_series = [sms_map.get(day, 0) for day in labels]

    total_gate_entries = sum(gate_series)
    total_sms_sent = sum(sms_series)

    today_str = today.strftime("%Y-%m-%d")
    present_today_ids = set(attendance_logs.distinct("student_id", {"date": today_str}))
    present_today_count = len([sid for sid in present_today_ids if sid])
    late_today_count = attendance_logs.count_documents({"date": today_str, "status": "Late"})

    end_str = end_date.strftime("%Y-%m-%d")
    total_students = students.count_documents({})
    late_ids = set(attendance_logs.distinct("student_id", {"date": end_str, "status": "Late"}))
    present_ids_all = set(attendance_logs.distinct("student_id", {"date": end_str}))
    present_ids = set([sid for sid in present_ids_all if sid]) - set([sid for sid in late_ids if sid])
    late_ids = set([sid for sid in late_ids if sid])
    absent_count = max(total_students - len(present_ids) - len(late_ids), 0)

    attendance_distribution = {
        "present": len(present_ids),
        "late": len(late_ids),
        "absent": absent_count,
    }

    return render_template(
        "analytics.html",
        filters={
            "range": range_type,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        },
        stats={
            "total_gate_entries": total_gate_entries,
            "total_sms_sent": total_sms_sent,
            "present_today": present_today_count,
            "late_today": late_today_count,
        },
        chart_labels=labels,
        gate_series=gate_series,
        sms_series=sms_series,
        attendance_distribution=attendance_distribution,
        grade_options=list(GRADE_LEVEL_OPTIONS),
        ai_defaults={
            "range": "7d" if range_type == "week" else "30d",
            "change_mode": "custom_range" if range_type == "custom" else "today_vs_yesterday",
        },
        **sidebar_context("analytics"),
    )


def analytics_collections():
    return {
        "students": students,
        "attendance_logs": attendance_logs,
        "sms_logs": sms_logs,
        "alerts": alerts,
        "failed_scans": failed_scans,
    }


@app.route("/api/analytics/ai/insights", methods=["GET"])
@require_permission("analytics", api=True)
def api_analytics_ai_insights():
    range_value = request.args.get("range", "7d").strip().lower()
    grade_value = request.args.get("grade", "").strip()
    section_value = request.args.get("section", "").strip()

    if range_value not in SUPPORTED_INSIGHT_RANGES:
        return api_error("Invalid range. Allowed values: today, 7d, 30d.", 400, "range")

    try:
        payload = get_ai_insights(
            analytics_collections(),
            range_key=range_value,
            grade=grade_value,
            section=section_value,
        )
        return api_success(payload)
    except ValueError as exc:
        return api_error(str(exc), 400)
    except Exception as exc:
        print(f"[ERROR] /api/analytics/ai/insights failed: {exc}")
        return api_error("Failed to generate AI insights.", 500)


@app.route("/api/analytics/ai/risk", methods=["GET"])
@require_permission("analytics", api=True)
def api_analytics_ai_risk():
    target_value = request.args.get("target", "next_school_day").strip()
    grade_value = request.args.get("grade", "").strip()
    section_value = request.args.get("section", "").strip()
    try:
        limit_value = int(request.args.get("limit", "20"))
    except (TypeError, ValueError):
        limit_value = 20

    if target_value not in SUPPORTED_RISK_TARGETS:
        return api_error("Invalid target. Allowed values: next_school_day.", 400, "target")

    try:
        payload = get_risk_predictions(
            analytics_collections(),
            target=target_value,
            limit=limit_value,
            grade=grade_value,
            section=section_value,
        )
        return api_success(payload)
    except ValueError as exc:
        return api_error(str(exc), 400)
    except Exception as exc:
        print(f"[ERROR] /api/analytics/ai/risk failed: {exc}")
        return api_error("Failed to compute risk predictions.", 500)


@app.route("/api/analytics/ai/changes", methods=["GET"])
@require_permission("analytics", api=True)
def api_analytics_ai_changes():
    mode_value = request.args.get("mode", "today_vs_yesterday").strip()
    start_value = request.args.get("start", "").strip()
    end_value = request.args.get("end", "").strip()
    grade_value = request.args.get("grade", "").strip()
    section_value = request.args.get("section", "").strip()

    if mode_value not in SUPPORTED_CHANGE_MODES and not (start_value and end_value):
        return api_error(
            "Invalid mode. Allowed values: today_vs_yesterday, week_vs_last_week, custom_range.",
            400,
            "mode",
        )

    try:
        payload = get_change_explanations(
            analytics_collections(),
            mode=mode_value,
            start=start_value,
            end=end_value,
            grade=grade_value,
            section=section_value,
        )
        return api_success(payload)
    except ValueError as exc:
        return api_error(str(exc), 400)
    except Exception as exc:
        print(f"[ERROR] /api/analytics/ai/changes failed: {exc}")
        return api_error("Failed to compute change explanation.", 500)


@app.route("/api/analytics/ai/nlq", methods=["POST"])
@require_permission("analytics", api=True)
def api_analytics_ai_nlq():
    payload = parse_json_payload()
    query_text = str(payload.get("query", "")).strip()
    grade_value = str(payload.get("grade", "")).strip()
    section_value = str(payload.get("section", "")).strip()

    if not query_text:
        return api_error("Query is required.", 400, "query")

    try:
        result = run_nlq_query(
            analytics_collections(),
            query=query_text,
            grade=grade_value,
            section=section_value,
            llm_enabled=AI_NLQ_LLM_ENABLED,
        )
        return api_success(result)
    except ValueError as exc:
        return api_error(str(exc), 400, "query")
    except Exception as exc:
        print(f"[ERROR] /api/analytics/ai/nlq failed: {exc}")
        return api_error("Failed to process analytics query.", 500)


@app.route("/api/analytics/ai/actions", methods=["GET"])
@require_permission("analytics", api=True)
def api_analytics_ai_actions():
    range_value = request.args.get("range", "30d").strip().lower()
    grade_value = request.args.get("grade", "").strip()
    section_value = request.args.get("section", "").strip()

    if range_value not in SUPPORTED_INSIGHT_RANGES:
        return api_error("Invalid range. Allowed values: today, 7d, 30d.", 400, "range")

    try:
        payload = get_next_best_actions(
            analytics_collections(),
            range_key=range_value,
            grade=grade_value,
            section=section_value,
        )
        return api_success(payload)
    except ValueError as exc:
        return api_error(str(exc), 400)
    except Exception as exc:
        print(f"[ERROR] /api/analytics/ai/actions failed: {exc}")
        return api_error("Failed to compute next best actions.", 500)


# =====================================
# TEST SMS
# =====================================
@app.route("/test_sms")
@require_permission("users_manage", api=True)
def test_sms():
    verified_recipient = os.getenv("TEST_SMS_RECIPIENT") or os.getenv("VERIFIED_RECIPIENT")
    if not verified_recipient:
        return jsonify({"status": "failed", "error": "No test recipient number set in TEST_SMS_RECIPIENT."}), 400

    message = "This is a PHILSMS test message from CHS Gate Access."
    result = send_sms(verified_recipient, message, sms_type="transactional", metadata={"context": "test_sms"})
    if result.get("status") == "sent":
        return jsonify({"status": "sent", "provider": "PHILSMS", "sid": result.get("sid")})
    return jsonify({"status": "failed", "error": result.get("error", "Unknown error")})


@app.route("/api/debug/sms/test", methods=["POST"])
@require_permission("users_manage", api=True)
def debug_sms_test():
    if current_role() != "Full Admin":
        return jsonify({"status": "error", "message": "Only Full Admin can run SMS debug test."}), 403

    payload = parse_json_payload()
    to_value = (payload.get("to") or os.getenv("TEST_SMS_RECIPIENT") or "").strip()
    message = (payload.get("message") or "PHILSMS debug test from CHS Gate Access.").strip()

    if not to_value:
        return jsonify({"status": "error", "message": "Recipient is required (payload.to or TEST_SMS_RECIPIENT)."}), 400

    health = sms_provider.health_check()
    if health.get("status") != "ok":
        return jsonify({"status": "error", "message": "SMS provider health check failed.", "health": health}), 503

    result = send_sms(
        to_value,
        message,
        sms_type="transactional",
        metadata={"context": "debug_sms_test"},
        parent_contact=to_value,
    )
    if not result.get("log_id"):
        return jsonify({
            "status": "error",
            "message": "SMS attempted but no log entry id was returned.",
            "health": health,
            "result": result,
        }), 500

    log_doc = sms_logs.find_one({"_id": ObjectId(result["log_id"])})
    if log_doc:
        log_doc["_id"] = str(log_doc["_id"])

    http_code = 200 if result.get("status") == "sent" else 502
    return jsonify({
        "status": "ok" if result.get("status") == "sent" else "error",
        "health": health,
        "result": result,
        "log": log_doc,
    }), http_code


# =====================================
# RUN APP
# =====================================
if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "1").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=debug_mode, use_reloader=debug_mode)
