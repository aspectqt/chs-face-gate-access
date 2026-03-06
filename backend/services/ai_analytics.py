from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import copy
import json
import os
import re
import threading
import time


SUPPORTED_INSIGHT_RANGES = {"today", "7d", "30d"}
SUPPORTED_RISK_TARGETS = {"next_school_day"}
SUPPORTED_CHANGE_MODES = {"today_vs_yesterday", "week_vs_last_week", "custom_range"}

_CACHE_LOCK = threading.Lock()
_CACHE_STORE = {}


def _bounded_cache_ttl(default_ttl=120):
    raw = os.getenv("AI_ANALYTICS_CACHE_TTL", str(default_ttl))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = int(default_ttl)
    return min(max(value, 60), 300)


def _cache_get(key):
    now_ts = time.time()
    with _CACHE_LOCK:
        item = _CACHE_STORE.get(key)
        if not item:
            return None
        if item["expires_at"] <= now_ts:
            _CACHE_STORE.pop(key, None)
            return None
        return copy.deepcopy(item["value"])


def _cache_set(key, value, ttl_seconds):
    with _CACHE_LOCK:
        _CACHE_STORE[key] = {
            "expires_at": time.time() + int(ttl_seconds),
            "value": copy.deepcopy(value),
        }


def _cached_call(prefix, params, builder, ttl_seconds):
    cache_key = f"{prefix}:{json.dumps(params, sort_keys=True, default=str)}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    computed = builder()
    _cache_set(cache_key, computed, ttl_seconds)
    return copy.deepcopy(computed)


def _safe_strip(value):
    return str(value or "").strip()


def _coerce_date(value):
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()

    raw = _safe_strip(value)
    if not raw:
        return None
    if "T" in raw:
        raw = raw.split("T", 1)[0]
    if " " in raw:
        raw = raw.split(" ", 1)[0]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception:
        return None


def _coerce_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    raw = _safe_strip(value)
    if not raw:
        return None
    cleaned = raw.replace("Z", "")
    try:
        return datetime.fromisoformat(cleaned)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt)
        except Exception:
            continue
    return None


def _extract_log_date(doc):
    day = _coerce_date(doc.get("date"))
    if day:
        return day

    for key in ("timestamp", "createdAt", "updatedAt"):
        parsed = _coerce_datetime(doc.get(key))
        if parsed:
            return parsed.date()
    return None


def _normalize_grade_label(value):
    raw = _safe_strip(value)
    if not raw:
        return ""

    lowered = raw.lower()
    if lowered.startswith("grade "):
        number = _safe_strip(raw[6:])
        return f"Grade {number}" if number else ""

    if raw.isdigit():
        return f"Grade {raw}"

    match = re.search(r"\d+", raw)
    if match and raw.lower() in {"g" + match.group(0), "grade" + match.group(0)}:
        return f"Grade {match.group(0)}"

    return raw


def _grade_key(value):
    label = _normalize_grade_label(value)
    if not label:
        return ""
    lowered = label.lower()
    if lowered.startswith("grade "):
        return _safe_strip(label[6:])
    match = re.search(r"\d+", label)
    return match.group(0) if match else label.lower()


def _normalize_section(value):
    return _safe_strip(value)


def _matches_grade_section(meta, grade_filter="", section_filter=""):
    grade_key = _grade_key(grade_filter)
    section_key = _safe_strip(section_filter).lower()

    if grade_key:
        student_grade_key = _grade_key(meta.get("grade"))
        if student_grade_key != grade_key:
            return False

    if section_key:
        student_section = _safe_strip(meta.get("section")).lower()
        if student_section != section_key:
            return False

    return True


def _status_is_late(value):
    return _safe_strip(value).lower() == "late"


def _status_is_failed(value):
    status = _safe_strip(value).lower()
    if not status:
        return False
    return status in {"failed", "error", "undelivered", "not_sent", "failure"} or ("fail" in status)


def _pct_change(current_value, baseline_value):
    if baseline_value <= 0:
        return 0.0 if current_value <= 0 else 100.0
    return ((current_value - baseline_value) / baseline_value) * 100.0


def _severity_for_deviation(abs_deviation_pct, abs_delta):
    if abs_deviation_pct >= 70 or abs_delta >= 15:
        return "high"
    if abs_deviation_pct >= 35 or abs_delta >= 6:
        return "warn"
    return "info"


def _severity_rank(level):
    return {"high": 3, "warn": 2, "info": 1}.get(level, 0)


def _range_to_start_end(range_key, today):
    normalized = _safe_strip(range_key).lower()
    if normalized == "today":
        return today, today
    if normalized == "7d":
        return today - timedelta(days=6), today
    if normalized == "30d":
        return today - timedelta(days=29), today
    if normalized.endswith("d") and normalized[:-1].isdigit():
        days = min(max(int(normalized[:-1]), 1), 120)
        return today - timedelta(days=days - 1), today
    return today - timedelta(days=6), today


def _iter_days(start_date, end_date):
    days = []
    cursor = start_date
    while cursor <= end_date:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _iter_school_days(start_date, end_date):
    return [day for day in _iter_days(start_date, end_date) if day.weekday() < 5]


def _previous_school_days(anchor_date, count):
    items = []
    cursor = anchor_date - timedelta(days=1)
    while len(items) < count:
        if cursor.weekday() < 5:
            items.append(cursor)
        cursor -= timedelta(days=1)
    items.reverse()
    return items


def _last_school_days(anchor_date, count):
    items = []
    cursor = anchor_date
    while len(items) < count:
        if cursor.weekday() < 5:
            items.append(cursor)
        cursor -= timedelta(days=1)
    items.reverse()
    return items


def _next_school_day(from_day):
    cursor = from_day + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor


def _group_key(grade, section):
    grade_label = _normalize_grade_label(grade) or "Unknown Grade"
    section_label = _normalize_section(section) or "Unknown Section"
    return (grade_label, section_label)


def _group_label(group):
    return f"{group[0]} - {group[1]}"


def _load_students(students_col):
    projection = {
        "student_id": 1,
        "name": 1,
        "grade_level": 1,
        "grade": 1,
        "section": 1,
        "status": 1,
        "face_registered": 1,
    }
    students_by_id = {}
    for doc in students_col.find({}, projection):
        sid = _safe_strip(doc.get("student_id"))
        if not sid:
            continue
        students_by_id[sid] = {
            "student_id": sid,
            "name": _safe_strip(doc.get("name")),
            "grade": _normalize_grade_label(doc.get("grade_level") or doc.get("grade")),
            "section": _normalize_section(doc.get("section")),
            "status": _safe_strip(doc.get("status")) or "Active",
            "face_registered": bool(doc.get("face_registered")),
        }
    return students_by_id


def _resolve_meta_from_row(row, students_by_id):
    sid = _safe_strip(row.get("student_id"))
    student_meta = students_by_id.get(sid, {})
    grade = _normalize_grade_label(row.get("grade_level") or row.get("grade") or student_meta.get("grade"))
    section = _normalize_section(row.get("section") or student_meta.get("section"))
    return sid, {
        "student_id": sid,
        "grade": grade,
        "section": section,
        "name": _safe_strip(row.get("student_name") or row.get("name") or student_meta.get("name")),
        "face_registered": bool(student_meta.get("face_registered")),
    }


def _collect_attendance(attendance_col, start_date, end_date, students_by_id, grade_filter="", section_filter=""):
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    ts_start = f"{start_str}T00:00:00"
    ts_end = f"{end_str}T23:59:59"

    query = {
        "$or": [
            {"date": {"$gte": start_str, "$lte": end_str}},
            {"timestamp": {"$gte": ts_start, "$lte": ts_end}},
        ]
    }
    projection = {
        "student_id": 1,
        "student_name": 1,
        "status": 1,
        "date": 1,
        "timestamp": 1,
        "gate_action": 1,
        "grade_level": 1,
        "grade": 1,
        "section": 1,
    }

    day_present_sets = defaultdict(set)
    day_late_sets = defaultdict(set)
    group_day_present_sets = defaultdict(set)
    group_day_late_sets = defaultdict(set)
    student_present_days = defaultdict(set)
    student_late_days = defaultdict(set)
    gate_in_count = 0

    for row in attendance_col.find(query, projection):
        day = _extract_log_date(row)
        if not day or day < start_date or day > end_date:
            continue

        sid, row_meta = _resolve_meta_from_row(row, students_by_id)
        if not _matches_grade_section(row_meta, grade_filter=grade_filter, section_filter=section_filter):
            continue

        participant_id = sid or f"row:{_safe_strip(row.get('_id'))}:{_safe_strip(row.get('timestamp'))}"
        group = _group_key(row_meta.get("grade"), row_meta.get("section"))

        day_present_sets[day].add(participant_id)
        group_day_present_sets[(group, day)].add(participant_id)

        if sid:
            student_present_days[sid].add(day)

        if _status_is_late(row.get("status")):
            day_late_sets[day].add(participant_id)
            group_day_late_sets[(group, day)].add(participant_id)
            if sid:
                student_late_days[sid].add(day)

        gate_action = _safe_strip(row.get("gate_action")).upper()
        if gate_action in {"", "IN"}:
            gate_in_count += 1

    present_by_group = Counter()
    late_by_group = Counter()
    daily_present = Counter()
    daily_late = Counter()

    for (group, day), participant_ids in group_day_present_sets.items():
        count_value = len(participant_ids)
        present_by_group[group] += count_value
        daily_present[day.isoformat()] += count_value

    for (group, day), participant_ids in group_day_late_sets.items():
        count_value = len(participant_ids)
        late_by_group[group] += count_value
        daily_late[day.isoformat()] += count_value

    present_total = sum(len(value) for value in day_present_sets.values())
    late_total = sum(len(value) for value in day_late_sets.values())

    return {
        "present_student_days": int(present_total),
        "late_student_days": int(late_total),
        "gate_in_count": int(gate_in_count),
        "present_by_group": present_by_group,
        "late_by_group": late_by_group,
        "daily_present": daily_present,
        "daily_late": daily_late,
        "present_days_by_student": student_present_days,
        "late_days_by_student": student_late_days,
    }


def _collect_sms(sms_col, start_date, end_date, students_by_id, grade_filter="", section_filter=""):
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    ts_start = f"{start_str}T00:00:00"
    ts_end = f"{end_str}T23:59:59"

    query = {
        "$or": [
            {"date": {"$gte": start_str, "$lte": end_str}},
            {"timestamp": {"$gte": ts_start, "$lte": ts_end}},
            {"createdAt": {"$gte": ts_start, "$lte": ts_end}},
        ]
    }
    projection = {
        "student_id": 1,
        "name": 1,
        "status": 1,
        "date": 1,
        "timestamp": 1,
        "createdAt": 1,
        "grade_level": 1,
        "grade": 1,
        "section": 1,
    }

    total_count = 0
    failed_count = 0
    failed_by_student = Counter()
    failed_by_group = Counter()
    daily_failed = Counter()

    for row in sms_col.find(query, projection):
        day = _extract_log_date(row)
        if not day or day < start_date or day > end_date:
            continue

        sid, row_meta = _resolve_meta_from_row(row, students_by_id)
        if not _matches_grade_section(row_meta, grade_filter=grade_filter, section_filter=section_filter):
            continue

        total_count += 1
        if _status_is_failed(row.get("status")):
            failed_count += 1
            if sid:
                failed_by_student[sid] += 1
            failed_by_group[_group_key(row_meta.get("grade"), row_meta.get("section"))] += 1
            daily_failed[day.isoformat()] += 1

    return {
        "total_count": int(total_count),
        "failed_count": int(failed_count),
        "failed_by_student": failed_by_student,
        "failed_by_group": failed_by_group,
        "daily_failed": daily_failed,
    }


def _counter_contributions(current_counter, previous_counter):
    group_deltas = {}
    for group in set(current_counter.keys()) | set(previous_counter.keys()):
        group_deltas[group] = int(current_counter.get(group, 0)) - int(previous_counter.get(group, 0))

    overall = sum(group_deltas.values())
    if overall == 0:
        return []

    if overall > 0:
        relevant = {k: v for k, v in group_deltas.items() if v > 0}
    else:
        relevant = {k: v for k, v in group_deltas.items() if v < 0}

    denominator = sum(abs(v) for v in relevant.values()) or abs(overall)
    ranked = sorted(relevant.items(), key=lambda item: abs(item[1]), reverse=True)[:5]
    return [
        {
            "grade_section": _group_label(group),
            "delta": int(delta),
            "contribution_pct": round((abs(delta) / denominator) * 100.0, 1),
        }
        for group, delta in ranked
    ]


def _resolve_change_periods(mode, start_raw="", end_raw=""):
    today = date.today()
    mode_norm = _safe_strip(mode) or "today_vs_yesterday"

    start_custom = _coerce_date(start_raw)
    end_custom = _coerce_date(end_raw)
    if start_custom and end_custom:
        if start_custom > end_custom:
            start_custom, end_custom = end_custom, start_custom
        span_days = (end_custom - start_custom).days + 1
        previous_end = start_custom - timedelta(days=1)
        previous_start = previous_end - timedelta(days=span_days - 1)
        return {
            "mode": "custom_range",
            "current_start": start_custom,
            "current_end": end_custom,
            "previous_start": previous_start,
            "previous_end": previous_end,
            "current_label": f"{start_custom.isoformat()} to {end_custom.isoformat()}",
            "previous_label": f"{previous_start.isoformat()} to {previous_end.isoformat()}",
        }

    if mode_norm == "week_vs_last_week":
        current_end = today
        current_start = today - timedelta(days=6)
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=6)
        return {
            "mode": "week_vs_last_week",
            "current_start": current_start,
            "current_end": current_end,
            "previous_start": previous_start,
            "previous_end": previous_end,
            "current_label": "This Week",
            "previous_label": "Last Week",
        }

    current_start = today
    current_end = today
    previous_start = today - timedelta(days=1)
    previous_end = today - timedelta(days=1)
    return {
        "mode": "today_vs_yesterday",
        "current_start": current_start,
        "current_end": current_end,
        "previous_start": previous_start,
        "previous_end": previous_end,
        "current_label": "Today",
        "previous_label": "Yesterday",
    }


def get_ai_insights(collections, range_key="7d", grade="", section="", cache_ttl_seconds=None):
    ttl = _bounded_cache_ttl(cache_ttl_seconds or 120)
    normalized_range = _safe_strip(range_key).lower() or "7d"
    if normalized_range not in SUPPORTED_INSIGHT_RANGES:
        raise ValueError("Invalid range. Allowed values: today, 7d, 30d.")

    cache_params = {
        "range": normalized_range,
        "grade": _safe_strip(grade),
        "section": _safe_strip(section),
    }

    def _build():
        students_col = collections["students"]
        attendance_col = collections["attendance_logs"]
        sms_col = collections["sms_logs"]

        students_by_id = _load_students(students_col)
        today = date.today()
        current_start, current_end = _range_to_start_end(normalized_range, today)
        current_school_days = _iter_school_days(current_start, current_end)
        current_school_days_count = max(len(current_school_days), 1)

        baseline_school_days = _previous_school_days(current_start, 14)
        baseline_start = baseline_school_days[0]
        baseline_end = baseline_school_days[-1]

        current_att = _collect_attendance(
            attendance_col,
            current_start,
            current_end,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        baseline_att = _collect_attendance(
            attendance_col,
            baseline_start,
            baseline_end,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )

        current_sms = _collect_sms(
            sms_col,
            current_start,
            current_end,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        baseline_sms = _collect_sms(
            sms_col,
            baseline_start,
            baseline_end,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )

        insights = []

        all_groups = set(current_att["present_by_group"].keys()) | set(baseline_att["present_by_group"].keys())
        for group in all_groups:
            current_value = int(current_att["present_by_group"].get(group, 0))
            baseline_total = int(baseline_att["present_by_group"].get(group, 0))
            expected = (baseline_total / max(len(baseline_school_days), 1)) * current_school_days_count
            delta = current_value - expected
            deviation_pct = _pct_change(current_value, expected)

            if abs(delta) < 2 and abs(deviation_pct) < 25:
                continue

            direction = "drop" if delta < 0 else "spike"
            severity = _severity_for_deviation(abs(deviation_pct), abs(delta))
            label = _group_label(group)
            explanation = (
                f"{label} attendance shows a {direction}. "
                f"Current: {current_value}, baseline expectation: {round(expected, 1)}."
            )

            insights.append({
                "id": f"attendance_{group[0]}_{group[1]}_{direction}".replace(" ", "_").lower(),
                "type": "attendance_anomaly",
                "severity": severity,
                "title": f"Attendance {direction.capitalize()} - {label}",
                "explanation": explanation,
                "deviation_pct": round(deviation_pct, 1),
                "delta": round(delta, 1),
                "view_label": "View",
                "prefill": {"grade": group[0], "section": group[1]},
                "drilldown": {
                    "title": f"Attendance Comparison: {label}",
                    "columns": ["Metric", "Value"],
                    "rows": [
                        {"Metric": "Current attendance (student-days)", "Value": current_value},
                        {"Metric": "Expected attendance (baseline)", "Value": round(expected, 1)},
                        {"Metric": "Deviation (%)", "Value": round(deviation_pct, 1)},
                        {"Metric": "Delta", "Value": round(delta, 1)},
                    ],
                },
            })

        late_groups = set(current_att["late_by_group"].keys()) | set(baseline_att["late_by_group"].keys())
        for group in late_groups:
            current_late = int(current_att["late_by_group"].get(group, 0))
            baseline_late_total = int(baseline_att["late_by_group"].get(group, 0))
            expected_late = (baseline_late_total / max(len(baseline_school_days), 1)) * current_school_days_count
            delta_late = current_late - expected_late
            deviation_late = _pct_change(current_late, expected_late)

            if current_late < 2 or delta_late < 2 or deviation_late < 30:
                continue

            severity = "high" if deviation_late >= 70 or delta_late >= 8 else "warn"
            label = _group_label(group)
            explanation = (
                f"Late arrivals increased for {label}. "
                f"Current: {current_late}, baseline expectation: {round(expected_late, 1)}."
            )
            insights.append({
                "id": f"late_spike_{group[0]}_{group[1]}".replace(" ", "_").lower(),
                "type": "late_spike",
                "severity": severity,
                "title": f"Late Spike - {label}",
                "explanation": explanation,
                "deviation_pct": round(deviation_late, 1),
                "delta": round(delta_late, 1),
                "view_label": "View",
                "prefill": {"grade": group[0], "section": group[1]},
                "drilldown": {
                    "title": f"Late Arrival Details: {label}",
                    "columns": ["Metric", "Value"],
                    "rows": [
                        {"Metric": "Current late count", "Value": current_late},
                        {"Metric": "Expected late count", "Value": round(expected_late, 1)},
                        {"Metric": "Deviation (%)", "Value": round(deviation_late, 1)},
                        {"Metric": "Delta", "Value": round(delta_late, 1)},
                    ],
                },
            })

        attendance_count = int(current_att["present_student_days"])
        gate_entries_count = int(current_att["gate_in_count"])
        mismatch_delta = gate_entries_count - attendance_count
        mismatch_pct = _pct_change(gate_entries_count, attendance_count) if attendance_count > 0 else 0.0
        if attendance_count > 0 and abs(mismatch_delta) >= 5 and abs(mismatch_pct) >= 15:
            severity = "high" if abs(mismatch_pct) >= 50 else "warn"
            insights.append({
                "id": "gate_entries_mismatch",
                "type": "gate_mismatch",
                "severity": severity,
                "title": "Gate Entries Mismatch",
                "explanation": (
                    f"Gate IN events ({gate_entries_count}) differ from attendance student-days ({attendance_count})."
                ),
                "deviation_pct": round(mismatch_pct, 1),
                "delta": int(mismatch_delta),
                "view_label": "View",
                "prefill": {"grade": _safe_strip(grade), "section": _safe_strip(section)},
                "drilldown": {
                    "title": "Gate vs Attendance",
                    "columns": ["Metric", "Value"],
                    "rows": [
                        {"Metric": "Gate IN entries", "Value": gate_entries_count},
                        {"Metric": "Attendance student-days", "Value": attendance_count},
                        {"Metric": "Delta", "Value": mismatch_delta},
                        {"Metric": "Mismatch (%)", "Value": round(mismatch_pct, 1)},
                    ],
                },
            })

        current_failed_sms = int(current_sms["failed_count"])
        current_total_sms = int(current_sms["total_count"])
        baseline_failed_sms = int(baseline_sms["failed_count"])
        baseline_total_sms = int(baseline_sms["total_count"])
        expected_failed_sms = (
            (baseline_failed_sms / max(len(baseline_school_days), 1)) * current_school_days_count
        )
        failed_delta = current_failed_sms - expected_failed_sms
        failed_deviation_pct = _pct_change(current_failed_sms, expected_failed_sms)
        current_failure_rate = (current_failed_sms / current_total_sms) * 100 if current_total_sms > 0 else 0.0
        baseline_failure_rate = (baseline_failed_sms / baseline_total_sms) * 100 if baseline_total_sms > 0 else 0.0

        if current_failed_sms >= 3 and failed_delta >= 2 and failed_deviation_pct >= 30:
            severity = "high" if failed_deviation_pct >= 80 else "warn"
            insights.append({
                "id": "sms_failure_spike",
                "type": "sms_failure_spike",
                "severity": severity,
                "title": "SMS Failure Spike",
                "explanation": (
                    f"SMS failures rose to {current_failed_sms} "
                    f"(baseline expectation: {round(expected_failed_sms, 1)})."
                ),
                "deviation_pct": round(failed_deviation_pct, 1),
                "delta": round(failed_delta, 1),
                "view_label": "View",
                "prefill": {"grade": _safe_strip(grade), "section": _safe_strip(section)},
                "drilldown": {
                    "title": "SMS Failure Analysis",
                    "columns": ["Metric", "Value"],
                    "rows": [
                        {"Metric": "Current failed SMS", "Value": current_failed_sms},
                        {"Metric": "Baseline expected failed SMS", "Value": round(expected_failed_sms, 1)},
                        {"Metric": "Current failure rate (%)", "Value": round(current_failure_rate, 1)},
                        {"Metric": "Baseline failure rate (%)", "Value": round(baseline_failure_rate, 1)},
                    ],
                },
            })

        if not insights:
            insights.append({
                "id": "steady_state",
                "type": "info",
                "severity": "info",
                "title": "No major anomalies detected",
                "explanation": "Current attendance, gate, and SMS patterns are within baseline thresholds.",
                "view_label": "View",
                "prefill": {"grade": _safe_strip(grade), "section": _safe_strip(section)},
                "drilldown": {
                    "title": "Current Window Summary",
                    "columns": ["Metric", "Value"],
                    "rows": [
                        {"Metric": "Attendance student-days", "Value": attendance_count},
                        {"Metric": "Late student-days", "Value": int(current_att["late_student_days"])},
                        {"Metric": "Gate IN entries", "Value": gate_entries_count},
                        {"Metric": "Failed SMS", "Value": current_failed_sms},
                    ],
                },
            })

        insights_sorted = sorted(
            insights,
            key=lambda row: (
                _severity_rank(row.get("severity")),
                abs(float(row.get("deviation_pct", 0.0))),
                abs(float(row.get("delta", 0.0))),
            ),
            reverse=True,
        )[:12]

        return {
            "range": normalized_range,
            "window": {
                "start": current_start.isoformat(),
                "end": current_end.isoformat(),
            },
            "insights": insights_sorted,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    return _cached_call("ai_insights", cache_params, _build, ttl)


def get_risk_predictions(collections, target="next_school_day", limit=20, grade="", section="", cache_ttl_seconds=None):
    ttl = _bounded_cache_ttl(cache_ttl_seconds or 120)
    normalized_target = _safe_strip(target) or "next_school_day"
    if normalized_target not in SUPPORTED_RISK_TARGETS:
        raise ValueError("Invalid target. Allowed values: next_school_day.")

    try:
        safe_limit = int(limit)
    except (TypeError, ValueError):
        safe_limit = 20
    safe_limit = min(max(safe_limit, 5), 50)

    cache_params = {
        "target": normalized_target,
        "limit": safe_limit,
        "grade": _safe_strip(grade),
        "section": _safe_strip(section),
    }

    def _build():
        students_col = collections["students"]
        attendance_col = collections["attendance_logs"]

        students_by_id = _load_students(students_col)
        today = date.today()
        next_school_day = _next_school_day(today)

        history_school_days = _last_school_days(today, 40)
        history_start = history_school_days[0]

        attendance_snapshot = _collect_attendance(
            attendance_col,
            history_start,
            today,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )

        candidates = []
        for sid, meta in students_by_id.items():
            if _safe_strip(meta.get("status")).lower() == "inactive":
                continue
            if not _matches_grade_section(meta, grade_filter=grade, section_filter=section):
                continue
            candidates.append((sid, meta))

        school_days_last5 = history_school_days[-5:]
        school_days_last10 = history_school_days[-10:]
        school_days_streak_window = history_school_days[-15:]

        risk_rows = []
        target_weekday = next_school_day.weekday()

        for sid, meta in candidates:
            present_days = attendance_snapshot["present_days_by_student"].get(sid, set())
            late_days = attendance_snapshot["late_days_by_student"].get(sid, set())

            late_last5 = sum(1 for day in school_days_last5 if day in late_days)
            absent_last10 = sum(1 for day in school_days_last10 if day not in present_days)

            streak = 0
            for day in reversed(school_days_streak_window):
                if day in present_days:
                    break
                streak += 1

            weekday_days = [day for day in history_school_days if day.weekday() == target_weekday]
            weekday_absences = sum(1 for day in weekday_days if day not in present_days)
            weekday_absence_rate = (weekday_absences / len(weekday_days)) if weekday_days else 0.0

            score_late = late_last5 * 12
            score_absent = absent_last10 * 8
            score_streak = min(streak * 10, 30)
            score_weekday = int(round(weekday_absence_rate * 20)) if len(weekday_days) >= 2 else 0
            score_face = 8 if not bool(meta.get("face_registered")) else 0

            risk_score = min(100, score_late + score_absent + score_streak + score_weekday + score_face)
            if risk_score <= 0:
                continue

            reasons = []
            if score_absent > 0:
                reasons.append((score_absent, f"{absent_last10} absences in last 10 school days"))
            if score_late > 0:
                reasons.append((score_late, f"{late_last5} late records in last 5 school days"))
            if score_streak > 0:
                reasons.append((score_streak, f"{streak}-day current absence streak"))
            if score_weekday > 0:
                reasons.append((score_weekday, f"{round(weekday_absence_rate * 100)}% absence rate on {next_school_day.strftime('%A')}"))
            if score_face > 0:
                reasons.append((score_face, "face registration missing"))

            reasons.sort(key=lambda item: item[0], reverse=True)
            top_reasons = [text for _weight, text in reasons[:2]]

            risk_rows.append({
                "student_id": sid,
                "name": meta.get("name") or sid,
                "grade": meta.get("grade"),
                "section": meta.get("section"),
                "risk_score": int(risk_score),
                "reasons": top_reasons,
                "signals": {
                    "late_last_5": int(late_last5),
                    "absent_last_10": int(absent_last10),
                    "absence_streak": int(streak),
                    "weekday_absence_rate": round(weekday_absence_rate, 3),
                    "face_registered": bool(meta.get("face_registered")),
                },
            })

        risk_rows.sort(
            key=lambda row: (
                row.get("risk_score", 0),
                row.get("signals", {}).get("absent_last_10", 0),
                row.get("signals", {}).get("absence_streak", 0),
            ),
            reverse=True,
        )

        return {
            "target": normalized_target,
            "target_day": next_school_day.isoformat(),
            "formula": {
                "late_last_5_weight": 12,
                "absent_last_10_weight": 8,
                "streak_weight": 10,
                "weekday_pattern_weight": 20,
                "face_not_registered_penalty": 8,
                "max_score": 100,
            },
            "rows": risk_rows[:safe_limit],
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    return _cached_call("ai_risk", cache_params, _build, ttl)


def get_change_explanations(collections, mode="today_vs_yesterday", start="", end="", grade="", section="", cache_ttl_seconds=None):
    ttl = _bounded_cache_ttl(cache_ttl_seconds or 120)
    mode_norm = _safe_strip(mode) or "today_vs_yesterday"
    if mode_norm not in SUPPORTED_CHANGE_MODES and not (_coerce_date(start) and _coerce_date(end)):
        raise ValueError("Invalid mode. Allowed values: today_vs_yesterday, week_vs_last_week, custom_range.")

    cache_params = {
        "mode": mode_norm,
        "start": _safe_strip(start),
        "end": _safe_strip(end),
        "grade": _safe_strip(grade),
        "section": _safe_strip(section),
    }

    def _build():
        students_col = collections["students"]
        attendance_col = collections["attendance_logs"]
        sms_col = collections["sms_logs"]

        students_by_id = _load_students(students_col)
        periods = _resolve_change_periods(mode_norm, start_raw=start, end_raw=end)

        current_att = _collect_attendance(
            attendance_col,
            periods["current_start"],
            periods["current_end"],
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        previous_att = _collect_attendance(
            attendance_col,
            periods["previous_start"],
            periods["previous_end"],
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        current_sms = _collect_sms(
            sms_col,
            periods["current_start"],
            periods["current_end"],
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        previous_sms = _collect_sms(
            sms_col,
            periods["previous_start"],
            periods["previous_end"],
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )

        attendance_current = int(current_att["present_student_days"])
        attendance_previous = int(previous_att["present_student_days"])
        late_current = int(current_att["late_student_days"])
        late_previous = int(previous_att["late_student_days"])
        sms_failed_current = int(current_sms["failed_count"])
        sms_failed_previous = int(previous_sms["failed_count"])

        attendance_delta = attendance_current - attendance_previous
        late_delta = late_current - late_previous
        sms_failed_delta = sms_failed_current - sms_failed_previous

        attendance_contrib = _counter_contributions(current_att["present_by_group"], previous_att["present_by_group"])
        late_contrib = _counter_contributions(current_att["late_by_group"], previous_att["late_by_group"])
        sms_contrib = _counter_contributions(current_sms["failed_by_group"], previous_sms["failed_by_group"])

        explanations = []
        if attendance_delta == 0:
            explanations.append("Attendance is stable between the compared periods.")
        elif attendance_delta > 0:
            top = attendance_contrib[0] if attendance_contrib else None
            if top:
                explanations.append(
                    f"Attendance increased by {attendance_delta} student-days; "
                    f"{top['grade_section']} contributed {top['contribution_pct']}% of the gain."
                )
            else:
                explanations.append(f"Attendance increased by {attendance_delta} student-days.")
        else:
            top = attendance_contrib[0] if attendance_contrib else None
            if top:
                explanations.append(
                    f"Attendance dropped by {abs(attendance_delta)} student-days; "
                    f"{top['grade_section']} contributed {top['contribution_pct']}% of the decline."
                )
            else:
                explanations.append(f"Attendance dropped by {abs(attendance_delta)} student-days.")

        if late_delta > 0:
            top = late_contrib[0] if late_contrib else None
            if top:
                explanations.append(
                    f"Late arrivals increased by {late_delta}; "
                    f"largest increase came from {top['grade_section']}."
                )
            else:
                explanations.append(f"Late arrivals increased by {late_delta}.")
        elif late_delta < 0:
            explanations.append(f"Late arrivals improved by {abs(late_delta)} records.")
        else:
            explanations.append("Late arrivals are unchanged.")

        if sms_failed_delta > 0:
            top = sms_contrib[0] if sms_contrib else None
            if top:
                explanations.append(
                    f"SMS failures increased by {sms_failed_delta}; "
                    f"{top['grade_section']} contributed the most."
                )
            else:
                explanations.append(f"SMS failures increased by {sms_failed_delta}.")
        elif sms_failed_delta < 0:
            explanations.append(f"SMS failures improved by {abs(sms_failed_delta)}.")
        else:
            explanations.append("SMS failures are unchanged.")

        return {
            "mode": periods["mode"],
            "current_period": {
                "label": periods["current_label"],
                "start": periods["current_start"].isoformat(),
                "end": periods["current_end"].isoformat(),
            },
            "previous_period": {
                "label": periods["previous_label"],
                "start": periods["previous_start"].isoformat(),
                "end": periods["previous_end"].isoformat(),
            },
            "metrics": {
                "attendance": {
                    "current": attendance_current,
                    "previous": attendance_previous,
                    "delta": attendance_delta,
                    "delta_pct": round(_pct_change(attendance_current, attendance_previous), 1),
                },
                "late": {
                    "current": late_current,
                    "previous": late_previous,
                    "delta": late_delta,
                    "delta_pct": round(_pct_change(late_current, late_previous), 1),
                },
                "sms_failed": {
                    "current": sms_failed_current,
                    "previous": sms_failed_previous,
                    "delta": sms_failed_delta,
                    "delta_pct": round(_pct_change(sms_failed_current, sms_failed_previous), 1),
                },
            },
            "contributors": {
                "attendance": attendance_contrib,
                "late": late_contrib,
                "sms_failed": sms_contrib,
            },
            "explanations": explanations,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    return _cached_call("ai_changes", cache_params, _build, ttl)


def _extract_days_from_phrase(text, default_days=14):
    lowered = _safe_strip(text).lower()
    if "this month" in lowered:
        return 30
    if "this week" in lowered:
        return 7

    match_weeks = re.search(r"last\s+(\d+)\s+weeks?", lowered)
    if match_weeks:
        return min(max(int(match_weeks.group(1)) * 7, 1), 120)

    match_days = re.search(r"last\s+(\d+)\s+days?", lowered)
    if match_days:
        return min(max(int(match_days.group(1)), 1), 120)

    return default_days


def _parse_nlq_intent(query):
    lowered = _safe_strip(query).lower()
    grade_match = re.search(r"grade\s*(\d{1,2})", lowered)
    grade_hint = f"Grade {grade_match.group(1)}" if grade_match else ""

    if "late trend" in lowered:
        return {
            "intent": "late_trend",
            "grade": grade_hint,
            "days": _extract_days_from_phrase(lowered, default_days=30),
        }

    if "most absences" in lowered and "section" in lowered:
        return {
            "intent": "sections_with_most_absences",
            "grade": grade_hint,
            "days": _extract_days_from_phrase(lowered, default_days=14),
        }

    if "consecutive absences" in lowered:
        threshold_match = re.search(r"(\d+)\s*\+?\s*consecutive absences", lowered)
        threshold = int(threshold_match.group(1)) if threshold_match else 3
        threshold = min(max(threshold, 2), 15)
        return {
            "intent": "students_with_consecutive_absences",
            "grade": grade_hint,
            "threshold": threshold,
            "days": 60,
        }

    return None


def run_nlq_query(collections, query, grade="", section="", llm_enabled=False, cache_ttl_seconds=None):
    ttl = _bounded_cache_ttl(cache_ttl_seconds or 120)
    query_text = _safe_strip(query)
    if not query_text:
        raise ValueError("Query is required.")

    cache_params = {
        "query": query_text.lower(),
        "grade": _safe_strip(grade),
        "section": _safe_strip(section),
        "llm_enabled": bool(llm_enabled),
    }

    def _build():
        parsed = _parse_nlq_intent(query_text)
        if not parsed:
            raise ValueError(
                "Unsupported query. Try: "
                "'late trend for grade 9 this month', "
                "'sections with most absences last 2 weeks', or "
                "'students with 3+ consecutive absences'."
            )

        students_col = collections["students"]
        attendance_col = collections["attendance_logs"]
        students_by_id = _load_students(students_col)

        effective_grade = parsed.get("grade") or _safe_strip(grade)
        effective_section = _safe_strip(section)
        today = date.today()

        if parsed["intent"] == "late_trend":
            start_date = today - timedelta(days=parsed["days"] - 1)
            attendance_snapshot = _collect_attendance(
                attendance_col,
                start_date,
                today,
                students_by_id,
                grade_filter=effective_grade,
                section_filter=effective_section,
            )
            rows = []
            labels = []
            values = []
            for day in _iter_days(start_date, today):
                day_key = day.isoformat()
                count_value = int(attendance_snapshot["daily_late"].get(day_key, 0))
                rows.append({"Date": day_key, "Late Count": count_value})
                labels.append(day_key)
                values.append(count_value)

            return {
                "query": query_text,
                "intent": parsed["intent"],
                "title": "Late Trend",
                "columns": ["Date", "Late Count"],
                "rows": rows,
                "chart": {"type": "line", "labels": labels, "values": values, "label": "Late Count"},
                "notes": [
                    f"Range: {start_date.isoformat()} to {today.isoformat()}",
                    f"Grade filter: {effective_grade or 'All Grades'}",
                    f"Section filter: {effective_section or 'All Sections'}",
                ],
            }

        if parsed["intent"] == "sections_with_most_absences":
            start_date = today - timedelta(days=parsed["days"] - 1)
            school_days = _iter_school_days(start_date, today)
            attendance_snapshot = _collect_attendance(
                attendance_col,
                start_date,
                today,
                students_by_id,
                grade_filter=effective_grade,
                section_filter=effective_section,
            )

            section_absences = Counter()
            section_student_counts = Counter()
            for sid, meta in students_by_id.items():
                if _safe_strip(meta.get("status")).lower() == "inactive":
                    continue
                if not _matches_grade_section(meta, grade_filter=effective_grade, section_filter=effective_section):
                    continue
                label = f"{meta.get('grade') or 'Unknown Grade'} - {meta.get('section') or 'Unknown Section'}"
                section_student_counts[label] += 1
                present_days = attendance_snapshot["present_days_by_student"].get(sid, set())
                for day in school_days:
                    if day not in present_days:
                        section_absences[label] += 1

            ranked = section_absences.most_common(12)
            rows = [
                {
                    "Section": label,
                    "Absence Count": int(absence_count),
                    "Students": int(section_student_counts.get(label, 0)),
                }
                for label, absence_count in ranked
            ]

            return {
                "query": query_text,
                "intent": parsed["intent"],
                "title": "Sections with Most Absences",
                "columns": ["Section", "Absence Count", "Students"],
                "rows": rows,
                "chart": {
                    "type": "bar",
                    "labels": [row["Section"] for row in rows[:8]],
                    "values": [row["Absence Count"] for row in rows[:8]],
                    "label": "Absences",
                },
                "notes": [
                    f"School days analyzed: {len(school_days)}",
                    f"Range: {start_date.isoformat()} to {today.isoformat()}",
                ],
            }

        if parsed["intent"] == "students_with_consecutive_absences":
            days = parsed["days"]
            threshold = parsed["threshold"]
            start_date = today - timedelta(days=days - 1)
            school_days = _iter_school_days(start_date, today)
            attendance_snapshot = _collect_attendance(
                attendance_col,
                start_date,
                today,
                students_by_id,
                grade_filter=effective_grade,
                section_filter=effective_section,
            )

            rows = []
            for sid, meta in students_by_id.items():
                if _safe_strip(meta.get("status")).lower() == "inactive":
                    continue
                if not _matches_grade_section(meta, grade_filter=effective_grade, section_filter=effective_section):
                    continue

                present_days = attendance_snapshot["present_days_by_student"].get(sid, set())
                streak = 0
                for day in reversed(school_days):
                    if day in present_days:
                        break
                    streak += 1
                if streak < threshold:
                    continue

                latest_present = max(present_days).isoformat() if present_days else "No recent attendance"
                rows.append({
                    "Student ID": sid,
                    "Name": meta.get("name") or sid,
                    "Grade": meta.get("grade"),
                    "Section": meta.get("section"),
                    "Consecutive Absences": int(streak),
                    "Last Present": latest_present,
                })

            rows.sort(key=lambda row: row["Consecutive Absences"], reverse=True)
            rows = rows[:20]

            return {
                "query": query_text,
                "intent": parsed["intent"],
                "title": f"Students with {threshold}+ Consecutive Absences",
                "columns": ["Student ID", "Name", "Grade", "Section", "Consecutive Absences", "Last Present"],
                "rows": rows,
                "chart": {
                    "type": "bar",
                    "labels": [row["Name"] for row in rows[:8]],
                    "values": [row["Consecutive Absences"] for row in rows[:8]],
                    "label": "Consecutive Absences",
                },
                "notes": [
                    f"Threshold: {threshold}",
                    f"Range: {start_date.isoformat()} to {today.isoformat()}",
                ],
            }

        raise ValueError("Unable to process this query intent.")

    return _cached_call("ai_nlq", cache_params, _build, ttl)


def _failed_scan_candidates(failed_scans_col, students_by_id, start_date, end_date, grade="", section=""):
    if failed_scans_col is None:
        return []

    projection = {"student_id": 1, "reason": 1, "date": 1, "timestamp": 1}
    query = {"reason": {"$in": ["low_confidence", "face_not_encoded", "no_match"]}}
    sid_counter = Counter()

    for row in failed_scans_col.find(query, projection):
        day = _extract_log_date(row)
        if not day or day < start_date or day > end_date:
            continue
        sid = _safe_strip(row.get("student_id"))
        if not sid:
            continue
        meta = students_by_id.get(sid, {})
        if not _matches_grade_section(meta, grade_filter=grade, section_filter=section):
            continue
        sid_counter[sid] += 1

    flagged = [(sid, count) for sid, count in sid_counter.items() if count >= 3]
    flagged.sort(key=lambda item: item[1], reverse=True)
    return flagged


def get_next_best_actions(collections, range_key="30d", grade="", section="", cache_ttl_seconds=None):
    ttl = _bounded_cache_ttl(cache_ttl_seconds or 120)
    normalized_range = _safe_strip(range_key).lower() or "30d"

    cache_params = {
        "range": normalized_range,
        "grade": _safe_strip(grade),
        "section": _safe_strip(section),
    }

    def _build():
        students_col = collections["students"]
        attendance_col = collections["attendance_logs"]
        sms_col = collections["sms_logs"]
        failed_scans_col = collections.get("failed_scans")

        students_by_id = _load_students(students_col)
        today = date.today()
        start_date, end_date = _range_to_start_end(normalized_range, today)

        actions = []

        low_confidence_students = _failed_scan_candidates(
            failed_scans_col,
            students_by_id,
            start_date,
            end_date,
            grade=grade,
            section=section,
        )
        low_conf_rows = []
        for sid, incidents in low_confidence_students[:20]:
            meta = students_by_id.get(sid, {})
            low_conf_rows.append({
                "Student ID": sid,
                "Name": meta.get("name") or sid,
                "Grade": meta.get("grade"),
                "Section": meta.get("section"),
                "Low-Confidence Incidents": int(incidents),
            })

        actions.append({
            "id": "re_register_faces",
            "severity": "warn" if low_confidence_students else "info",
            "title": f"Re-register faces for {len(low_confidence_students)} students",
            "description": "Repeated low-confidence scans can reduce recognition accuracy.",
            "count": len(low_confidence_students),
            "button_label": "View",
            "prefill": {"grade": _safe_strip(grade), "section": _safe_strip(section)},
            "drilldown": {
                "title": "Students with repeated low-confidence scans",
                "columns": ["Student ID", "Name", "Grade", "Section", "Low-Confidence Incidents"],
                "rows": low_conf_rows,
            },
        })

        sms_snapshot = _collect_sms(
            sms_col,
            start_date,
            end_date,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        failed_sms_students = [(sid, cnt) for sid, cnt in sms_snapshot["failed_by_student"].items() if cnt >= 3]
        failed_sms_students.sort(key=lambda item: item[1], reverse=True)

        sms_rows = []
        for sid, failures in failed_sms_students[:20]:
            meta = students_by_id.get(sid, {})
            sms_rows.append({
                "Student ID": sid,
                "Name": meta.get("name") or sid,
                "Grade": meta.get("grade"),
                "Section": meta.get("section"),
                "Failed SMS": int(failures),
            })

        actions.append({
            "id": "verify_contacts",
            "severity": "high" if failed_sms_students else "info",
            "title": f"Verify contacts for {len(failed_sms_students)} students",
            "description": "These students have 3 or more failed SMS sends in the selected range.",
            "count": len(failed_sms_students),
            "button_label": "View",
            "prefill": {"grade": _safe_strip(grade), "section": _safe_strip(section)},
            "drilldown": {
                "title": "Students with repeated SMS failures",
                "columns": ["Student ID", "Name", "Grade", "Section", "Failed SMS"],
                "rows": sms_rows,
            },
        })

        baseline_school_days = _previous_school_days(today, 14)
        baseline_start = baseline_school_days[0]
        baseline_end = baseline_school_days[-1]

        today_att = _collect_attendance(
            attendance_col,
            today,
            today,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )
        baseline_att = _collect_attendance(
            attendance_col,
            baseline_start,
            baseline_end,
            students_by_id,
            grade_filter=grade,
            section_filter=section,
        )

        candidate_drop = None
        for group in set(today_att["present_by_group"].keys()) | set(baseline_att["present_by_group"].keys()):
            current_value = int(today_att["present_by_group"].get(group, 0))
            baseline_total = int(baseline_att["present_by_group"].get(group, 0))
            expected_today = baseline_total / max(len(baseline_school_days), 1)
            delta = current_value - expected_today
            if delta >= -2:
                continue
            deviation_pct = _pct_change(current_value, expected_today)
            candidate = {
                "group": group,
                "delta": round(delta, 1),
                "deviation_pct": round(deviation_pct, 1),
                "current": current_value,
                "expected": round(expected_today, 1),
            }
            if not candidate_drop or candidate["delta"] < candidate_drop["delta"]:
                candidate_drop = candidate

        if candidate_drop:
            grade_label, section_label = candidate_drop["group"]
            actions.append({
                "id": "investigate_attendance_drop",
                "severity": "high",
                "title": f"Investigate {grade_label} - {section_label} attendance drop",
                "description": (
                    f"Today is {abs(candidate_drop['delta'])} student-days below baseline "
                    f"({candidate_drop['current']} vs {candidate_drop['expected']})."
                ),
                "count": int(abs(candidate_drop["delta"])),
                "button_label": "View",
                "prefill": {"grade": grade_label, "section": section_label},
                "drilldown": {
                    "title": "Attendance drop details",
                    "columns": ["Metric", "Value"],
                    "rows": [
                        {"Metric": "Current attendance today", "Value": candidate_drop["current"]},
                        {"Metric": "Baseline expected today", "Value": candidate_drop["expected"]},
                        {"Metric": "Delta", "Value": candidate_drop["delta"]},
                        {"Metric": "Deviation (%)", "Value": candidate_drop["deviation_pct"]},
                    ],
                },
            })

        actions.sort(
            key=lambda row: (_severity_rank(row.get("severity")), row.get("count", 0)),
            reverse=True,
        )

        return {
            "range": normalized_range,
            "actions": actions,
            "window": {"start": start_date.isoformat(), "end": end_date.isoformat()},
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    return _cached_call("ai_actions", cache_params, _build, ttl)
