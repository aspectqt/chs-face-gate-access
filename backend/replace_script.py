import re

replacement = '''def get_default_schedule():
    settings = system_settings.find_one({"key": "default_schedule"})
    if settings and "schedule" in settings:
        return settings["schedule"]
    return {
        "morning_start": "05:00",
        "morning_late": "08:15",
        "noon_start": "12:00",
        "afternoon_start": "13:00",
        "afternoon_late": "13:15",
        "afternoon_end": "17:00"
    }

def get_active_schedule(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    event = calendar_events.find_one({"date": date_str})
    if event:
        if event.get("custom_schedule"):
            return {
                "type": event.get("type", "event"),
                "special_condition": event.get("special_condition"),
                "schedule": event["custom_schedule"]
            }
        return {
            "type": event.get("type", "event"),
            "special_condition": event.get("special_condition"),
            "schedule": get_default_schedule()
        }
    return {
        "type": "regular",
        "special_condition": None,
        "schedule": get_default_schedule()
    }

def parse_time_str(time_str):
    if not time_str: return None
    try:
        parts = str(time_str).split(":")
        return dtime(hour=int(parts[0]), minute=int(parts[1]))
    except:
        return None

def session_info_for_time(dt):
    active_sched = get_active_schedule(dt)
    sched = active_sched.get("schedule", {})
    
    m_start = parse_time_str(sched.get("morning_start")) or MORNING_START
    m_late_thr = parse_time_str(sched.get("morning_late")) or MORNING_LATE_THRESHOLD
    n_start = parse_time_str(sched.get("noon_start")) or NOON_START
    a_start = parse_time_str(sched.get("afternoon_start")) or AFTERNOON_START
    a_late_thr = parse_time_str(sched.get("afternoon_late")) or AFTERNOON_LATE_THRESHOLD
    a_end = parse_time_str(sched.get("afternoon_end")) or AFTERNOON_END_START
    
    t = dt.time()
    
    is_holiday = active_sched.get("type") == "holiday"
    special_cond = active_sched.get("special_condition")
    
    def make_res(session, action, status, label):
        display_msg = f"{label}"
        if special_cond:
            display_msg += f" ({special_cond})"
            
        if is_holiday:
            status = "Holiday"
            display_msg = f"{label} (Holiday)"
        elif status == "Late":
            display_msg += " - You are Late"
            
        voice_msg = display_msg.replace("-", ",").replace(" (", ", ").replace(")", "")
        return {
            "session": session,
            "gate_action": action,
            "verification_label": label,
            "status": status,
            "display_message": display_msg,
            "voice_message": voice_msg,
        }

    if m_start <= t < n_start:
        is_late = t >= m_late_thr
        return make_res("Morning In", "IN", "Late" if is_late else "Present", "Verified In")

    if n_start <= t < a_start:
        return make_res("Noon Out", "OUT", "Present", "Verified Out")

    if a_start <= t < a_end:
        is_late = t >= a_late_thr
        return make_res("Afternoon In", "IN", "Late" if is_late else "Present", "Verified In")

    if t >= a_end:
        return make_res("Afternoon Out", "OUT", "Present", "Verified Out")

    return make_res("Morning In", "IN", "Present", "Verified In")
'''

with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()

pattern = re.compile(r'def session_info_for_time\(dt\):.*?def normalize_scan_session_mode', re.DOTALL)
if pattern.search(text):
    text = pattern.sub(replacement + '\n\ndef normalize_scan_session_mode', text)
    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print('SUCCESS')
else:
    print('Regex failed to match')
