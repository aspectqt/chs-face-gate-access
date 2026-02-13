from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from config import db
from functools import wraps
from bson.objectid import ObjectId
from datetime import datetime
import cv2
import base64
import numpy as np
import os
import json

app = Flask(__name__)
app.secret_key = "super_secret_key"

# -------------------------
# Ensure faces folder exists
# -------------------------
os.makedirs("faces", exist_ok=True)

# -------------------------
# Haarcascade for AI Face Detection
# -------------------------
CASCADE_PATH = os.path.join(os.path.dirname(__file__), "haarcascade", "haarcascade_frontalface_default.xml")

if not os.path.exists(CASCADE_PATH):
    raise FileNotFoundError(f"Haarcascade XML file not found! Place it here: {CASCADE_PATH}")

face_cascade = cv2.CascadeClassifier(CASCADE_PATH)
if face_cascade.empty():
    raise Exception(f"Failed to load Haarcascade XML! Check the file: {CASCADE_PATH}")

# -------------------------
# LOGIN REQUIRED DECORATOR
# -------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "admin" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

# -------------------------
# LOGIN
# -------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        admin = db.admins.find_one({
            "username": request.form["username"],
            "password": request.form["password"]
        })
        if admin:
            session["admin"] = admin["username"]
            return redirect(url_for("dashboard"))
        return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

# -------------------------
# LOGOUT
# -------------------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# -------------------------
# DASHBOARD
# -------------------------
@app.route("/dashboard")
@login_required
def dashboard():

    students = list(db.students.find())
    total_students = len(students)

    total_gate_logs = db.gate_logs.count_documents({})
    total_sms = db.sms_logs.count_documents({})

    pipeline = [
        {"$group": {"_id": "$date", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    daily_logs = list(db.gate_logs.aggregate(pipeline))

    dates = [d["_id"] for d in daily_logs]
    counts = [d["count"] for d in daily_logs]

    in_count = db.gate_logs.count_documents({"action": "IN"})
    out_count = db.gate_logs.count_documents({"action": "OUT"})

    return render_template(
        "dashboard.html",
        students=students,
        total_students=total_students,
        total_gate_logs=total_gate_logs,
        total_sms=total_sms,
        dates=dates,
        counts=counts,
        in_count=in_count,
        out_count=out_count
    )

# -------------------------
# STUDENTS PAGE + FACE SAVE
# -------------------------
@app.route("/students", methods=["GET", "POST"])
@login_required
def students():

    if request.method == "POST":

        student_id = request.form["student_id"]
        name = request.form["name"]
        grade = request.form["grade"]
        section = request.form["section"]
        parent_contact = request.form["parent_contact"]

        # Save student info
        db.students.insert_one({
            "student_id": student_id,
            "name": name,
            "grade": grade,
            "section": section,
            "parent_contact": parent_contact
        })

        # Save AI face samples
        if "faces" in request.form and request.form["faces"]:
            face_list = json.loads(request.form["faces"])

            for i, face in enumerate(face_list):
                try:
                    header, encoded = face.split(",", 1)
                    data = base64.b64decode(encoded)
                    with open(f"faces/{student_id}_{i}.jpg", "wb") as f:
                        f.write(data)
                except Exception as e:
                    print(f"Failed to save face sample {i} for {student_id}: {e}")

        return redirect(url_for("students"))

    students = list(db.students.find())
    return render_template("students.html", students=students)

# -------------------------
# AI FACE PROCESSING ROUTE
# -------------------------
@app.route("/process-face", methods=["POST"])
@login_required
def process_face():

    try:
        data = request.json["image"]
        header, encoded = data.split(",", 1)
        img = base64.b64decode(encoded)
        np_arr = np.frombuffer(img, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        detected_faces = face_cascade.detectMultiScale(gray, 1.3, 5)

        # No face detected
        if len(detected_faces) == 0:
            return jsonify({"success": False, "message": "No face detected"})

        x, y, w, h = detected_faces[0]

        # Check if face centered
        center_x = x + w / 2
        frame_center = frame.shape[1] / 2

        if abs(center_x - frame_center) > 80:
            return jsonify({"success": False, "message": "Face not centered"})

        # Crop face
        face_crop = frame[y:y+h, x:x+w]

        # Blur detection (AI Quality Score)
        blur_score = cv2.Laplacian(gray, cv2.CV_64F).var()

        if blur_score < 50:
            return jsonify({"success": False, "message": "Image too blurry"})

        _, buffer = cv2.imencode(".jpg", face_crop)
        cropped_base64 = "data:image/jpeg;base64," + base64.b64encode(buffer).decode()

        return jsonify({
            "success": True,
            "image": cropped_base64
        })

    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

# -------------------------
# DELETE STUDENT
# -------------------------
@app.route("/students/delete/<id>")
@login_required
def delete_student(id):
    db.students.delete_one({"_id": ObjectId(id)})
    return redirect(url_for("students"))

# -------------------------
# EDIT STUDENT
# -------------------------
@app.route("/students/edit/<id>", methods=["POST"])
@login_required
def edit_student(id):
    db.students.update_one(
        {"_id": ObjectId(id)},
        {"$set": {
            "student_id": request.form["student_id"],
            "name": request.form["name"],
            "grade": request.form["grade"],
            "section": request.form["section"],
            "parent_contact": request.form["parent_contact"]
        }}
    )
    return redirect(url_for("students"))

# -------------------------
# GATE LOGS
# -------------------------
@app.route("/gate-logs")
@login_required
def gate_logs():
    logs = list(db.gate_logs.find())
    return render_template("gate_logs.html", logs=logs)

# -------------------------
# SMS LOGS
# -------------------------
@app.route("/sms-logs")
@login_required
def sms_logs():
    logs = list(db.sms_logs.find())
    return render_template("sms_logs.html", logs=logs)

# -------------------------
# SIMULATE GATE
# -------------------------
@app.route("/simulate-gate/<student_id>")
def simulate_gate(student_id):

    student = db.students.find_one({"student_id": student_id})

    if not student:
        return jsonify({"status": "FAILED", "error": "Student not found"}), 404

    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%I:%M %p")

    last_in = db.gate_logs.find_one(
        {"student_id": student_id, "action": "IN", "time_out": None},
        sort=[("_id", -1)]
    )

    if last_in:
        db.gate_logs.update_one(
            {"_id": last_in["_id"]},
            {"$set": {"time_out": time_str, "action": "OUT"}}
        )
        message = f"Your child {student['name']} has EXITED the school premises."
    else:
        db.gate_logs.insert_one({
            "student_id": student["student_id"],
            "name": student["name"],
            "action": "IN",
            "time_in": time_str,
            "time_out": None,
            "date": date_str
        })
        message = f"Your child {student['name']} has ENTERED the school premises."

    db.sms_logs.insert_one({
        "student_id": student["student_id"],
        "name": student["name"],
        "parent_contact": student["parent_contact"],
        "message": message,
        "status": "SENT",
        "date": date_str,
        "time": time_str
    })

    return jsonify({
        "status": "SUCCESS",
        "student_id": student["student_id"],
        "name": student["name"],
        "message": message
    })

# -------------------------
# HOME
# -------------------------
@app.route("/")
def home():
    return "Face Recognition Gate Access API is running"

if __name__ == "__main__":
    app.run(debug=True)
