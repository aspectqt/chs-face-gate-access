import cv2
import face_recognition
import numpy as np
from pymongo import MongoClient
import time

# -------------------------------
# MongoDB setup
# -------------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["face_gate_db"]
students_collection = db["students"]

# -------------------------------
# Load student encodings
# -------------------------------
def load_student_encodings():
    students = list(students_collection.find({}))
    encodings = []
    ids = []
    names = []

    for s in students:
        if "descriptor" in s and len(s["descriptor"]) > 0:
            desc_array = np.array(s["descriptor"], dtype=np.float64)
            # Ensure 128-dim
            if desc_array.size != 128:
                desc_array = desc_array[:128] if desc_array.size > 128 else np.pad(desc_array, (0, 128 - desc_array.size))
                students_collection.update_one({"_id": s["_id"]}, {"$set": {"descriptor": desc_array.tolist()}})
                print(f"Fixed descriptor for {s['name']} (was {desc_array.size})")
            encodings.append(desc_array)
            ids.append(s["student_id"])
            names.append(s["name"])
            print(f"Loaded encoding for {s['name']} shape: {desc_array.shape}")

    return encodings, ids, names

print("Loading registered students...")
known_encodings, student_ids, student_names = load_student_encodings()
print(f"{len(known_encodings)} encodings loaded.")

# -------------------------------
# Start webcam
# -------------------------------
video_capture = cv2.VideoCapture(0)
time.sleep(2)
print("Gate Scanner Started. Press 'q' to quit.")

while True:
    ret, frame = video_capture.read()
    if not ret:
        print("Failed to grab frame")
        break

    # Resize for faster processing
    small_frame = cv2.resize(frame, (0, 0), fx=0.25, fy=0.25)
    rgb_small_frame = cv2.cvtColor(small_frame, cv2.COLOR_BGR2RGB)

    # Detect faces
    face_locations = face_recognition.face_locations(rgb_small_frame)

    face_encodings = []
    for face_location in face_locations:
        try:
            encoding = face_recognition.face_encodings(rgb_small_frame, [face_location])[0]
            face_encodings.append(encoding)
        except IndexError:
            # Skip faces where encoding fails
            print("Warning: Failed to compute encoding for a detected face.")
            continue

    # Recognize faces
    for face_encoding, face_location in zip(face_encodings, face_locations):
        name = "Unknown"
        if known_encodings:
            matches = face_recognition.compare_faces(known_encodings, face_encoding)
            face_distances = face_recognition.face_distance(known_encodings, face_encoding)
            best_match_index = np.argmin(face_distances)
            if matches[best_match_index]:
                name = student_names[best_match_index]

        # Scale back face locations
        top, right, bottom, left = [v * 4 for v in face_location]
        cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)
        cv2.putText(frame, name, (left, top - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
        print(f"Detected: {name}")

    cv2.imshow('Gate Scanner', frame)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

video_capture.release()
cv2.destroyAllWindows()