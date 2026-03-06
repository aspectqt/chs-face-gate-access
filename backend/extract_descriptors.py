import os
import face_recognition
from pymongo import MongoClient

# -------------------------
# MongoDB Setup
# -------------------------
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
db = client["face_gate_db"]

# -------------------------
# Paths
# -------------------------
FACES_DIR = "faces"  # This is where we store faces per student_id

# -------------------------
# Loop through students
# -------------------------
for student_id in os.listdir(FACES_DIR):
    student_path = os.path.join(FACES_DIR, student_id)
    if not os.path.isdir(student_path):
        continue

    descriptors = []

    for img_file in os.listdir(student_path):
        img_path = os.path.join(student_path, img_file)
        image = face_recognition.load_image_file(img_path)

        # Detect faces
        face_locations = face_recognition.face_locations(image)
        if not face_locations:
            print(f"No face detected in {img_path}, skipping...")
            continue

        # Compute face descriptor (encoding)
        face_encodings = face_recognition.face_encodings(image, known_face_locations=face_locations)
        if not face_encodings:
            print(f"Failed to compute descriptor for {img_path}, skipping...")
            continue

        descriptors.append(face_encodings[0].tolist())  # convert numpy array to list

    if descriptors:
        # Update student in MongoDB with descriptors
        result = db.students.update_one(
            {"student_id": student_id},
            {"$set": {"descriptor": descriptors}}
        )
        if result.matched_count:
            print(f"Saved descriptors for student_id {student_id}")
        else:
            print(f"No matching student found in DB for {student_id}")
    else:
        print(f"No valid descriptors found for student_id {student_id}")
