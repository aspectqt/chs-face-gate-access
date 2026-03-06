from config import students
from io import BytesIO
import base64
import numpy as np
from PIL import Image
import face_recognition


def normalize_grade_level(value):
    v = (value or "").strip()
    if not v:
        return ""
    lower = v.lower()
    if lower.startswith("grade "):
        v = v[6:].strip()
    if v.isdigit() and v in {"7", "8", "9", "10", "11", "12"}:
        return f"Grade {v}"
    return v


def normalize_gender(value):
    v = (value or "").strip().lower()
    if v == "male":
        return "Male"
    if v == "female":
        return "Female"
    return ""


def run():
    updated = 0
    cursor = students.find({})
    for doc in cursor:
        faces = doc.get("face_data", doc.get("faces", []))
        if not isinstance(faces, list):
            faces = []
        faces = faces[:3]

        gender = normalize_gender(doc.get("gender") or doc.get("sex"))
        grade_level = normalize_grade_level(doc.get("grade_level") or doc.get("grade"))
        profile_photo = doc.get("profile_photo") or (faces[0] if faces else "")
        face_encodings = []
        for raw in faces:
            if not raw or "," not in raw:
                continue
            try:
                img_b64 = raw.split(",", 1)[1]
                img = Image.open(BytesIO(base64.b64decode(img_b64))).convert("RGB")
                np_img = np.array(img)
                enc_rows = face_recognition.face_encodings(np_img)
                if enc_rows:
                    face_encodings.append(enc_rows[0].tolist())
            except Exception:
                continue

        patch = {
            "gender": gender,
            "sex": gender,
            "grade_level": grade_level,
            "grade": grade_level,
            "face_data": faces,
            "faces": faces,
            "face_encodings": face_encodings,
            "profile_photo": profile_photo,
        }
        students.update_one({"_id": doc["_id"]}, {"$set": patch})
        updated += 1

    print(f"Updated {updated} student documents.")


if __name__ == "__main__":
    run()
