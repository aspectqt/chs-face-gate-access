from config import students


def run():
    with_face_data = students.update_many(
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
    without_face_data = students.update_many(
        {"face_registered": {"$exists": False}},
        {"$set": {"face_registered": False}},
    )

    print(
        "Updated face_registered defaults. "
        f"registered={with_face_data.modified_count}, "
        f"not_registered={without_face_data.modified_count}"
    )


if __name__ == "__main__":
    run()
