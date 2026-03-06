from datetime import datetime
from pymongo import ASCENDING
from config import db


def main():
    source = db["Attendance"]
    target = db["attendance_logs"]

    target.create_index([("legacy_id", ASCENDING)], unique=True, sparse=True)
    target.create_index([("date", ASCENDING)])
    target.create_index([("student_id", ASCENDING), ("date", ASCENDING)])

    total = source.count_documents({})
    migrated = 0
    skipped = 0

    print(f"[{datetime.now().isoformat(timespec='seconds')}] Starting migration Attendance -> attendance_logs")
    print(f"Source documents: {total}")

    for row in source.find():
        legacy_id = str(row.get("_id"))
        if not legacy_id:
            skipped += 1
            continue

        doc = dict(row)
        doc.pop("_id", None)
        doc["legacy_id"] = legacy_id
        doc.setdefault("source", "legacy_attendance")

        result = target.update_one(
            {"legacy_id": legacy_id},
            {"$setOnInsert": doc},
            upsert=True,
        )
        if result.upserted_id:
            migrated += 1
        else:
            skipped += 1

    print(f"Migrated: {migrated}")
    print(f"Skipped (already existing/invalid): {skipped}")
    print(f"Target documents: {target.count_documents({})}")
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Migration completed")


if __name__ == "__main__":
    main()
