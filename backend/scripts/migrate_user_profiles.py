from config import users


def run():
    updated = 0
    for doc in users.find({}):
        username = (doc.get("username") or "").strip()
        if not username:
            continue

        patch = {
            "fullName": (doc.get("fullName") or "").strip() or username,
            "email": (doc.get("email") or "").strip() or f"{username}@chs.local",
            "phone": (doc.get("phone") or "").strip(),
            "address": (doc.get("address") or "").strip(),
            "bio": (doc.get("bio") or "").strip(),
            "avatarUrl": (doc.get("avatarUrl") or "").strip(),
            "twoFactorEnabled": bool(doc.get("twoFactorEnabled", False)),
            "updatedAt": (doc.get("updatedAt") or doc.get("updated_at") or doc.get("created_at") or ""),
        }
        users.update_one({"_id": doc["_id"]}, {"$set": patch})
        updated += 1

    print(f"Updated {updated} user documents.")


if __name__ == "__main__":
    run()
