import os
from pymongo import MongoClient, ASCENDING, DESCENDING


MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")

MONGO_URI = os.getenv(
    "MONGO_URI",
    f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
)

DB_NAME = os.getenv("MONGO_DB", "datalake")


def ensure_collection(db, name: str):
    if name not in db.list_collection_names():
        db.create_collection(name)
        print(f"[OK] coleção criada: {name}")
    else:
        print(f"[OK] coleção já existe: {name}")


def ensure_indexes(db):
    assets = db["assets"]
    json_objects = db["json_objects"]

    # -------- assets indexes --------
    assets.create_index([("type", ASCENDING), ("source", ASCENDING), ("ingestedAt", DESCENDING)],
                        name="ix_assets_type_source_ingestedAt")

    assets.create_index([("tags", ASCENDING)], name="ix_assets_tags")

    assets.create_index([("branchId", ASCENDING), ("date", DESCENDING)],
                        name="ix_assets_branch_date")

    assets.create_index([("sha256", ASCENDING)],
                        name="ux_assets_sha256",
                        unique=True,
                        sparse=True)

    assets.create_index([("filename", ASCENDING)], name="ix_assets_filename")

    # -------- json_objects indexes --------
    json_objects.create_index([("schema", ASCENDING), ("source", ASCENDING), ("ingestedAt", DESCENDING)],
                              name="ix_json_schema_source_ingestedAt")

    json_objects.create_index([("branchId", ASCENDING), ("eventDate", DESCENDING)],
                              name="ix_json_branch_eventDate")

    print("[OK] índices garantidos (create_index é idempotente por name).")


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Cria coleções principais
    ensure_collection(db, "json_objects")
    ensure_collection(db, "assets")

    # Garante índices
    ensure_indexes(db)

    # “Ping” simples
    db.command("ping")
    print(f"\nDatalake inicializado ✅ (db={DB_NAME})")


if __name__ == "__main__":
    main()