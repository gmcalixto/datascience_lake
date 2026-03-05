import os
import json
import hashlib
from datetime import datetime, timezone, date
from pathlib import Path

from pymongo import MongoClient
from bson.binary import Binary


# -----------------------------
# Config
# -----------------------------
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")

MONGO_URI = os.getenv(
    "MONGO_URI",
    f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
)

DB_NAME = os.getenv("MONGO_DB", "datalake")

JSON_DIR = os.getenv("JSON_DIR", "lake_data/json/sample_20")  # default: amostra
IMG_DIR = os.getenv("IMG_DIR", "lake_data/images")

MAX_IMAGE_BYTES = int(os.getenv("MAX_IMAGE_BYTES", str(16 * 1024 * 1024)))  # 16MB
SOURCE_NAME = os.getenv("SOURCE_NAME", "local_drop")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def guess_branch_and_date_from_filename(name: str):
    """
    Opcional: tenta extrair branchId e date do nome do arquivo.
    Exemplos aceitos:
      - "branch3_2026-03-05_img_001.png" -> branchId=3, date=2026-03-05
      - "filial2_2026-02-10_nota.jpg"    -> branchId=2, date=2026-02-10
    Se não casar, retorna (None, None).
    """
    lower = name.lower()

    branch_id = None
    for key in ["branch", "filial"]:
        if key in lower:
            # pega dígitos logo após "branch" ou "filial"
            idx = lower.find(key) + len(key)
            digits = ""
            while idx < len(lower) and lower[idx].isdigit():
                digits += lower[idx]
                idx += 1
            if digits:
                branch_id = int(digits)
            break

    # data no formato YYYY-MM-DD em qualquer posição
    dt = None
    for i in range(len(name) - 9):
        s = name[i:i+10]
        try:
            dt = datetime.strptime(s, "%Y-%m-%d").date()
            break
        except ValueError:
            continue

    return branch_id, dt


def ingest_json_files(json_col, ingested_at: datetime):
    json_path = Path(JSON_DIR)
    if not json_path.exists():
        print(f"[JSON] pasta não encontrada: {JSON_DIR}")
        return 0

    inserted = 0
    for p in sorted(json_path.glob("*.json")):
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[JSON] inválido: {p.name} | erro: {e}")
            continue

        wrapped = {
            "schema": doc.get("schema", "unknown_v1"),
            "source": doc.get("source", SOURCE_NAME),
            "branchId": doc.get("branchId"),
            "eventDate": doc.get("eventDate"),
            "payload": doc.get("payload", doc),
            "filename": p.name,
            "ingestedAt": doc.get("ingestedAt") or ingested_at.isoformat(),
        }

        json_col.insert_one(wrapped)
        inserted += 1

    print(f"[JSON] inseridos: {inserted}")
    return inserted


def ingest_images_as_binary(assets_col, ingested_at: datetime):
    img_path = Path(IMG_DIR)
    if not img_path.exists():
        print(f"[IMG] pasta não encontrada: {IMG_DIR}")
        return 0

    exts = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".webp": "image/webp"}
    inserted = 0
    skipped_big = 0
    skipped_dup = 0

    for p in sorted(img_path.glob("*.*")):
        suffix = p.suffix.lower()
        if suffix not in exts:
            continue

        data = p.read_bytes()
        size = len(data)

        if size >= MAX_IMAGE_BYTES:
            skipped_big += 1
            print(f"[IMG] pulado (>= {MAX_IMAGE_BYTES} bytes): {p.name} ({size})")
            continue

        digest = sha256_bytes(data)

        if assets_col.find_one({"sha256": digest}):
            skipped_dup += 1
            continue

        branch_id, dt = guess_branch_and_date_from_filename(p.name)

        doc = {
            "type": "image",
            "source": SOURCE_NAME,
            "filename": p.name,
            "contentType": exts[suffix],
            "sizeBytes": size,
            "sha256": digest,
            "tags": [],
            "branchId": branch_id,
            "date": dt.isoformat() if isinstance(dt, date) else None,
            "content": Binary(data),
            "ingestedAt": ingested_at,
        }

        assets_col.insert_one(doc)
        inserted += 1

    print(f"[IMG] inseridos: {inserted} | duplicados ignorados: {skipped_dup} | grandes ignorados: {skipped_big}")
    return inserted


def main():
    ingested_at = datetime.now(timezone.utc)

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    json_col = db["json_objects"]
    assets_col = db["assets"]

    # opcional: garante índice único por sha256 (idempotência)
    # Se você já criou no init, isso só reforça.
    assets_col.create_index("sha256", unique=True, sparse=True, name="ux_assets_sha256")

    print(f"Conectado em Mongo: {DB_NAME}")
    ingest_images_as_binary(assets_col, ingested_at)
    print("\nOK ✅")


if __name__ == "__main__":
    main()