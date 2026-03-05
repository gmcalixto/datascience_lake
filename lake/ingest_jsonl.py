import os
import json
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError


MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")

MONGO_URI = os.getenv(
    "MONGO_URI",
    f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
)


DB_NAME = os.getenv("MONGO_DB", "datalake")
COLL_NAME = os.getenv("COLL", "json_objects")

JSONL_FILE = os.getenv("JSONL_FILE", "lake_data/json/events_10000.jsonl")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# Se quiser ingestão idempotente, crie índice único em "id" e mantenha IGNORE_DUPLICATES=True
IGNORE_DUPLICATES = os.getenv("IGNORE_DUPLICATES", "true").lower() == "true"


def ensure_unique_id_index(col):
    # Índice único em id para evitar duplicar em re-ingestões
    col.create_index("id", unique=True, name="ux_json_objects_id")


def main():
    path = Path(JSONL_FILE)
    if not path.exists():
        raise SystemExit(f"Arquivo não encontrado: {path}")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLL_NAME]

    if IGNORE_DUPLICATES:
        ensure_unique_id_index(col)

    total = 0
    batch_ops = []
    ingested_at = datetime.now(timezone.utc)

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[WARN] Linha {line_no} inválida: {e}")
                continue

            # Garantias mínimas de envelope
            doc.setdefault("ingestedAt", ingested_at.isoformat())

            batch_ops.append(InsertOne(doc))

            if len(batch_ops) >= BATCH_SIZE:
                total += flush_batch(col, batch_ops)
                batch_ops = []
                print(f"[OK] inseridos até agora: {total}")

    if batch_ops:
        total += flush_batch(col, batch_ops)

    print(f"\nConcluído ✅ Total inserido (contando duplicados ignorados): {total}")
    print(f"Collection: {DB_NAME}.{COLL_NAME}")


def flush_batch(col, ops):
    try:
        res = col.bulk_write(ops, ordered=False)
        return res.inserted_count
    except BulkWriteError as bwe:
        # Se houver duplicados por índice único, o bulk pode trazer erros,
        # mas ainda assim terá inserido parte do lote.
        inserted = bwe.details.get("nInserted", 0)
        if IGNORE_DUPLICATES:
            # filtra erros de duplicidade e segue
            dup_errors = [e for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000]
            other_errors = [e for e in bwe.details.get("writeErrors", []) if e.get("code") != 11000]
            if other_errors:
                print(f"[ERRO] Falhas não-duplicidade no lote: {other_errors[:3]} (mostrando 3)")
                raise
            print(f"[INFO] Duplicados ignorados no lote: {len(dup_errors)}")
            return inserted
        else:
            raise


if __name__ == "__main__":
    main()