import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient


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
COLL_NAME = os.getenv("COLL", "json_objects")

DAYS = int(os.getenv("DAYS", "30"))              # janela para série temporal
OUT_DIR = os.getenv("OUT_DIR", "out_lake_plots") # pasta de saída
TOP_SCHEMAS = int(os.getenv("TOP_SCHEMAS", "8"))


def ensure_out_dir():
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)


def get_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLL_NAME]


def plot_top_schemas(col):
    """
    1) Barras: top schemas por volume
    """
    pipeline = [
        {"$group": {"_id": "$schema", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": TOP_SCHEMAS},
    ]
    data = list(col.aggregate(pipeline))
    if not data:
        print("[plot_top_schemas] Sem dados.")
        return

    df = pd.DataFrame(data).rename(columns={"_id": "schema"})
    df = df.sort_values("count", ascending=True)  # deixa barras horizontais mais legíveis

    plt.figure()
    plt.barh(df["schema"], df["count"])
    plt.title(f"Top {TOP_SCHEMAS} schemas por volume (Data Lake)")
    plt.xlabel("Quantidade de eventos")
    plt.ylabel("Schema")
    plt.tight_layout()

    path = Path(OUT_DIR) / "01_top_schemas.png"
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def plot_volume_diario(col):
    """
    2) Linhas: volume diário (últimos DAYS dias) com base em eventDate (ISO)
    """
    dt_fim = datetime.now(timezone.utc)
    dt_ini = dt_fim - timedelta(days=DAYS)

    # Como eventDate é string ISO, faremos $match por string >= ISO do limite.
    # (ISO UTC ordena bem lexicograficamente quando formato é consistente.)
    iso_ini = dt_ini.isoformat()

    pipeline = [
        {"$match": {"eventDate": {"$gte": iso_ini}}},
        {
            "$project": {
                "day": {"$substrBytes": ["$eventDate", 0, 10]}  # YYYY-MM-DD
            }
        },
        {"$group": {"_id": "$day", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    data = list(col.aggregate(pipeline))
    if not data:
        print("[plot_volume_diario] Sem dados na janela.")
        return

    df = pd.DataFrame(data).rename(columns={"_id": "day"})
    df["day"] = pd.to_datetime(df["day"], errors="coerce")
    df = df.dropna().sort_values("day")

    plt.figure()
    plt.plot(df["day"], df["count"])
    plt.title(f"Volume diário de eventos (últimos {DAYS} dias)")
    plt.xlabel("Dia")
    plt.ylabel("Quantidade de eventos")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()

    path = Path(OUT_DIR) / "02_volume_diario.png"
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def plot_sentimento_por_filial(col):
    """
    3) Barras empilhadas: sentimento por filial (somente docs com payload.sentiment)
    """
    pipeline = [
        {"$match": {"payload.sentiment": {"$exists": True}, "branchId": {"$ne": None}}},
        {
            "$group": {
                "_id": {"branchId": "$branchId", "sentiment": "$payload.sentiment"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.branchId": 1}},
    ]
    data = list(col.aggregate(pipeline))
    if not data:
        print("[plot_sentimento_por_filial] Não há docs com payload.sentiment e branchId.")
        return

    df = pd.DataFrame(data)
    df["branchId"] = df["_id"].apply(lambda x: x.get("branchId"))
    df["sentiment"] = df["_id"].apply(lambda x: x.get("sentiment"))
    df["count"] = df["count"].astype(int)
    df = df.drop(columns=["_id"])

    pivot = df.pivot_table(index="branchId", columns="sentiment", values="count", aggfunc="sum", fill_value=0)

    # Ordena filiais
    pivot = pivot.sort_index()

    plt.figure()
    bottom = None
    x = range(len(pivot.index))

    for sentiment in pivot.columns:
        vals = pivot[sentiment].values
        if bottom is None:
            plt.bar(x, vals, label=sentiment)
            bottom = vals
        else:
            plt.bar(x, vals, bottom=bottom, label=sentiment)
            bottom = bottom + vals

    plt.title("Sentimento por filial (docs com payload.sentiment)")
    plt.xlabel("Filial (branchId)")
    plt.ylabel("Quantidade de eventos")
    plt.xticks(list(x), [str(b) for b in pivot.index])
    plt.legend()
    plt.tight_layout()

    path = Path(OUT_DIR) / "03_sentimento_por_filial_empilhado.png"
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def main():
    ensure_out_dir()
    col = get_collection()

    total = col.count_documents({})
    print(f"Conectado em {DB_NAME}.{COLL_NAME} | docs = {total}")

    plot_top_schemas(col)
    plot_volume_diario(col)
    plot_sentimento_por_filial(col)

    print(f"\nImagens geradas em: {OUT_DIR}/")


if __name__ == "__main__":
    main()