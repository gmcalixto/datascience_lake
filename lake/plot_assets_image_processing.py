import os
from io import BytesIO

import numpy as np
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
ASSETS_COLL = os.getenv("ASSETS_COLL", "assets")

# Limite de docs para evitar carregar demais em memória
LIMIT = int(os.getenv("LIMIT", "0"))  # 0 = sem limite

# Pasta para salvar o gráfico
OUT_DIR = os.getenv("OUT_DIR", "out_lake_plots")
OUT_FILE = os.getenv("OUT_FILE", "assets_image_size_hist.png")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][ASSETS_COLL]

    query = {"type": "image", "content": {"$exists": True}}
    projection = {"_id": 0, "filename": 1, "sizeBytes": 1, "contentType": 1}

    cursor = col.find(query, projection=projection)
    if LIMIT and LIMIT > 0:
        cursor = cursor.limit(LIMIT)

    sizes = []
    filenames = []
    types = []

    for doc in cursor:
        # Preferir sizeBytes armazenado; se não existir, ignora (ou poderia calcular)
        size = doc.get("sizeBytes")
        if size is None:
            continue
        sizes.append(size)
        filenames.append(doc.get("filename", ""))
        types.append(doc.get("contentType", ""))

    if not sizes:
        print("Nenhuma imagem encontrada em assets (type='image' com sizeBytes/content).")
        return

    sizes_kb = np.array(sizes, dtype=float) / 1024.0

    print(f"Imagens processadas: {len(sizes_kb)}")
    print(f"Tamanho (KB) | min={sizes_kb.min():.2f}  média={sizes_kb.mean():.2f}  max={sizes_kb.max():.2f}")

    # -----------------------------
    # Plot: histograma do tamanho em KB
    # -----------------------------
    plt.figure()
    plt.hist(sizes_kb, bins=20)
    plt.title("Distribuição do tamanho das imagens (assets)")
    plt.xlabel("Tamanho (KB)")
    plt.ylabel("Quantidade de imagens")
    plt.tight_layout()

    out_path = os.path.join(OUT_DIR, OUT_FILE)
    plt.savefig(out_path, dpi=140)
    plt.close()

    print(f"[OK] Gráfico salvo em: {out_path}")


if __name__ == "__main__":
    main()