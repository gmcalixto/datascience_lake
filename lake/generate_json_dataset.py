import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


N = int(os.getenv("N", "10000"))
OUT_FILE = os.getenv("OUT_FILE", "lake_data/json/events_10000.jsonl")
SAMPLE_DIR = os.getenv("SAMPLE_DIR", "lake_data/json/sample_20")
SEED = int(os.getenv("SEED", "42"))

random.seed(SEED)


# Vocabulários simples (PT-BR) para compor textos variados
BLOG_TOPICS = [
    "arquitetura de dados", "python", "devops", "segurança", "cloud", "engenharia de software",
    "data warehouse", "data lake", "etl", "observabilidade", "kafka", "mongodb"
]

HASHTAGS = [
    "#dados", "#analytics", "#cloud", "#devops", "#python", "#bigdata",
    "#engenhariadesoftware", "#etl", "#datalake", "#datawarehouse"
]

EMOJIS = ["", "", "", "🙂", "😅", "🔥", "🚀", "🤔", "✅", "📊", "🧠"]

CITIES = [
    ("São Paulo", "SP"), ("Campinas", "SP"), ("Santos", "SP"),
    ("São José dos Campos", "SP"), ("Sorocaba", "SP")
]

PLATFORMS = ["twitter_like", "instagram_like", "linkedin_like", "forum_like"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Edge", "Safari"]
OS_LIST = ["Windows", "Linux", "macOS", "Android", "iOS"]

PRODUCT_CATEGORIES = ["Eletrônicos", "Vestuário", "Calçados", "Utilidades", "Acessórios"]
TICKET_CATEGORIES = ["Login", "Pagamento", "Entrega", "Bug", "Dúvida", "Performance"]
TICKET_PRIORITY = ["baixa", "média", "alta", "crítica"]

SENTIMENTS = ["positivo", "neutro", "negativo"]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def rand_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def rand_user():
    # Usuário “fake” sem PII real
    return {
        "userId": str(uuid.uuid4()),
        "handle": f"user_{random.randint(1000, 999999)}",
        "segment": random.choice(["visitante", "cadastrado", "premium", "colaborador"]),
    }


def rand_context():
    city, uf = random.choice(CITIES)
    return {
        "device": {
            "type": random.choice(DEVICE_TYPES),
            "os": random.choice(OS_LIST),
            "browser": random.choice(BROWSERS),
        },
        "location": {"city": city, "uf": uf},
        "app": {"version": f"{random.randint(1,4)}.{random.randint(0,20)}.{random.randint(0,50)}"},
    }


def make_blog_comment(ts: datetime):
    topic = random.choice(BLOG_TOPICS)
    sentiment = random.choices(SENTIMENTS, weights=[0.45, 0.35, 0.20])[0]
    words = random.randint(12, 40)
    text = f"Comentário sobre {topic}: " + " ".join(
        random.choice(BLOG_TOPICS + ["ótimo", "confuso", "bem explicado", "faltou exemplo", "prático", "didático"])
        for _ in range(words)
    )
    return {
        "schema": "blog_comment_v1",
        "source": "blog",
        "eventDate": iso(ts),
        "payload": {
            "postId": f"post_{random.randint(1, 500)}",
            "commentId": str(uuid.uuid4()),
            "text": text + " " + random.choice(EMOJIS),
            "sentiment": sentiment,
            "likes": random.randint(0, 200),
            "replies": random.randint(0, 40),
            "topic": topic,
        },
    }


def make_social_post(ts: datetime):
    topic = random.choice(BLOG_TOPICS)
    platform = random.choice(PLATFORMS)
    sentiment = random.choices(SENTIMENTS, weights=[0.40, 0.40, 0.20])[0]
    tag_count = random.randint(1, 4)
    tags = random.sample(HASHTAGS, tag_count)
    words = random.randint(8, 25)

    text = " ".join(
        random.choice([topic, "aprendi", "testando", "implementando", "dica", "pipeline", "query", "dashboard", "modelo"])
        for _ in range(words)
    )
    return {
        "schema": "social_post_v1",
        "source": platform,
        "eventDate": iso(ts),
        "payload": {
            "postId": str(uuid.uuid4()),
            "text": f"{text} {' '.join(tags)} {random.choice(EMOJIS)}",
            "sentiment": sentiment,
            "shares": random.randint(0, 500),
            "comments": random.randint(0, 200),
            "views": random.randint(50, 50000),
            "tags": tags,
            "topic": topic,
        },
    }


def make_product_review(ts: datetime):
    category = random.choice(PRODUCT_CATEGORIES)
    rating = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.08, 0.20, 0.35, 0.32])[0]
    sentiment = "positivo" if rating >= 4 else ("neutro" if rating == 3 else "negativo")

    pros = random.sample(["qualidade", "preço", "entrega", "durabilidade", "design", "conforto"], random.randint(1, 3))
    cons = random.sample(["atraso", "tamanho", "bateria", "embalagem", "suporte", "ruído"], random.randint(0, 2))

    return {
        "schema": "product_review_v1",
        "source": "ecommerce",
        "eventDate": iso(ts),
        "payload": {
            "reviewId": str(uuid.uuid4()),
            "productId": f"prod_{random.randint(1, 2000)}",
            "category": category,
            "rating": rating,
            "sentiment": sentiment,
            "title": random.choice(["Recomendo", "Bom, mas poderia ser melhor", "Não gostei", "Excelente custo-benefício"]),
            "pros": pros,
            "cons": cons,
            "verifiedPurchase": random.choice([True, False]),
        },
    }


def make_support_ticket(ts: datetime):
    category = random.choice(TICKET_CATEGORIES)
    priority = random.choices(TICKET_PRIORITY, weights=[0.45, 0.35, 0.15, 0.05])[0]
    status = random.choices(["aberto", "em_atendimento", "resolvido", "cancelado"], weights=[0.35, 0.25, 0.35, 0.05])[0]
    sla_hours = {"baixa": 72, "média": 48, "alta": 24, "crítica": 4}[priority]

    return {
        "schema": "support_ticket_v1",
        "source": "helpdesk",
        "eventDate": iso(ts),
        "payload": {
            "ticketId": str(uuid.uuid4()),
            "category": category,
            "priority": priority,
            "status": status,
            "slaHours": sla_hours,
            "subject": f"{category} - solicitação {random.randint(1000, 9999)}",
            "description": " ".join(random.choice(BLOG_TOPICS + TICKET_CATEGORIES + ["erro", "lento", "não acessa", "timeout", "atualização"]) for _ in range(random.randint(12, 30))),
            "resolutionTimeMin": random.randint(5, 600) if status == "resolvido" else None,
        },
    }


def make_event_log(ts: datetime):
    event = random.choice(["page_view", "add_to_cart", "purchase", "login", "logout", "api_call", "error"])
    level = "ERROR" if event == "error" else random.choice(["INFO", "DEBUG", "WARN"])
    latency_ms = random.randint(10, 4000) if event in ["api_call", "page_view"] else None
    http = {
        "method": random.choice(["GET", "POST", "PUT"]),
        "status": random.choice([200, 200, 200, 201, 204, 400, 401, 403, 404, 500]),
        "path": random.choice(["/login", "/checkout", "/products", "/api/v1/items", "/api/v1/orders"]),
    } if event in ["api_call", "error"] else None

    return {
        "schema": "app_event_v1",
        "source": "webapp",
        "eventDate": iso(ts),
        "payload": {
            "eventId": str(uuid.uuid4()),
            "eventType": event,
            "level": level,
            "latencyMs": latency_ms,
            "http": http,
            "message": " ".join(random.choice(["ok", "processado", "requisição", "falha", "cache", "db", "fila", "retry"]) for _ in range(random.randint(6, 16))),
        },
    }


GENERATORS = [
    (make_blog_comment, 0.25),
    (make_social_post, 0.25),
    (make_product_review, 0.20),
    (make_support_ticket, 0.15),
    (make_event_log, 0.15),
]


def pick_generator():
    r = random.random()
    acc = 0.0
    for fn, w in GENERATORS:
        acc += w
        if r <= acc:
            return fn
    return GENERATORS[-1][0]


def main():
    out_path = Path(OUT_FILE)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Janela temporal: últimos 90 dias
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=90)

    with out_path.open("w", encoding="utf-8") as f:
        for i in range(N):
            ts = rand_dt(start, end)
            branch_id = random.randint(1, 5)  # suas 5 filiais

            base = pick_generator()(ts)
            # Envelopa com campos comuns de data lake
            doc = {
                "id": str(uuid.uuid4()),
                "schema": base["schema"],
                "source": base["source"],
                "branchId": branch_id,
                "eventDate": base["eventDate"],
                "user": rand_user(),
                "context": rand_context(),
                "payload": base["payload"],
                "ingestedAt": iso(end),
            }

            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

            if (i + 1) % 1000 == 0:
                print(f"[OK] gerados {i+1}/{N}")

    # Opcional: gerar 20 arquivos separados para testar ingestão “1 arquivo por doc”
    sample_dir = Path(SAMPLE_DIR)
    sample_dir.mkdir(parents=True, exist_ok=True)
    with out_path.open("r", encoding="utf-8") as fin:
        for idx in range(20):
            line = fin.readline()
            if not line:
                break
            (sample_dir / f"sample_{idx+1:02d}.json").write_text(line, encoding="utf-8")

    print(f"\nDataset gerado ✅")
    print(f"- JSONL: {out_path} ({N} linhas)")
    print(f"- Amostra: {sample_dir}/ (20 arquivos)")


if __name__ == "__main__":
    main()