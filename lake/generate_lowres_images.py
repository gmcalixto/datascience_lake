import os
import random
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


N = int(os.getenv("N", "100"))
OUT_DIR = os.getenv("OUT_DIR", "lake_data/images")
W = int(os.getenv("W", "96"))
H = int(os.getenv("H", "96"))
SEED = int(os.getenv("SEED", "42"))

random.seed(SEED)


def clamp(x: int) -> int:
    return max(0, min(255, x))


def rand_color():
    return (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))


def make_gradient_bg(w: int, h: int):
    # gradiente simples horizontal
    c1 = rand_color()
    c2 = rand_color()
    img = Image.new("RGB", (w, h))
    px = img.load()
    for x in range(w):
        t = x / max(1, (w - 1))
        r = int(c1[0] * (1 - t) + c2[0] * t)
        g = int(c1[1] * (1 - t) + c2[1] * t)
        b = int(c1[2] * (1 - t) + c2[2] * t)
        for y in range(h):
            px[x, y] = (r, g, b)
    return img


def add_noise(img: Image.Image, intensity: int = 25):
    px = img.load()
    w, h = img.size
    for _ in range(w * h // 3):  # ruído parcial
        x = random.randint(0, w - 1)
        y = random.randint(0, h - 1)
        r, g, b = px[x, y]
        px[x, y] = (
            clamp(r + random.randint(-intensity, intensity)),
            clamp(g + random.randint(-intensity, intensity)),
            clamp(b + random.randint(-intensity, intensity)),
        )


def add_shapes(draw: ImageDraw.ImageDraw, w: int, h: int):
    # desenha 3 a 7 formas
    for _ in range(random.randint(3, 7)):
        shape = random.choice(["rect", "ellipse", "line"])
        c = rand_color()
        if shape == "rect":
            x1, y1 = random.randint(0, w - 1), random.randint(0, h - 1)
            x2, y2 = random.randint(x1, w - 1), random.randint(y1, h - 1)
            draw.rectangle([x1, y1, x2, y2], outline=c, width=1)
        elif shape == "ellipse":
            x1, y1 = random.randint(0, w - 1), random.randint(0, h - 1)
            x2, y2 = random.randint(x1, w - 1), random.randint(y1, h - 1)
            draw.ellipse([x1, y1, x2, y2], outline=c, width=1)
        else:
            x1, y1 = random.randint(0, w - 1), random.randint(0, h - 1)
            x2, y2 = random.randint(0, w - 1), random.randint(0, h - 1)
            draw.line([x1, y1, x2, y2], fill=c, width=1)


def main():
    out = Path(OUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    # fonte padrão (se não houver, Pillow usa fallback)
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    for i in range(1, N + 1):
        img = make_gradient_bg(W, H)
        draw = ImageDraw.Draw(img)

        add_shapes(draw, W, H)
        add_noise(img, intensity=random.randint(10, 35))

        # rótulo curto (ajuda na demo de consulta)
        label = f"img_{i:03d}"
        draw.text((4, 4), label, fill=(255, 255, 255), font=font)

        # salva PNG (leve e sem perda)
        filename = out / f"{label}.png"
        img.save(filename, format="PNG", optimize=True)

    print(f"OK ✅ Geradas {N} imagens em: {out.resolve()}")


if __name__ == "__main__":
    main()