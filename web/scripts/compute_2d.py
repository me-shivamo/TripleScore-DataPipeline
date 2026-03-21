"""Reduce 768-dim embeddings to 2D using t-SNE and output questions.json."""
import json
import numpy as np
from sklearn.manifold import TSNE
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
EMBED_DIR = ROOT / "05_Embedded-Output"
OUTPUT = ROOT / "web" / "public" / "data" / "questions.json"

# Load all embedded JSON files (exclude index.json)
data = []
for f in sorted(EMBED_DIR.glob("*.json")):
    if f.name == "index.json":
        continue
    print(f"Loading {f.name}")
    data.extend(json.loads(f.read_text()))

print(f"Total questions: {len(data)}")

embeddings = np.array([q["embedding"] for q in data])

tsne = TSNE(n_components=2, random_state=42, perplexity=min(30, len(data) - 1))
coords = tsne.fit_transform(embeddings)

# Normalize to 0-1
coords[:, 0] = (coords[:, 0] - coords[:, 0].min()) / (coords[:, 0].max() - coords[:, 0].min())
coords[:, 1] = (coords[:, 1] - coords[:, 1].min()) / (coords[:, 1].max() - coords[:, 1].min())

output = []
for i, q in enumerate(data):
    output.append({
        "id": i + 1,
        "question": q["question"],
        "options": q["options"],
        "type": q["type"],
        "correct_answer": q["correct_answer"],
        "explanation": q["explanation"],
        "subject": q["subject"],
        "topic": q["topic"],
        "chapter": q["chapter"],
        "x": float(coords[i, 0]),
        "y": float(coords[i, 1]),
        "embedding": q["embedding"],
    })

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text(json.dumps(output, indent=2))
print(f"Wrote {len(output)} questions to {OUTPUT}")
