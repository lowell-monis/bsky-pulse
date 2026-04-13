# bsky-pulse

> **Live sentiment analysis pipeline for the Bluesky (AT Protocol) firehose**

bsky-pulse is an automated, asynchronous data pipeline that streams live posts from Bluesky based on configurable keywords, stores them in either a local JSON file or a MongoDB/Atlas backend, and produces sentiment visualisations (word clouds, VADER sentiment scores, frequency charts) via a Jupyter Notebook.

---

## Table of Contents

- [Features](#features)
- [Repository Structure](#repository-structure)
- [Quick Start](#quick-start)
- [Configuration & Secrets](#configuration--secrets)
- [Scripts](#scripts)
- [Decision Guide](#decision-guide)
- [Responsible AI Use Disclosure](#responsible-ai-use-disclosure)

---

## Features

- **Real-time firehose streaming** via the Bluesky Jetstream WebSocket (`asyncio` + `websockets`)
- **Dual storage backend** — lightweight local JSON *or* scalable MongoDB/Atlas
- **Misra-Gries heavy hitter pruning** — automatically evicts low-frequency keywords to keep storage lean
- **Export** — dump collected posts to CSV or JSON for offline analysis
- **Migration script** — push a local MongoDB collection to an Atlas cluster with duplicate skipping
- **Jupyter Notebook** — word cloud, VADER sentiment distribution, and rolling sentiment-over-time charts
- **Secrets management** — all credentials are loaded from a `.env` file; a `.env.example` template is provided

---

## Repository Structure

```
bsky-pulse/
├── .env.example          # Secrets template — copy to .env and fill in values
├── .gitignore
├── pyproject.toml        # Project metadata & dependencies
├── README.md
├── data/                 # Local JSON storage (git-ignored except .gitkeep)
├── notebooks/
│   └── exploration.ipynb # Data exploration & sentiment visualisation
└── src/
    ├── collect.py        # Async firehose collector (JSON or MongoDB)
    ├── export.py         # Export / prune posts
    ├── migrate.py        # Local MongoDB → Atlas migration
    └── misra_gries.py    # Misra-Gries heavy hitter algorithm
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -e .
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env — set STORAGE_BACKEND, JSON_PATH or MONGO_URI, etc.
```

### 3. Collect posts

```bash
# Stream indefinitely into a local JSON file (default)
python src/collect.py

# Stream with custom keywords, stop after 200 posts
python src/collect.py --keywords "climate change" "renewable energy" --limit 200

# Use MongoDB backend
STORAGE_BACKEND=mongodb python src/collect.py
```

### 4. Export or prune

```bash
# Export latest 1000 posts to CSV
python src/export.py --format csv --out data/export.csv --limit 1000

# Prune posts older than 7 days, keeping top-20 keyword clusters
python src/export.py --prune --days 7 --top-k 20
```

### 5. Migrate local MongoDB → Atlas

```bash
# Fill in LOCAL_MONGO_URI and CLUSTER_URI in .env first
python src/migrate.py
```

### 6. Open the notebook

```bash
jupyter notebook notebooks/exploration.ipynb
```

---

## Configuration & Secrets

All credentials and runtime settings are read from a `.env` file in the project root.  
**Never commit your `.env` file** — it is already listed in `.gitignore`.

| Variable | Default | Description |
|---|---|---|
| `STORAGE_BACKEND` | `json` | `json` or `mongodb` |
| `JSON_PATH` | `data/posts.json` | Path for local JSON store |
| `MONGO_URI` | *(empty)* | Atlas connection string (X.509 auth) |
| `CERT_FILE_PATH` | *(empty)* | Absolute path to X.509 `.pem` file |
| `LOCAL_MONGO_URI` | `mongodb://localhost:27017` | Local MongoDB for migration |
| `CLUSTER_URI` | *(empty)* | Destination Atlas cluster URI |
| `JETSTREAM_URI` | Jetstream east endpoint | Bluesky Jetstream WebSocket URL |

See `.env.example` for a fully commented template.

---

## Scripts

### `src/collect.py`
Connects to the Bluesky Jetstream and captures English posts matching any of the configured keywords.  
Writes to the configured backend on every match and tracks keyword frequencies with the Misra-Gries algorithm.

```
usage: collect.py [-h] [--keywords KW [KW ...]] [--limit N]
```

### `src/export.py`
Reads the post store and exports to CSV or JSON, or prunes low-frequency / old posts.

```
usage: export.py [-h] [--backend {json,mongodb}] [--format {csv,json}]
                 [--out PATH] [--limit N]
                 [--prune] [--days N] [--top-k N]
```

### `src/migrate.py`
One-shot migration from a local MongoDB collection to an Atlas cluster.  
Duplicate documents (same `_id`) are automatically skipped.

### `src/misra_gries.py`
Pure-Python implementation of the Misra-Gries frequent-items algorithm.  
Import and use directly:

```python
from misra_gries import MisraGries
mg = MisraGries(k=20)
mg.add("llm")
print(mg.heavy_hitters())
```

---

## Decision Guide

### Storage: JSON vs MongoDB

| Factor | Local JSON | MongoDB / Atlas |
|---|---|---|
| Setup effort | None | Requires Atlas account + X.509 cert |
| Scalability | Limited by disk / RAM | Horizontally scalable |
| Query flexibility | Manual filtering | Full aggregation pipeline |
| Cost | Free | Free tier up to 512 MB |
| Best for | Local dev, small runs | Production, multi-session, large datasets |

**Recommendation:** Start with JSON for local testing. Switch to MongoDB when you need persistence across reboots or multi-machine access.

---

### Hosting: Oracle Cloud vs Local

| Factor | Local machine | Oracle Cloud (Always Free) |
|---|---|---|
| Cost | $0 | $0 (Always Free tier) |
| Uptime | Depends on your machine | 24/7 |
| Setup | Run `python src/collect.py` | SSH, install deps, run as a `systemd` service |
| Latency to Jetstream | Variable | Low (Oracle US East ≈ Jetstream US East) |
| Maintenance | None | OS updates, firewall rules |

**Recommendation:** Use local mode during development. Deploy to an Oracle Cloud Always Free ARM instance for continuous collection.

---

### Visualisation: Live Web App vs Static Notebook

| Factor | Jupyter Notebook | Live Web App |
|---|---|---|
| Development speed | Fast | Slower (requires a framework) |
| Interactivity | Cell-by-cell | Real-time dashboard |
| Deployment | Local only | Requires hosting |
| Best for | Exploration, one-off runs | Public-facing, auto-refreshing |

**Recommendation:** Use the provided notebook for exploration. Graduate to a Streamlit or Dash app once the pipeline is stable.

---

## Responsible AI Use Disclosure

I intend to use GitHub Copilot and other LLM tools to scaffold the asynchronous boilerplate for the Bluesky firehose and to assist in drafting the automation scripts for data ingestion and continuous integration of the data into visualisations. While AI will be used to accelerate the development of the data pipeline and cloud deployment commands, I remain the sole party responsible for the logic, security, and verification of the code. All AI-generated snippets will be manually reviewed, tested for performance bottlenecks (especially regarding database latency), and revised to ensure they meet the specific requirements of a comprehensive, ethical, and refined industry standard. I do not treat AI output as an unquestioned authority and will verify the final workflow through live testing.
