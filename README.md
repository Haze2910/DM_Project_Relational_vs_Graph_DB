# DM_Project_Relational_vs_Graph_DB

A benchmarking project comparing query performance between a **relational database (PostgreSQL)** and a **graph database (Neo4j)** on a food/recipe/nutrients dataset.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.x](https://www.python.org/downloads/)
- [uv](https://github.com/astral-sh/uv) *(recommended)* or `pip`

---

## Setup

### 1. Start the databases with Docker

Spin up both PostgreSQL and Neo4j using Docker Compose:

```bash
docker compose up -d
```

Wait a few seconds for both services to be ready before proceeding.

### 2. Install Python dependencies

**With `uv` (recommended):**

```bash
uv sync
```

**With `pip`:**

```bash
pip install .
```

---

## Running the Project

Once Docker is running and dependencies are installed, run the main script:

```bash
python main.py
```

This will load the dataset into both databases, create the scales and execute the benchmark queries.

---
