# E-Commerce Polyglot Data Pipeline — Capstone Project

This is your scaffold for the capstone project. The web API is fully wired up and running. **Your job is to implement the data access layer** — the `DBAccess` class in `src/ecommerce_pipeline/db_access.py`.

---

## Project Architecture

```
POST /orders ──► router ──► DBAccess.create_order() ──► PostgreSQL (transaction)
                                                     └──► MongoDB   (snapshot)
                                                     └──► Redis     (Phase 2: inventory counter)
                                                     └──► Neo4j     (Phase 3: co-purchase graph)

GET /products/{id} ──► DBAccess.get_product() ──► Redis    (Phase 2: cache check)
                                              └──► MongoDB  (source of truth)
```

Four databases, each chosen for what it does best:

| Database | Role |
|----------|------|
| **PostgreSQL** | ACID transactions, normalized schema, analytical SQL queries |
| **MongoDB** | Flexible product catalog, denormalized order snapshots |
| **Redis** | Sub-millisecond cache, atomic inventory counters, per-user lists |
| **Neo4j** | Graph traversal for product recommendations |

---

## Project Structure

```
scaffold/
├── docker-compose.yml          ← Start all 4 databases with one command
├── .env.example                ← Copy to .env and fill in your credentials
├── pyproject.toml              ← Python dependencies
│
├── src/ecommerce_pipeline/
│   ├── db.py                   ← Database connection setup (already done)
│   ├── postgres_models.py      ← SQLAlchemy ORM models (TODO: implement tables)
│   ├── db_access.py            ← DBAccess class (TODO: implement each method)
│   │
│   ├── models/
│   │   ├── requests.py         ← Pydantic request models (already done)
│   │   └── responses.py        ← Pydantic response models (already done)
│   │
│   └── api/
│       ├── app.py              ← FastAPI app setup (already done)
│       └── routes/
│           ├── products.py     ← Product endpoints (already done)
│           ├── orders.py       ← Order endpoints (already done)
│           ├── customers.py    ← Customer endpoints (already done)
│           └── analytics.py    ← Analytics endpoints (already done)
│
├── seed_data/                  ← Put your seed data JSON files here
├── scripts/                    ← Put your seed/utility scripts here
└── tests/                      ← Put your tests here
```

---

## Getting Started

### 1. Start the databases

```bash
docker compose up -d
```

This starts PostgreSQL, MongoDB, Redis, and Neo4j. Wait a few seconds for them to initialize.

You can open the Neo4j Browser at http://localhost:7474 (login: `neo4j` / `neo4jpassword`).

### 2. Set up your Python environment

```bash
# Copy and edit the environment file
cp .env.example .env

# Install all dependencies and create the virtual environment in one step
uv sync
```

`uv sync` reads `pyproject.toml`, creates a `.venv/` directory, and installs all runtime and dev dependencies (including an editable install of the project itself). No need to activate the venv manually.

### 3. Run the API server

```bash
uv run uvicorn ecommerce_pipeline.api.app:app --reload
```

Open http://localhost:8000/docs to see all available endpoints. Every endpoint already exists — they will return `501 Not Implemented` until you implement the corresponding `DBAccess` method.

### 4. Implement the data layer

Open `src/ecommerce_pipeline/db_access.py`. Each method has:
- A docstring explaining what it should do
- A `raise NotImplementedError(...)` placeholder

Work through the phases in order:

**Phase 1 — PostgreSQL + MongoDB**
1. Define your SQLAlchemy models in `postgres_models.py`
2. Implement `create_order` — the core ACID transaction
3. Implement `get_product`, `search_products` — MongoDB reads
4. Implement `get_order`, `get_order_history` — MongoDB snapshots
5. Implement `revenue_by_category` — SQL aggregation

**Phase 2 — Redis**
6. Implement `init_inventory_counters` — sync Redis from Postgres
7. Add cache-aside logic to `get_product`
8. Implement `record_product_view`, `get_recently_viewed`

**Phase 3 — Neo4j**
9. Implement `seed_recommendation_graph` — build the co-purchase graph
10. Implement `get_recommendations` — Cypher graph traversal

---

## Interface Contract

The full interface specification (method signatures, return shapes, database schemas) is in the course materials:

```
materials/project/interface-spec.md
```

Read this before implementing anything. It tells you exactly what each method must return and what side effects it must produce.

---

## Seeding the Databases

After implementing Phase 1, load the sample data:

```bash
# Seed Postgres + MongoDB (Phase 1)
uv run python -m scripts.seed --phase 1

# Seed Redis inventory counters (Phase 2)
uv run python -m scripts.seed --phase 2

# Seed Neo4j recommendation graph (Phase 3)
uv run python -m scripts.seed --phase 3

# Seed all at once
uv run python -m scripts.seed --phase all
```

## Running Tests

```bash
uv run pytest tests/
```

Run only a specific phase:

```bash
uv run pytest tests/test_phase1_postgres.py
uv run pytest tests/test_phase1_mongo.py
uv run pytest tests/test_phase1_integration.py
uv run pytest tests/test_phase2_redis.py
uv run pytest tests/test_phase3_neo4j.py
```

Tests use separate test databases (`ecommerce_test`) and are fully isolated — each test cleans up after itself.

---

## Tips

- Run `docker compose logs -f postgres` to see Postgres logs if queries fail
- Use the `/docs` endpoint in your browser as a live test client
- For Phase 2+, confirm Redis is running with `redis-cli ping`
- For Phase 3, check Neo4j Browser at http://localhost:7474 to inspect the graph visually
