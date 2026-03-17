"""
Seed script for the ecommerce-pipeline project.

Loads JSON seed data into all databases:
  Phase 1: PostgreSQL (normalized) + MongoDB (denormalized)
  Phase 2: Redis (inventory counters)
  Phase 3: Neo4j (recommendation graph)

Usage:
    python -m scripts.seed --phase 1
    python -m scripts.seed --phase 2
    python -m scripts.seed --phase 3
    python -m scripts.seed --phase all
"""

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SEED_DIR = Path(__file__).parent.parent / "seed_data"


def load_json(filename: str) -> list[dict]:
    path = SEED_DIR / filename
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Phase 1: PostgreSQL + MongoDB
# ---------------------------------------------------------------------------

def seed_phase1() -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from pymongo import MongoClient, ASCENDING

    # Import student-implemented models — this will raise clearly if not yet built
    from ecommerce_pipeline.postgres_models import (
        Base,
        Customer,
        Product,
        ProductElectronics,
        ProductClothing,
        ClothingSize,
        ClothingColor,
        ProductBooks,
    )

    pg_url = _pg_url()
    engine = create_engine(pg_url, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    products = load_json("products.json")
    customers = load_json("customers.json")

    # ---- Postgres ----
    with Session() as session:
        # Customers
        for c in customers:
            existing = session.get(Customer, c["id"])
            if existing is None:
                session.add(Customer(id=c["id"], name=c["name"], email=c["email"]))
        session.commit()
        print(f"  Postgres: inserted {len(customers)} customers")

        # Products (only electronics, clothing, books have Postgres tables)
        pg_products = [p for p in products if p["category"] in ("electronics", "clothing", "books")]
        inserted_products = 0
        for p in pg_products:
            existing = session.get(Product, p["id"])
            if existing is not None:
                continue
            product = Product(
                id=p["id"],
                name=p["name"],
                price=p["price"],
                stock_quantity=p["stock_quantity"],
                category=p["category"],
                description=p["description"],
            )
            session.add(product)
            session.flush()  # get the id assigned before adding category rows

            cf = p["category_fields"]
            if p["category"] == "electronics":
                session.add(ProductElectronics(
                    product_id=p["id"],
                    cpu=cf.get("cpu"),
                    ram_gb=cf.get("ram_gb"),
                    storage_gb=cf.get("storage_gb"),
                    screen_inches=cf.get("screen_inches"),
                ))
            elif p["category"] == "clothing":
                session.add(ProductClothing(product_id=p["id"], material=cf.get("material")))
                session.flush()
                for size in cf.get("sizes", []):
                    session.add(ClothingSize(clothing_id=p["id"], size=size))
                for color in cf.get("colors", []):
                    session.add(ClothingColor(clothing_id=p["id"], color=color))
            elif p["category"] == "books":
                session.add(ProductBooks(
                    product_id=p["id"],
                    isbn=cf.get("isbn"),
                    author=cf.get("author"),
                    page_count=cf.get("page_count"),
                    genre=cf.get("genre"),
                ))
            inserted_products += 1

        session.commit()
        print(f"  Postgres: inserted {inserted_products} products (electronics, clothing, books)")

    # ---- MongoDB ----
    mongo_db = _mongo_db()
    catalog = mongo_db["product_catalog"]

    # Ensure unique index on the numeric `id` field
    catalog.create_index([("id", ASCENDING)], unique=True)

    inserted_mongo = 0
    for p in products:
        if catalog.find_one({"id": p["id"]}) is None:
            catalog.insert_one(dict(p))
            inserted_mongo += 1

    print(f"  MongoDB:  inserted {inserted_mongo} products into product_catalog")


# ---------------------------------------------------------------------------
# Phase 2: Redis inventory counters
# ---------------------------------------------------------------------------

def seed_phase2() -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ecommerce_pipeline.postgres_models import Base, Product
    from ecommerce_pipeline.db_access import DBAccess

    pg_url = _pg_url()
    engine = create_engine(pg_url, echo=False)
    Session = sessionmaker(bind=engine)

    redis_client = _redis_client()
    mongo_db = _mongo_db()

    db = DBAccess(Session, mongo_db, redis_client=redis_client)
    db.init_inventory_counters()

    # Count how many keys were set
    with Session() as session:
        count = session.query(Product).count()
    print(f"  Redis: initialized {count} inventory counters (inventory:{{product_id}})")


# ---------------------------------------------------------------------------
# Phase 3: Neo4j recommendation graph
# ---------------------------------------------------------------------------

def seed_phase3() -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ecommerce_pipeline.db_access import DBAccess

    pg_url = _pg_url()
    engine = create_engine(pg_url, echo=False)
    Session = sessionmaker(bind=engine)

    redis_client = _redis_client()
    mongo_db = _mongo_db()
    neo4j_driver = _neo4j_driver()

    db = DBAccess(Session, mongo_db, redis_client=redis_client, neo4j_driver=neo4j_driver)

    orders = load_json("historical_orders.json")
    # seed_recommendation_graph expects list of {"order_id": int, "product_ids": [int]}
    graph_orders = [{"order_id": o["order_id"], "product_ids": o["product_ids"]} for o in orders]
    db.seed_recommendation_graph(graph_orders)

    print(f"  Neo4j: seeded recommendation graph from {len(orders)} historical orders")

    neo4j_driver.close()


# ---------------------------------------------------------------------------
# Connection helpers — read env vars with defaults matching docker-compose
# ---------------------------------------------------------------------------

def _pg_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB", "ecommerce")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd  = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _mongo_db():
    from pymongo import MongoClient
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    db   = os.environ.get("MONGO_DB", "ecommerce")
    return MongoClient(host, port)[db]


def _redis_client():
    import redis
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, decode_responses=True)


def _neo4j_driver():
    from neo4j import GraphDatabase
    host = os.environ.get("NEO4J_HOST", "localhost")
    port = os.environ.get("NEO4J_BOLT_PORT", "7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd  = os.environ.get("NEO4J_PASSWORD", "neo4jpassword")
    return GraphDatabase.driver(f"bolt://{host}:{port}", auth=(user, pwd))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the ecommerce-pipeline databases")
    parser.add_argument(
        "--phase",
        choices=["1", "2", "3", "all"],
        default="all",
        help="Which phase to seed (default: all)",
    )
    args = parser.parse_args()

    phases = ["1", "2", "3"] if args.phase == "all" else [args.phase]

    for phase in phases:
        print(f"\nSeeding Phase {phase}...")
        if phase == "1":
            seed_phase1()
        elif phase == "2":
            seed_phase2()
        elif phase == "3":
            seed_phase3()

    print("\nDone.")


if __name__ == "__main__":
    main()
