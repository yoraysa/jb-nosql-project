"""
Seed script — loads data into all databases.

Usage:
    uv run python -m scripts.seed

Prerequisites:
    Run scripts.migrate first to create database structures.

What to implement in seed():
    Phase 1: Load products.json + customers.json into Postgres and MongoDB
    Phase 2: Initialize Redis inventory counters from Postgres product stock
    Phase 3: Build Neo4j co-purchase graph from historical_orders.json

Seed data files are in the seed_data/ directory.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SEED_DIR = Path(__file__).parent.parent / "seed_data"


def seed(engine, mongo_db, redis_client=None, neo4j_driver=None):
    """Load seed data into all databases.

    Add your seeding logic here incrementally as you progress through phases.

    Args:
        engine: SQLAlchemy engine connected to Postgres
        mongo_db: pymongo Database instance
        redis_client: redis.Redis instance or None (Phase 2+)
        neo4j_driver: neo4j.Driver instance or None (Phase 3)

    Tip: Use json.load() to read the files in seed_data/:
        products = json.load(open(SEED_DIR / "products.json"))
        customers = json.load(open(SEED_DIR / "customers.json"))
        historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))
    """
    import json
    from itertools import combinations
    from sqlalchemy.orm import sessionmaker
    
    # Load seed data files
    with open(SEED_DIR / "products.json") as f:
        products = json.load(f)
    with open(SEED_DIR / "customers.json") as f:
        customers = json.load(f)
    with open(SEED_DIR / "historical_orders.json") as f:
        historical_orders = json.load(f)
    
    # ── Phase 1: Load into Postgres and MongoDB ───────────────────────────
    
    from ecommerce_pipeline.postgres_models import (
        Customer, Product, ProductElectronics, ProductClothing, 
        ProductBooks, ProductFood, ProductHome, ClothingSize, ClothingColor
    )
    
    # Create a session factory
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # Insert customers into Postgres
        for customer_data in customers:
            customer = Customer(
                id=customer_data["id"],
                name=customer_data["name"],
                email=customer_data["email"],
            )
            session.add(customer)
        
        # Insert products into Postgres
        for product_data in products:
            product = Product(
                id=product_data["id"],
                name=product_data["name"],
                price=product_data["price"],
                stock_quantity=product_data["stock_quantity"],
                category=product_data["category"],
                description=product_data.get("description"),
            )
            session.add(product)
            
            # Handle category-specific fields
            category_fields = product_data.get("category_fields", {})
            category = product_data["category"]
            
            if category == "electronics":
                electronics = ProductElectronics(
                    product_id=product_data["id"],
                    cpu=category_fields.get("cpu"),
                    ram_gb=category_fields.get("ram_gb"),
                    storage_gb=category_fields.get("storage_gb"),
                    screen_inches=category_fields.get("screen_inches"),
                )
                session.add(electronics)
            
            elif category == "clothing":
                clothing = ProductClothing(
                    product_id=product_data["id"],
                    material=category_fields.get("material"),
                )
                session.add(clothing)
                
                # Add sizes
                for size in category_fields.get("sizes", []):
                    clothing_size = ClothingSize(
                        clothing_id=product_data["id"],
                        size=size,
                    )
                    session.add(clothing_size)
                
                # Add colors
                for color in category_fields.get("colors", []):
                    clothing_color = ClothingColor(
                        clothing_id=product_data["id"],
                        color=color,
                    )
                    session.add(clothing_color)
            
            elif category == "books":
                books = ProductBooks(
                    product_id=product_data["id"],
                    isbn=category_fields.get("isbn"),
                    author=category_fields.get("author"),
                    page_count=category_fields.get("page_count"),
                    genre=category_fields.get("genre"),
                )
                session.add(books)
            
            elif category == "food":
                food = ProductFood(
                    product_id=product_data["id"],
                    weight_g=category_fields.get("weight_g"),
                    organic=1 if category_fields.get("organic") else 0,
                    allergens=category_fields.get("allergens"),
                )
                session.add(food)
            
            elif category == "home":
                home = ProductHome(
                    product_id=product_data["id"],
                    dimensions=category_fields.get("dimensions"),
                    material=category_fields.get("material"),
                    assembly_required=1 if category_fields.get("assembly_required") else 0,
                )
                session.add(home)
        
        session.commit()
        
        # Insert products into MongoDB
        product_catalog = mongo_db["product_catalog"]
        for product_data in products:
            doc = {
                "id": product_data["id"],
                "name": product_data["name"],
                "price": product_data["price"],
                "stock_quantity": product_data["stock_quantity"],
                "category": product_data["category"],
                "description": product_data.get("description"),
                "category_fields": product_data.get("category_fields", {}),
            }
            product_catalog.insert_one(doc)
        
        # ── Phase 2: Initialize Redis inventory counters ────────────────────
        
        if redis_client:
            for product_data in products:
                redis_client.set(f"inventory:{product_data['id']}", str(product_data["stock_quantity"]))
        
        # ── Phase 3: Build Neo4j co-purchase graph ─────────────────────────
        
        if neo4j_driver:
            # Create Product nodes in Neo4j
            with neo4j_driver.session() as neo_session:
                # Create/update product nodes
                for product_data in products:
                    neo_session.run(
                        """
                        MERGE (p:Product {id: $product_id})
                        SET p.name = $name, p.price = $price
                        """,
                        product_id=product_data["id"],
                        name=product_data["name"],
                        price=product_data["price"],
                    )
                
                # Create BOUGHT_TOGETHER relationships from historical orders
                for order_data in historical_orders:
                    product_ids = order_data["product_ids"]
                    
                    # For each pair of products in the order, create/update a BOUGHT_TOGETHER edge
                    for product_a, product_b in combinations(product_ids, 2):
                        # Ensure consistent direction (lower ID first) to avoid duplicate edges
                        if product_a > product_b:
                            product_a, product_b = product_b, product_a
                        
                        neo_session.run(
                            """
                            MATCH (a:Product {id: $product_a}), (b:Product {id: $product_b})
                            MERGE (a)-[r:BOUGHT_TOGETHER]->(b)
                            ON CREATE SET r.count = 1
                            ON MATCH SET r.count = r.count + 1
                            """,
                            product_a=product_a,
                            product_b=product_b,
                        )
    
    finally:
        session.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _pg_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _mongo_db():
    from pymongo import MongoClient

    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    db = os.environ.get("MONGO_DB", "ecommerce")
    return MongoClient(host, port)[db]


def _redis_client():
    host = os.environ.get("REDIS_HOST")
    if not host:
        return None
    import redis

    port = int(os.environ.get("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, decode_responses=True)


def _neo4j_driver():
    host = os.environ.get("NEO4J_HOST")
    pwd = os.environ.get("NEO4J_PASSWORD")
    if not host or not pwd:
        return None
    from neo4j import GraphDatabase

    port = os.environ.get("NEO4J_BOLT_PORT", "7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    return GraphDatabase.driver(f"bolt://{host}:{port}", auth=(user, pwd))


def main():
    from sqlalchemy import create_engine

    engine = create_engine(_pg_url(), echo=False)
    mongo_db = _mongo_db()
    redis_client = _redis_client()
    neo4j_driver = _neo4j_driver()

    print("Seeding databases...")
    seed(engine, mongo_db, redis_client, neo4j_driver)
    print("Seeding complete.")

    if neo4j_driver:
        neo4j_driver.close()
    engine.dispose()


if __name__ == "__main__":
    main()
