"""
Shared fixtures for all test phases.

Database isolation strategy:
  - Postgres:  separate test database (ecommerce_test). Tables created once per session,
               truncated after every test in FK-safe order.
  - MongoDB:   separate test database (ecommerce_test). Collections dropped after every test.
  - Redis:     db=1 (separate from app db=0). Flushed after every test.
  - Neo4j:     all nodes/relationships deleted after every test.

Connection defaults match docker-compose.yml. Override with env vars if needed.
"""

import os
import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import redis as redis_lib
from neo4j import GraphDatabase

load_dotenv()

from ecommerce_pipeline.db_access import DBAccess
from ecommerce_pipeline.postgres_models import (
    Base,
    Customer,
    Product,
    ProductElectronics,
    ProductClothing,
    ClothingSize,
    ClothingColor,
    ProductBooks,
    Order,
    OrderItem,
)


# ---------------------------------------------------------------------------
# Connection helpers (read env vars, fall back to docker-compose defaults)
# ---------------------------------------------------------------------------

def _pg_test_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd  = os.environ.get("POSTGRES_PASSWORD", "postgres")
    db   = os.environ.get("POSTGRES_TEST_DB", "ecommerce_test")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _mongo_test_db_name() -> str:
    return os.environ.get("MONGO_TEST_DB", "ecommerce_test")


def _mongo_host() -> str:
    return os.environ.get("MONGO_HOST", "localhost")


def _mongo_port() -> int:
    return int(os.environ.get("MONGO_PORT", "27017"))


def _redis_host() -> str:
    return os.environ.get("REDIS_HOST", "localhost")


def _redis_port() -> int:
    return int(os.environ.get("REDIS_PORT", "6379"))


def _neo4j_uri() -> str:
    host = os.environ.get("NEO4J_HOST", "localhost")
    port = os.environ.get("NEO4J_BOLT_PORT", "7687")
    return f"bolt://{host}:{port}"


def _neo4j_auth() -> tuple[str, str]:
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd  = os.environ.get("NEO4J_PASSWORD", "neo4jpassword")
    return user, pwd


# ---------------------------------------------------------------------------
# Session-scoped engine — tables created once for the entire test run
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pg_engine():
    engine = create_engine(_pg_test_url(), echo=False)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


# ---------------------------------------------------------------------------
# Function-scoped fixtures — reset state after every test
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_session_factory(pg_engine):
    """Return a sessionmaker. Truncates all tables after the test."""
    factory = sessionmaker(bind=pg_engine)
    yield factory
    # Truncate in FK-safe order (children first)
    with pg_engine.connect() as conn:
        conn.execute(text(
            "TRUNCATE TABLE order_items, orders, clothing_sizes, clothing_colors, "
            "product_electronics, product_clothing, product_books, products, customers "
            "RESTART IDENTITY CASCADE"
        ))
        conn.commit()


@pytest.fixture
def mongo_db():
    """Return pymongo Database. Drops all test collections after the test."""
    client = MongoClient(_mongo_host(), _mongo_port())
    db = client[_mongo_test_db_name()]
    yield db
    client.drop_database(_mongo_test_db_name())
    client.close()


@pytest.fixture
def redis_client():
    """Return redis.Redis on db=1. Flushes db after the test."""
    r = redis_lib.Redis(host=_redis_host(), port=_redis_port(), db=1, decode_responses=True)
    yield r
    r.flushdb()
    r.close()


@pytest.fixture
def neo4j_driver():
    """Return neo4j.Driver. Deletes all nodes/relationships before and after each test."""
    driver = GraphDatabase.driver(_neo4j_uri(), auth=_neo4j_auth())
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    yield driver
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()


# ---------------------------------------------------------------------------
# DBAccess fixtures for each phase
# ---------------------------------------------------------------------------

@pytest.fixture
def db_phase1(pg_session_factory, mongo_db):
    """Phase 1: Postgres + MongoDB only."""
    return DBAccess(pg_session_factory, mongo_db)


@pytest.fixture
def db_phase2(pg_session_factory, mongo_db, redis_client):
    """Phase 2: Postgres + MongoDB + Redis."""
    return DBAccess(pg_session_factory, mongo_db, redis_client=redis_client)


@pytest.fixture
def db(pg_session_factory, mongo_db, redis_client, neo4j_driver):
    """Phase 3 / full: all four databases."""
    return DBAccess(pg_session_factory, mongo_db, redis_client=redis_client, neo4j_driver=neo4j_driver)


# ---------------------------------------------------------------------------
# Helper functions for test setup (insert data directly, bypassing DBAccess)
# ---------------------------------------------------------------------------

def make_customer(session_factory, *, id: int = 1, name: str = "Test User",
                  email: str = "test@example.com") -> dict:
    """Insert a customer row directly and return a dict matching snapshot shape."""
    with session_factory() as session:
        c = Customer(id=id, name=name, email=email)
        session.add(c)
        session.commit()
    return {"id": id, "name": name, "email": email}


def make_product_postgres(session_factory, *,
                          id: int,
                          name: str = "Test Product",
                          price: float = 99.99,
                          stock_quantity: int = 10,
                          category: str = "electronics",
                          description: str = "A test product.",
                          category_fields: dict | None = None) -> dict:
    """Insert a product + category row into Postgres and return the product dict."""
    with session_factory() as session:
        product = Product(
            id=id,
            name=name,
            price=price,
            stock_quantity=stock_quantity,
            category=category,
            description=description,
        )
        session.add(product)
        session.flush()

        cf = category_fields or {}
        if category == "electronics":
            session.add(ProductElectronics(
                product_id=id,
                cpu=cf.get("cpu"),
                ram_gb=cf.get("ram_gb"),
                storage_gb=cf.get("storage_gb"),
                screen_inches=cf.get("screen_inches"),
            ))
        elif category == "clothing":
            session.add(ProductClothing(product_id=id, material=cf.get("material")))
            session.flush()
            for size in cf.get("sizes", []):
                session.add(ClothingSize(clothing_id=id, size=size))
            for color in cf.get("colors", []):
                session.add(ClothingColor(clothing_id=id, color=color))
        elif category == "books":
            session.add(ProductBooks(
                product_id=id,
                isbn=cf.get("isbn"),
                author=cf.get("author"),
                page_count=cf.get("page_count"),
                genre=cf.get("genre"),
            ))
        session.commit()

    return {
        "id": id,
        "name": name,
        "price": price,
        "stock_quantity": stock_quantity,
        "category": category,
        "description": description,
        "category_fields": cf,
    }


def make_product_mongo(mongo_db, *,
                       id: int,
                       name: str = "Test Product",
                       price: float = 99.99,
                       stock_quantity: int = 10,
                       category: str = "electronics",
                       description: str = "A test product.",
                       category_fields: dict | None = None) -> dict:
    """Insert a product document into MongoDB product_catalog."""
    doc = {
        "id": id,
        "name": name,
        "price": price,
        "stock_quantity": stock_quantity,
        "category": category,
        "description": description,
        "category_fields": category_fields or {},
    }
    mongo_db["product_catalog"].insert_one(dict(doc))
    return doc


def make_snapshot(mongo_db, *,
                  order_id: int,
                  customer: dict,
                  items: list[dict],
                  total_amount: float,
                  status: str = "completed",
                  created_at: str = "2025-01-01T10:00:00") -> dict:
    """Insert an order snapshot document directly into MongoDB."""
    doc = {
        "order_id": order_id,
        "customer": customer,
        "items": items,
        "total_amount": total_amount,
        "status": status,
        "created_at": created_at,
    }
    mongo_db["order_snapshots"].insert_one(dict(doc))
    return doc


# ---------------------------------------------------------------------------
# Convenience pytest fixtures wrapping the helpers above
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_customer(pg_session_factory):
    """A single customer row in Postgres."""
    return make_customer(pg_session_factory, id=1, name="Alice Johnson", email="alice@example.com")


@pytest.fixture
def sample_electronics_postgres(pg_session_factory):
    """One electronics product in Postgres."""
    return make_product_postgres(
        pg_session_factory,
        id=1,
        name="Laptop Pro",
        price=1299.99,
        stock_quantity=10,
        category="electronics",
        category_fields={"cpu": "Apple M3", "ram_gb": 18, "storage_gb": 512, "screen_inches": 16.2},
    )


@pytest.fixture
def sample_electronics_mongo(mongo_db):
    """One electronics product in MongoDB."""
    return make_product_mongo(
        mongo_db,
        id=1,
        name="Laptop Pro",
        price=1299.99,
        stock_quantity=10,
        category="electronics",
        category_fields={"cpu": "Apple M3", "ram_gb": 18, "storage_gb": 512, "screen_inches": 16.2},
    )
