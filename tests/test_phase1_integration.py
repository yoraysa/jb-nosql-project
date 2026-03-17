"""
Phase 1 — Integration tests (Postgres + MongoDB cross-DB flows).

Tests verify behavior that spans both databases:
  - create_order writes to Postgres AND saves a MongoDB snapshot
  - Snapshot is consistent with Postgres order data
  - Failed order (insufficient stock) writes nothing to MongoDB
  - Snapshot preserves price at time of order even if MongoDB price changes later
"""

import pytest

from tests.conftest import make_customer, make_product_postgres, make_product_mongo


class TestFullOrderFlow:
    def test_create_order_writes_to_both_databases(self, db_phase1, pg_session_factory, mongo_db):
        """create_order produces a Postgres order row AND a MongoDB order snapshot."""
        from ecommerce_pipeline.postgres_models import Order

        make_customer(pg_session_factory, id=1, email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=50.00, stock_quantity=10, category="electronics")
        make_product_mongo(mongo_db, id=1, price=50.00, category="electronics", category_fields={})

        result = db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 2}])
        order_id = result["order_id"]

        # Postgres: order row exists
        with pg_session_factory() as session:
            pg_order = session.get(Order, order_id)
            assert pg_order is not None
            assert float(pg_order.total_amount) == pytest.approx(100.00)

        # MongoDB: snapshot exists
        snapshot = mongo_db["order_snapshots"].find_one({"order_id": order_id})
        assert snapshot is not None
        assert snapshot["total_amount"] == pytest.approx(100.00)

    def test_order_snapshot_matches_postgres_data(self, db_phase1, pg_session_factory, mongo_db):
        """MongoDB snapshot total_amount, item count, and customer_id match Postgres."""
        from ecommerce_pipeline.postgres_models import Order, OrderItem
        from sqlalchemy import select

        make_customer(pg_session_factory, id=1, name="Alice Johnson", email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=100.00, stock_quantity=20, category="electronics")
        make_product_postgres(
            pg_session_factory, id=2, price=40.00, stock_quantity=20, category="books",
            category_fields={"isbn": "111", "author": "A", "page_count": 200, "genre": "tech"},
        )
        make_product_mongo(mongo_db, id=1, price=100.00, category="electronics", category_fields={})
        make_product_mongo(mongo_db, id=2, price=40.00, category="books",
                           category_fields={"isbn": "111", "author": "A", "page_count": 200, "genre": "tech"})

        result = db_phase1.create_order(
            customer_id=1,
            items=[{"product_id": 1, "quantity": 1}, {"product_id": 2, "quantity": 2}],
        )
        order_id = result["order_id"]

        with pg_session_factory() as session:
            pg_order = session.get(Order, order_id)
            pg_items = session.execute(
                select(OrderItem).where(OrderItem.order_id == order_id)
            ).scalars().all()

        snapshot = mongo_db["order_snapshots"].find_one({"order_id": order_id})

        assert float(pg_order.total_amount) == pytest.approx(snapshot["total_amount"])
        assert len(pg_items) == len(snapshot["items"])
        assert pg_order.customer_id == snapshot["customer"]["id"]

    def test_failed_order_writes_no_snapshot(self, db_phase1, pg_session_factory, mongo_db):
        """When Postgres rejects the order (insufficient stock), no snapshot is written."""
        make_customer(pg_session_factory, id=1, email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=10.00, stock_quantity=1, category="electronics")
        make_product_mongo(mongo_db, id=1, price=10.00, category="electronics", category_fields={})

        with pytest.raises(ValueError):
            db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 10}])

        count = mongo_db["order_snapshots"].count_documents({})
        assert count == 0

    def test_snapshot_preserves_price_at_order_time(self, db_phase1, pg_session_factory, mongo_db):
        """Order snapshot captures unit_price at the moment of purchase, not the current MongoDB price."""
        make_customer(pg_session_factory, id=1, email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=100.00, stock_quantity=20, category="electronics")
        make_product_mongo(mongo_db, id=1, name="Test Product", price=100.00, category="electronics", category_fields={})

        result = db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 1}])
        order_id = result["order_id"]

        # Simulate a price change in MongoDB after the order
        mongo_db["product_catalog"].update_one({"id": 1}, {"$set": {"price": 999.99}})

        snapshot = db_phase1.get_order(order_id)
        assert snapshot is not None
        # The snapshot must still show the original price
        assert snapshot["items"][0]["unit_price"] == pytest.approx(100.00)
