"""
Phase 1 — PostgreSQL tests.

Tests verify Postgres-specific behavior of DBAccess:
  - create_order: ACID transaction, inventory decrement, total calculation
  - revenue_by_category: SQL aggregation

All setup inserts data directly via SQLAlchemy (not through DBAccess), since
DBAccess has no create_customer or create_product methods.
"""

import pytest
from sqlalchemy import select

from ecommerce_pipeline.postgres_models import Customer, Product, Order, OrderItem
from tests.conftest import make_customer, make_product_postgres


# ---------------------------------------------------------------------------
# create_order tests
# ---------------------------------------------------------------------------

class TestCreateOrder:
    def test_create_order_returns_correct_shape(self, db_phase1, pg_session_factory):
        """create_order returns a dict with the fields specified in the interface."""
        make_customer(pg_session_factory, id=1, email="c1@example.com")
        make_product_postgres(pg_session_factory, id=1, price=100.00, stock_quantity=10, category="electronics")

        result = db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 2}])

        assert isinstance(result["order_id"], int)
        assert result["customer_id"] == 1
        assert result["status"] == "completed"
        assert result["total_amount"] == pytest.approx(200.00)
        assert len(result["items"]) == 1
        item = result["items"][0]
        assert item["product_id"] == 1
        assert item["quantity"] == 2
        assert item["unit_price"] == pytest.approx(100.00)

    def test_create_order_persists_to_database(self, db_phase1, pg_session_factory):
        """create_order writes rows to orders and order_items tables."""
        make_customer(pg_session_factory, id=1, email="c1@example.com")
        make_product_postgres(pg_session_factory, id=1, price=50.00, stock_quantity=20, category="books",
                              category_fields={"isbn": "123", "author": "A", "page_count": 100, "genre": "tech"})
        make_product_postgres(pg_session_factory, id=2, price=30.00, stock_quantity=20, category="books",
                              category_fields={"isbn": "456", "author": "B", "page_count": 200, "genre": "tech"})

        result = db_phase1.create_order(
            customer_id=1,
            items=[{"product_id": 1, "quantity": 1}, {"product_id": 2, "quantity": 3}],
        )
        order_id = result["order_id"]

        with pg_session_factory() as session:
            order = session.get(Order, order_id)
            assert order is not None
            assert order.customer_id == 1
            assert float(order.total_amount) == pytest.approx(140.00)

            items = session.execute(
                select(OrderItem).where(OrderItem.order_id == order_id)
            ).scalars().all()
            assert len(items) == 2

    def test_order_reduces_inventory(self, db_phase1, pg_session_factory):
        """create_order decrements stock_quantity in the products table."""
        make_customer(pg_session_factory, id=1, email="c1@example.com")
        make_product_postgres(pg_session_factory, id=1, price=10.00, stock_quantity=10, category="electronics")

        db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 3}])

        with pg_session_factory() as session:
            product = session.get(Product, 1)
            assert product.stock_quantity == 7

    def test_order_fails_insufficient_inventory(self, db_phase1, pg_session_factory):
        """create_order raises ValueError when stock is insufficient; stock unchanged."""
        make_customer(pg_session_factory, id=1, email="c1@example.com")
        make_product_postgres(pg_session_factory, id=1, price=10.00, stock_quantity=2, category="electronics")

        with pytest.raises(ValueError, match="Insufficient stock"):
            db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 5}])

        with pg_session_factory() as session:
            product = session.get(Product, 1)
            assert product.stock_quantity == 2  # unchanged

    def test_order_is_atomic_on_partial_failure(self, db_phase1, pg_session_factory):
        """If one item has insufficient stock, the entire transaction rolls back."""
        make_customer(pg_session_factory, id=1, email="c1@example.com")
        make_product_postgres(pg_session_factory, id=1, price=10.00, stock_quantity=20, category="electronics")
        make_product_postgres(pg_session_factory, id=2, price=10.00, stock_quantity=1,  category="electronics")

        with pytest.raises(ValueError, match="Insufficient stock"):
            db_phase1.create_order(
                customer_id=1,
                items=[
                    {"product_id": 1, "quantity": 5},   # would succeed
                    {"product_id": 2, "quantity": 5},   # fails — stock=1
                ],
            )

        with pg_session_factory() as session:
            # product 1 stock must still be 20 (full rollback)
            assert session.get(Product, 1).stock_quantity == 20
            assert session.get(Product, 2).stock_quantity == 1


# ---------------------------------------------------------------------------
# revenue_by_category tests
# ---------------------------------------------------------------------------

class TestRevenueByCategory:
    def test_returns_correct_sums_sorted_descending(self, db_phase1, pg_session_factory):
        """revenue_by_category aggregates order_items totals per category, sorted desc."""
        make_customer(pg_session_factory, id=1, email="c1@example.com")
        make_product_postgres(pg_session_factory, id=1, price=100.00, stock_quantity=50, category="electronics")
        make_product_postgres(
            pg_session_factory, id=2, price=50.00, stock_quantity=50, category="books",
            category_fields={"isbn": "111", "author": "A", "page_count": 100, "genre": "tech"},
        )

        # Place two orders: electronics revenue = 200, books revenue = 150
        db_phase1.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 2}])
        db_phase1.create_order(customer_id=1, items=[{"product_id": 2, "quantity": 3}])

        result = db_phase1.revenue_by_category()

        assert isinstance(result, list)
        categories = {r["category"]: r["total_revenue"] for r in result}
        assert categories["electronics"] == pytest.approx(200.00)
        assert categories["books"] == pytest.approx(150.00)

        # Verify sort order: highest first
        revenues = [r["total_revenue"] for r in result]
        assert revenues == sorted(revenues, reverse=True)

    def test_returns_empty_when_no_orders(self, db_phase1, pg_session_factory):
        """revenue_by_category returns empty list when there are no orders."""
        result = db_phase1.revenue_by_category()
        assert result == []
