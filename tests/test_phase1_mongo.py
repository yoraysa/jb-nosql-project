"""
Phase 1 — MongoDB tests.

Tests verify MongoDB-specific behavior of DBAccess:
  - get_product: single-document read from product_catalog by numeric id
  - search_products: filter by category and/or name substring
  - save_order_snapshot: denormalized snapshot written to order_snapshots
  - get_order: fetch one snapshot by order_id
  - get_order_history: list snapshots for a customer, sorted by created_at desc

All setup inserts documents directly via pymongo (not through DBAccess).
"""

import pytest

from tests.conftest import make_product_mongo, make_snapshot


# ---------------------------------------------------------------------------
# get_product tests
# ---------------------------------------------------------------------------

class TestGetProduct:
    def test_returns_electronics_with_category_fields(self, db_phase1, mongo_db):
        """get_product returns a complete product dict with nested category_fields."""
        make_product_mongo(
            mongo_db, id=1, name="Laptop Pro", price=1299.99, stock_quantity=10,
            category="electronics",
            category_fields={"cpu": "Apple M3", "ram_gb": 18, "storage_gb": 512, "screen_inches": 16.2},
        )

        result = db_phase1.get_product(1)

        assert result is not None
        assert result["id"] == 1
        assert result["name"] == "Laptop Pro"
        assert result["price"] == pytest.approx(1299.99)
        assert result["category"] == "electronics"
        cf = result["category_fields"]
        assert cf["cpu"] == "Apple M3"
        assert cf["ram_gb"] == 18
        assert cf["storage_gb"] == 512
        assert cf["screen_inches"] == pytest.approx(16.2)

    def test_returns_clothing_with_sizes_and_colors(self, db_phase1, mongo_db):
        """get_product returns sizes and colors lists for a clothing product."""
        make_product_mongo(
            mongo_db, id=2, name="Wool Sweater", price=89.99, stock_quantity=40,
            category="clothing",
            category_fields={"material": "merino wool", "sizes": ["S", "M", "L"], "colors": ["navy"]},
        )

        result = db_phase1.get_product(2)

        assert result is not None
        cf = result["category_fields"]
        assert isinstance(cf["sizes"], list)
        assert isinstance(cf["colors"], list)
        assert "M" in cf["sizes"]
        assert "navy" in cf["colors"]

    def test_returns_food_product_mongodb_only(self, db_phase1, mongo_db):
        """get_product works for food category products that exist only in MongoDB."""
        make_product_mongo(
            mongo_db, id=25, name="Organic Granola", price=12.99, stock_quantity=200,
            category="food",
            category_fields={"weight_g": 500, "organic": True, "allergens": ["nuts", "gluten"]},
        )

        result = db_phase1.get_product(25)

        assert result is not None
        assert result["category"] == "food"
        assert result["category_fields"]["organic"] is True
        assert "nuts" in result["category_fields"]["allergens"]

    def test_returns_none_for_unknown_id(self, db_phase1, mongo_db):
        """get_product returns None when no document with the given id exists."""
        result = db_phase1.get_product(9999)
        assert result is None


# ---------------------------------------------------------------------------
# search_products tests
# ---------------------------------------------------------------------------

class TestSearchProducts:
    def test_filter_by_category(self, db_phase1, mongo_db):
        """search_products(category=...) returns only matching category."""
        make_product_mongo(mongo_db, id=1, category="electronics",
                           category_fields={"cpu": "M3", "ram_gb": 16, "storage_gb": 256, "screen_inches": 14.0})
        make_product_mongo(mongo_db, id=2, category="electronics",
                           category_fields={"cpu": "i9", "ram_gb": 32, "storage_gb": 1000, "screen_inches": 15.6})
        make_product_mongo(mongo_db, id=3, category="books",
                           category_fields={"isbn": "111", "author": "A", "page_count": 300, "genre": "tech"})

        result = db_phase1.search_products(category="electronics")

        assert len(result) == 2
        assert all(r["category"] == "electronics" for r in result)

    def test_filter_by_name_case_insensitive(self, db_phase1, mongo_db):
        """search_products(q=...) does case-insensitive substring match on name."""
        make_product_mongo(mongo_db, id=1, name="Laptop Pro", category="electronics",
                           category_fields={})
        make_product_mongo(mongo_db, id=2, name="Gaming Laptop", category="electronics",
                           category_fields={})
        make_product_mongo(mongo_db, id=3, name="Wireless Mouse", category="electronics",
                           category_fields={})

        result = db_phase1.search_products(q="laptop")

        assert len(result) == 2
        names = {r["name"] for r in result}
        assert names == {"Laptop Pro", "Gaming Laptop"}

    def test_filter_by_category_and_name(self, db_phase1, mongo_db):
        """search_products with both category and q applies AND logic."""
        make_product_mongo(mongo_db, id=1, name="Laptop Pro", category="electronics", category_fields={})
        make_product_mongo(mongo_db, id=2, name="Laptop Stand", category="home",
                           category_fields={"dimensions": {"width": 30, "height": 20, "depth": 10},
                                            "material": "aluminum", "assembly_required": False})
        make_product_mongo(mongo_db, id=3, name="Wireless Mouse", category="electronics", category_fields={})

        result = db_phase1.search_products(category="electronics", q="laptop")

        assert len(result) == 1
        assert result[0]["name"] == "Laptop Pro"

    def test_returns_all_when_no_filters(self, db_phase1, mongo_db):
        """search_products() with no arguments returns all products."""
        for i in range(1, 6):
            make_product_mongo(mongo_db, id=i, category="electronics", category_fields={})

        result = db_phase1.search_products()
        assert len(result) == 5


# ---------------------------------------------------------------------------
# save_order_snapshot tests
# ---------------------------------------------------------------------------

class TestSaveOrderSnapshot:
    def test_snapshot_stored_in_mongodb(self, db_phase1, mongo_db):
        """save_order_snapshot inserts a document into order_snapshots collection."""
        customer = {"id": 1, "name": "Alice", "email": "alice@example.com"}
        items = [{"product_id": 1, "product_name": "Laptop Pro", "quantity": 1, "unit_price": 1299.99}]

        inserted_id = db_phase1.save_order_snapshot(
            order_id=42,
            customer=customer,
            items=items,
            total_amount=1299.99,
            status="completed",
            created_at="2025-01-15T10:30:00",
        )

        assert inserted_id is not None
        assert isinstance(inserted_id, str)

        doc = mongo_db["order_snapshots"].find_one({"order_id": 42})
        assert doc is not None
        assert doc["customer"]["name"] == "Alice"
        assert doc["total_amount"] == pytest.approx(1299.99)
        assert len(doc["items"]) == 1
        assert doc["items"][0]["product_name"] == "Laptop Pro"
        assert doc["status"] == "completed"


# ---------------------------------------------------------------------------
# get_order tests
# ---------------------------------------------------------------------------

class TestGetOrder:
    def test_returns_order_snapshot(self, db_phase1, mongo_db):
        """get_order returns the snapshot document for a given order_id."""
        customer = {"id": 1, "name": "Alice", "email": "alice@example.com"}
        items = [{"product_id": 1, "product_name": "Laptop Pro", "quantity": 1, "unit_price": 1299.99}]
        make_snapshot(mongo_db, order_id=10, customer=customer, items=items, total_amount=1299.99)

        result = db_phase1.get_order(10)

        assert result is not None
        assert result["order_id"] == 10
        assert result["customer"]["name"] == "Alice"
        assert len(result["items"]) == 1

    def test_returns_none_for_unknown_order(self, db_phase1, mongo_db):
        """get_order returns None if no snapshot exists for the given order_id."""
        result = db_phase1.get_order(9999)
        assert result is None


# ---------------------------------------------------------------------------
# get_order_history tests
# ---------------------------------------------------------------------------

class TestGetOrderHistory:
    def test_returns_orders_for_customer(self, db_phase1, mongo_db):
        """get_order_history returns all snapshots for the given customer_id."""
        customer = {"id": 1, "name": "Alice", "email": "alice@example.com"}
        items = [{"product_id": 1, "product_name": "Mouse", "quantity": 1, "unit_price": 49.99}]

        make_snapshot(mongo_db, order_id=1, customer=customer, items=items,
                      total_amount=49.99, created_at="2025-01-01T10:00:00")
        make_snapshot(mongo_db, order_id=2, customer=customer, items=items,
                      total_amount=49.99, created_at="2025-01-02T10:00:00")
        make_snapshot(mongo_db, order_id=3, customer=customer, items=items,
                      total_amount=49.99, created_at="2025-01-03T10:00:00")

        other_customer = {"id": 2, "name": "Bob", "email": "bob@example.com"}
        make_snapshot(mongo_db, order_id=4, customer=other_customer, items=items,
                      total_amount=49.99, created_at="2025-01-04T10:00:00")

        result = db_phase1.get_order_history(1)

        assert len(result) == 3
        assert all(r["customer"]["id"] == 1 for r in result)

    def test_returns_sorted_descending_by_created_at(self, db_phase1, mongo_db):
        """get_order_history results are sorted newest-first."""
        customer = {"id": 1, "name": "Alice", "email": "alice@example.com"}
        items = [{"product_id": 1, "product_name": "Mouse", "quantity": 1, "unit_price": 49.99}]

        make_snapshot(mongo_db, order_id=1, customer=customer, items=items,
                      total_amount=49.99, created_at="2025-01-01T10:00:00")
        make_snapshot(mongo_db, order_id=2, customer=customer, items=items,
                      total_amount=49.99, created_at="2025-01-03T10:00:00")
        make_snapshot(mongo_db, order_id=3, customer=customer, items=items,
                      total_amount=49.99, created_at="2025-01-02T10:00:00")

        result = db_phase1.get_order_history(1)

        dates = [r["created_at"] for r in result]
        assert dates == sorted(dates, reverse=True)

    def test_returns_empty_for_customer_with_no_orders(self, db_phase1, mongo_db):
        """get_order_history returns [] for a customer_id with no snapshots."""
        result = db_phase1.get_order_history(999)
        assert result == []
