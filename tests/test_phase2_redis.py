"""
Phase 2 — Redis tests.

Tests verify Redis-specific behavior added in Phase 2:
  - get_product: cache-aside pattern (check Redis → miss → fetch Mongo → store in Redis)
  - invalidate_product_cache: DEL removes the cached key
  - init_inventory_counters: sets inventory:{id} keys from Postgres stock values
  - create_order: DECR inventory counter for each ordered product
  - record_product_view / get_recently_viewed: LPUSH/LTRIM/LRANGE list operations

Tests use the db_phase2 fixture (Postgres + MongoDB + Redis, no Neo4j).
Redis key patterns:
  product:{product_id}          — JSON-serialized product dict, TTL=300s
  inventory:{product_id}        — integer string, no TTL
  recently_viewed:{customer_id} — list of product_id strings, head = most recent
"""

import json
import pytest

from tests.conftest import make_customer, make_product_postgres, make_product_mongo


# ---------------------------------------------------------------------------
# Cache-aside tests (get_product with Redis)
# ---------------------------------------------------------------------------

class TestProductCache:
    def test_product_cached_after_first_read(self, db_phase2, mongo_db, redis_client):
        """First call to get_product stores the result in Redis."""
        make_product_mongo(
            mongo_db, id=1, name="Laptop Pro", price=1299.99, stock_quantity=10,
            category="electronics",
            category_fields={"cpu": "M3", "ram_gb": 18, "storage_gb": 512, "screen_inches": 16.2},
        )

        db_phase2.get_product(1)

        cached = redis_client.get("product:1")
        assert cached is not None
        parsed = json.loads(cached)
        assert parsed["name"] == "Laptop Pro"

    def test_second_read_returns_same_data(self, db_phase2, mongo_db, redis_client):
        """Subsequent calls return the same data whether served from Redis or MongoDB."""
        make_product_mongo(
            mongo_db, id=1, name="Laptop Pro", price=1299.99, stock_quantity=10,
            category="electronics", category_fields={},
        )

        first  = db_phase2.get_product(1)
        second = db_phase2.get_product(1)

        assert first["name"] == second["name"]
        assert first["price"] == pytest.approx(second["price"])

    def test_cache_not_populated_for_missing_product(self, db_phase2, mongo_db, redis_client):
        """If the product doesn't exist in MongoDB, no Redis key is written."""
        result = db_phase2.get_product(9999)

        assert result is None
        assert redis_client.get("product:9999") is None

    def test_cache_invalidated_removes_key(self, db_phase2, mongo_db, redis_client):
        """invalidate_product_cache deletes the Redis key for the given product."""
        make_product_mongo(mongo_db, id=1, category="electronics", category_fields={})
        db_phase2.get_product(1)
        assert redis_client.get("product:1") is not None

        db_phase2.invalidate_product_cache(1)

        assert redis_client.get("product:1") is None

    def test_invalidate_is_noop_when_key_absent(self, db_phase2, redis_client):
        """invalidate_product_cache does not raise when the key is already missing."""
        db_phase2.invalidate_product_cache(9999)  # must not raise


# ---------------------------------------------------------------------------
# Inventory counter tests
# ---------------------------------------------------------------------------

class TestInventoryCounters:
    def test_init_inventory_counters_sets_all_keys(self, db_phase2, pg_session_factory, redis_client):
        """init_inventory_counters creates inventory:{id} keys matching Postgres stock."""
        make_product_postgres(pg_session_factory, id=1, stock_quantity=25, category="electronics")
        make_product_postgres(
            pg_session_factory, id=2, stock_quantity=50, category="books",
            category_fields={"isbn": "111", "author": "A", "page_count": 100, "genre": "tech"},
        )

        db_phase2.init_inventory_counters()

        assert redis_client.get("inventory:1") == "25"
        assert redis_client.get("inventory:2") == "50"

    def test_order_decrements_inventory_counter(self, db_phase2, pg_session_factory, mongo_db, redis_client):
        """create_order decrements the Redis inventory counter by the ordered quantity."""
        make_customer(pg_session_factory, id=1, email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=50.00, stock_quantity=20, category="electronics")
        make_product_mongo(mongo_db, id=1, price=50.00, category="electronics", category_fields={})
        db_phase2.init_inventory_counters()

        db_phase2.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 3}])

        assert redis_client.get("inventory:1") == "17"

    def test_inventory_counter_unchanged_on_failed_order(self, db_phase2, pg_session_factory, redis_client):
        """When an order fails (insufficient stock), the Redis counter is not modified."""
        make_customer(pg_session_factory, id=1, email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=10.00, stock_quantity=2, category="electronics")
        db_phase2.init_inventory_counters()

        with pytest.raises(ValueError):
            db_phase2.create_order(customer_id=1, items=[{"product_id": 1, "quantity": 10}])

        assert redis_client.get("inventory:1") == "2"


# ---------------------------------------------------------------------------
# Recently viewed tests
# ---------------------------------------------------------------------------

class TestRecentlyViewed:
    def test_record_view_returns_product_in_list(self, db_phase2, redis_client):
        """record_product_view followed by get_recently_viewed returns the product."""
        db_phase2.record_product_view(customer_id=1, product_id=42)

        result = db_phase2.get_recently_viewed(1)

        assert 42 in result

    def test_most_recent_product_is_first(self, db_phase2, redis_client):
        """get_recently_viewed returns products in reverse chronological order."""
        db_phase2.record_product_view(customer_id=1, product_id=1)
        db_phase2.record_product_view(customer_id=1, product_id=2)
        db_phase2.record_product_view(customer_id=1, product_id=3)

        result = db_phase2.get_recently_viewed(1)

        assert result[0] == 3
        assert result[1] == 2
        assert result[2] == 1

    def test_list_capped_at_ten_items(self, db_phase2, redis_client):
        """get_recently_viewed returns at most 10 product ids."""
        for product_id in range(1, 16):
            db_phase2.record_product_view(customer_id=1, product_id=product_id)

        result = db_phase2.get_recently_viewed(1)

        assert len(result) == 10

    def test_empty_list_for_new_customer(self, db_phase2, redis_client):
        """get_recently_viewed returns [] for a customer with no views recorded."""
        result = db_phase2.get_recently_viewed(9999)
        assert result == []
