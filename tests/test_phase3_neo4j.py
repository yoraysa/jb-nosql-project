"""
Phase 3 — Neo4j recommendation engine tests.

Tests verify graph-specific behavior added in Phase 3:
  - seed_recommendation_graph: creates Product nodes and BOUGHT_TOGETHER edges
  - Edge weight increments with repeated co-purchases
  - get_recommendations: returns neighbors sorted by weight, excluding the seed product
  - create_order (Phase 3): updates the graph in addition to Postgres + MongoDB

Tests use the full `db` fixture (all four databases).

Neo4j graph schema:
  (:Product {id: int, name: str})
  -[:BOUGHT_TOGETHER {weight: int}]->
  (:Product {id: int, name: str})
"""

import pytest

from tests.conftest import make_customer, make_product_postgres, make_product_mongo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def count_nodes(neo4j_driver) -> int:
    with neo4j_driver.session() as session:
        result = session.run("MATCH (p:Product) RETURN count(p) AS n")
        return result.single()["n"]


def edge_weight(neo4j_driver, id_a: int, id_b: int) -> int | None:
    """Return the BOUGHT_TOGETHER weight between two products (either direction)."""
    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (a:Product {id: $id_a})-[r:BOUGHT_TOGETHER]-(b:Product {id: $id_b})
            RETURN r.weight AS w
            """,
            id_a=id_a, id_b=id_b,
        )
        record = result.single()
        return record["w"] if record else None


def node_ids(neo4j_driver) -> set[int]:
    with neo4j_driver.session() as session:
        result = session.run("MATCH (p:Product) RETURN p.id AS id")
        return {r["id"] for r in result}


# ---------------------------------------------------------------------------
# seed_recommendation_graph tests
# ---------------------------------------------------------------------------

class TestSeedRecommendationGraph:
    def test_creates_product_nodes(self, db, neo4j_driver):
        """seed_recommendation_graph creates a :Product node for every distinct product."""
        orders = [
            {"order_id": 1, "product_ids": [1, 2]},
            {"order_id": 2, "product_ids": [3, 4]},
        ]
        db.seed_recommendation_graph(orders)

        ids = node_ids(neo4j_driver)
        assert {1, 2, 3, 4} == ids

    def test_creates_copurchase_edge(self, db, neo4j_driver):
        """Products in the same order are linked by a BOUGHT_TOGETHER relationship."""
        orders = [{"order_id": 1, "product_ids": [1, 3]}]
        db.seed_recommendation_graph(orders)

        weight = edge_weight(neo4j_driver, 1, 3)
        assert weight is not None
        assert weight >= 1

    def test_edge_weight_accumulates_across_orders(self, db, neo4j_driver):
        """Co-purchasing the same pair in multiple orders increments the edge weight."""
        orders = [
            {"order_id": 1, "product_ids": [1, 3]},
            {"order_id": 2, "product_ids": [1, 3]},
            {"order_id": 3, "product_ids": [1, 3]},
        ]
        db.seed_recommendation_graph(orders)

        weight = edge_weight(neo4j_driver, 1, 3)
        assert weight == 3

    def test_multi_product_order_creates_all_pairs(self, db, neo4j_driver):
        """An order with 3 products creates edges for all C(3,2)=3 pairs."""
        orders = [{"order_id": 1, "product_ids": [1, 2, 3]}]
        db.seed_recommendation_graph(orders)

        assert edge_weight(neo4j_driver, 1, 2) is not None
        assert edge_weight(neo4j_driver, 1, 3) is not None
        assert edge_weight(neo4j_driver, 2, 3) is not None


# ---------------------------------------------------------------------------
# get_recommendations tests
# ---------------------------------------------------------------------------

class TestGetRecommendations:
    def test_returns_recommendations(self, db, neo4j_driver):
        """get_recommendations returns a non-empty list after seeding the graph."""
        orders = [{"order_id": 1, "product_ids": [1, 2]},
                  {"order_id": 2, "product_ids": [1, 3]}]
        db.seed_recommendation_graph(orders)

        result = db.get_recommendations(1)

        assert isinstance(result, list)
        assert len(result) > 0

    def test_recommendations_sorted_by_weight_descending(self, db, neo4j_driver):
        """Recommendations for a product are returned highest weight first."""
        orders = (
            [{"order_id": i, "product_ids": [1, 2]} for i in range(1, 6)]   # weight 5
            + [{"order_id": i, "product_ids": [1, 3]} for i in range(6, 8)] # weight 2
        )
        db.seed_recommendation_graph(orders)

        result = db.get_recommendations(1)

        assert len(result) >= 2
        scores = [r["score"] for r in result]
        assert scores == sorted(scores, reverse=True)

        # Product 2 (weight 5) must come before product 3 (weight 2)
        rec_ids = [r["product_id"] for r in result]
        assert rec_ids.index(2) < rec_ids.index(3)

    def test_no_self_recommendation(self, db, neo4j_driver):
        """The seed product itself must not appear in its own recommendations."""
        orders = [{"order_id": 1, "product_ids": [1, 2, 3]}]
        db.seed_recommendation_graph(orders)

        result = db.get_recommendations(1)

        rec_ids = [r["product_id"] for r in result]
        assert 1 not in rec_ids

    def test_recommendation_shape(self, db, neo4j_driver):
        """Each recommendation dict has product_id, name, and score keys."""
        orders = [{"order_id": 1, "product_ids": [1, 2]}]
        db.seed_recommendation_graph(orders)

        result = db.get_recommendations(1)

        assert len(result) > 0
        rec = result[0]
        assert "product_id" in rec
        assert "name" in rec
        assert "score" in rec

    def test_returns_empty_for_unknown_product(self, db, neo4j_driver):
        """get_recommendations returns [] for a product not in the graph."""
        result = db.get_recommendations(9999)
        assert result == []


# ---------------------------------------------------------------------------
# Live graph update test (create_order in Phase 3)
# ---------------------------------------------------------------------------

class TestGraphUpdatedOnOrder:
    def test_new_order_creates_graph_edge(self, db, pg_session_factory, mongo_db, neo4j_driver):
        """create_order in Phase 3 adds a BOUGHT_TOGETHER edge for newly co-purchased products."""
        make_customer(pg_session_factory, id=1, email="alice@example.com")
        make_product_postgres(pg_session_factory, id=1, price=50.00, stock_quantity=20, category="electronics")
        make_product_postgres(
            pg_session_factory, id=7, price=89.99, stock_quantity=20, category="electronics",
        )
        make_product_mongo(mongo_db, id=1, price=50.00, category="electronics", category_fields={})
        make_product_mongo(mongo_db, id=7, price=89.99, category="electronics", category_fields={})

        db.create_order(customer_id=1, items=[
            {"product_id": 1, "quantity": 1},
            {"product_id": 7, "quantity": 1},
        ])

        weight = edge_weight(neo4j_driver, 1, 7)
        assert weight is not None
        assert weight >= 1
