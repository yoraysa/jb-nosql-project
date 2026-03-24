"""
DBAccess — the data access layer.

This is one of the files you implement. The web API is already wired up;
every route calls one method on this class. Your job is to replace each
`raise NotImplementedError(...)` with a real implementation.

Work through the phases in order. Read the corresponding lesson file before
starting each phase.

You also implement scripts/migrate.py and scripts/seed.py alongside this file.
"""

from __future__ import annotations

import json
import logging
from itertools import combinations
from typing import TYPE_CHECKING

from decimal import Decimal

from sqlalchemy import select

from ecommerce_pipeline.models.responses import (
    OrderCustomerEmbed,
    OrderItemResponse,
    OrderResponse,
    OrderSnapshotResponse,
    ProductResponse,
    CategoryRevenueResponse,
    RecommendationResponse,
)

if TYPE_CHECKING:
    import neo4j
    import redis as redis_lib
    from pymongo.database import Database as MongoDatabase
    from sqlalchemy.orm import sessionmaker

    from ecommerce_pipeline.models.requests import OrderItemRequest
    from ecommerce_pipeline.models.responses import (
        CategoryRevenueResponse,
        OrderItemResponse,
        OrderResponse,
        OrderSnapshotResponse,
        ProductResponse,
        RecommendationResponse,
    )

logger = logging.getLogger(__name__)


class DBAccess:
    def __init__(
        self,
        pg_session_factory: sessionmaker,
        mongo_db: MongoDatabase,
        redis_client: redis_lib.Redis | None = None,
        neo4j_driver: neo4j.Driver | None = None,
        ) -> None:

        self._pg_session_factory = pg_session_factory
        self._mongo_db = mongo_db
        self._redis = redis_client
        self._neo4j = neo4j_driver

    # ── Phase 1 ───────────────────────────────────────────────────────────────

    def create_order(self, customer_id: int, items: list[OrderItemRequest]) -> OrderResponse:
        """Place an order atomically.

        See OrderItemRequest in models/requests.py for the input shape.
        See OrderResponse in models/responses.py for the return shape.

        Raises ValueError if any product has insufficient stock. When that
        happens, no data is modified in any database.

        After the order is persisted transactionally, a denormalized snapshot
        is saved for read access, and downstream counters and graph edges are
        updated (best-effort, does not roll back the order on failure).
        """


        try:
            customer_id = int(customer_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("customer id must be an integer") from exc

        # Basic input validation: an order must contain at least one line item.
        if not items:
            raise ValueError("items must not be empty")

        from ecommerce_pipeline.postgres_models import Customer, Order, OrderItem, Product

        # Normalize and validate request payload:
        # - Coerce types to int
        # - Enforce positive quantities
        # - Aggregate quantities per product so we can validate stock once per product
        qty_by_product_id: dict[int, int] = {}
        for item in items:
            product_id = int(item.product_id)
            quantity = int(item.quantity)
            if quantity <= 0:
                raise ValueError("quantity must be greater than 0")
            qty_by_product_id[product_id] = qty_by_product_id.get(product_id, 0) + quantity

        # Fast pre-check: if Redis inventory counters are populated, check them
        # before starting a heavy Postgres transaction.
        insufficient_stock = False
        try:
            for product_id, needed_qty in qty_by_product_id.items():
                val = self._redis.get(f"inventory:{product_id}")
                if val is not None and int(val) < needed_qty:
                    insufficient_stock = True
        except Exception:
            logger.warning("Fast inventory check failed (Redis)")
        if insufficient_stock:
            print("Insufficient stock (Redis)")
            raise ValueError("Insufficient stock (Redis)")

        # We only need to load/lock products referenced by the order.
        product_ids = list(qty_by_product_id.keys())

        with self._pg_session_factory() as session:
            try:
                with session.begin():
                    # Transactional write path (all-or-nothing):
                    # If anything fails inside this block, SQLAlchemy will roll back and
                    # Postgres state (including stock quantities) will not be modified.

                    # When the customer_id doesn’t exist we create a new Customer row (or upsert)
                    existing_customer = session.get(Customer, customer_id)
                    if existing_customer is None:
                        raise ValueError(f"Customer not exists")
                        
                    # Load and lock product rows for update to prevent concurrent
                    # overselling. This ensures the stock check and stock decrement are
                    # consistent within this transaction.
                    products = (
                        session.execute(
                            select(Product)
                            .where(Product.id.in_(product_ids))
                            .with_for_update()
                        )
                        .scalars()
                        .all()
                    )
                    product_by_id = {p.id: p for p in products}

                    # Reject orders that reference unknown product IDs.
                    missing = [pid for pid in product_ids if pid not in product_by_id]
                    if missing:
                        raise ValueError(f"product not found: {missing[0]} (Postgres)")

                    # Validate stock availability for each product (using aggregated
                    # quantities). Any insufficiency aborts the transaction.
                    for pid, needed_qty in qty_by_product_id.items():
                        if product_by_id[pid].stock_quantity < needed_qty:
                            print("Insufficient stock (Postgres)")
                            raise ValueError("Insufficient stock (Postgres)")

                    # Build order line items and compute the total. We build three
                    # parallel structures:
                    # - `order_items`: ORM objects persisted to Postgres
                    # - `items_list`: returned to the caller + MongoDB snapshot (denormalized)
                    total_amount = 0.0
                    order_items: list[OrderItem] = []
                    items_list: list[OrderItemResponse] = []

                    for item in items:
                        pid = int(item.product_id)
                        qty = int(item.quantity)
                        product = product_by_id[pid]
                        unit_price = float(product.price)
                        total_amount += unit_price * qty

                        order_items.append(
                            OrderItem(
                                product_id=pid,
                                quantity=qty,
                                unit_price=unit_price,
                            )
                        )
                        items_list.append(
                            OrderItemResponse(
                                product_id=pid,
                                product_name=product.name,
                                quantity=qty,
                                unit_price=unit_price,
                            )
                        )


                    # Apply stock decrements (still inside the transaction and while
                    # holding row locks).
                    for pid, needed_qty in qty_by_product_id.items():
                        product_by_id[pid].stock_quantity -= needed_qty

                    # Persist the order header and obtain its generated primary key
                    # for linking order items.
                    order = Order(
                        customer_id=customer_id,
                        status="completed",
                        total_amount=total_amount,
                    )
                    session.add(order)
                    session.flush()  # assign order.id and defaults

                    # Persist order items referencing the newly created order.
                    for oi in order_items:
                        oi.order_id = order.id
                        session.add(oi)

                # After commit, build an API-friendly response payload.
                created_at = (
                    order.created_at.isoformat()
                    if hasattr(order.created_at, "isoformat")
                    else str(order.created_at)
                )
                result = OrderResponse(
                    order_id=int(order.id),
                    customer_id=int(order.customer_id),
                    status=str(order.status),
                    total_amount=float(order.total_amount),
                    created_at=created_at,
                    items=items_list,
                )
            except Exception:
                # Defensive rollback: `session.begin()` will roll back on exceptions,
                # but we also explicitly rollback to keep session state clean.
                session.rollback()
                raise

        # Best-effort: snapshot in MongoDB for fast reads
        # This happens after the transactional Postgres commit and does not
        # affect the order outcome if it fails.
        self.save_order_snapshot(
            result.order_id,
            self.get_customer_embed(customer_id),
            result.items,
            result.total_amount,
            result.status,
            result.created_at,
        )

        # Best-effort: decrement Redis inventory counters
        try:
            for product_id, needed_qty in qty_by_product_id.items():
                self._redis.decrby(f"inventory:{product_id}", needed_qty)
        except Exception:
            logger.exception("Failed to decrement Redis inventory counters (best-effort)")

        # Best-effort: update the co-purchase recommendation graph in Neo4j
        # with the products from this new order.
        try:
            product_ids = [int(item.product_id) for item in items]
            self.seed_recommendation_graph(
                [{"order_id": int(order.id), "product_ids": product_ids}]
            )
        except Exception:
            logger.exception("Failed to update recommendation graph (best-effort)")

        return result

    def get_customer_embed(self, customer_id: int) -> OrderCustomerEmbed:
        """Fetch a customer and return their embedded representation.
        
        Returns an OrderCustomerEmbed with id, name, and email.
        Raises ValueError if the customer is not found.
        """
        from ecommerce_pipeline.postgres_models import Customer

        with self._pg_session_factory() as session:
            customer = session.get(Customer, customer_id)
            if customer is None:
                raise ValueError(f"Customer {customer_id} not found")
            
            return OrderCustomerEmbed(
                id=customer.id,
                name=customer.name,
                email=customer.email,
            )

    def save_order_snapshot(
        self,
        order_id: int,
        customer: OrderCustomerEmbed,
        items: list[OrderItemResponse],
        total_amount: float,
        status: str,
        created_at: str,
    ) -> str:
        """Save a denormalized order snapshot for fast read access.

        See OrderCustomerEmbed and OrderItemResponse in models/responses.py
        for the input shapes.

        Embeds all customer and product details as they existed at the time
        of the order, so the snapshot remains accurate even if prices or
        names change later.

        Returns a string identifier for the saved document.

        Called internally by create_order after the transactional write
        commits. Not called directly by routes.
        """

        try:
            # Pymongo cannot serialize Pydantic models directly; convert to dicts.
            items_dicts = [
                item.model_dump() if hasattr(item, "model_dump") else item
                for item in items
            ]
            result = self._mongo_db["order_snapshots"].insert_one(
                {
                    "order_id": order_id,
                    "customer": customer.model_dump(),
                    "items": items_dicts,
                    "total_amount": total_amount,
                    "status": status,
                    "created_at": created_at,
                }
            )
            return str(result.inserted_id)
        except Exception:
            logger.exception("Failed to write order snapshot (best-effort)")
            return ""












    def get_product(self, product_id: int) -> ProductResponse | None:
        """Fetch a product by its integer ID.

        See ProductResponse in models/responses.py for the return shape.
        Returns None if not found.

        cache-aside redis/mongo:
        search product in redis
            if not found, search in mongo (if found, write to redis)
                if not found, search in postgres (if found, write to redis+mongo)
                    if not found, return None
        """

        def write_to_redis(product_id, result):
            try:
                self._redis.set(f"product:{product_id}", result.model_dump_json(), ex=300)
                print(f"Cache set for product {product_id} (Redis)")
            except Exception as e:
                logger.exception("Failed to cache product (best-effort)")


        # Cache-aside pattern: first, check Redis
        try:
            cached = self._redis.get(f"product:{product_id}")
            if cached:
                print(f"Cache hit for product {product_id} (Redis)")
                return ProductResponse(**json.loads(cached))
        except Exception as e:
            logger.exception("Failed to get product from cache (best-effort)")

        # Second, check MongoDB
        try:
            product_doc = self._mongo_db["product_catalog"].find_one({"id": product_id})
            if product_doc:
                product_doc.pop('_id', None)
                result = ProductResponse(**product_doc)
                
                print(f"Cache hit for product {product_id} (MongoDB)")

                # Cache it in Redis if possible
                write_to_redis(product_id, result)

                return result
        except Exception as e:
            logger.exception("Failed to get product from MongoDB (best-effort)")

        # Third, fetch from Postgres (cache miss)
        print(f"Cache miss for product {product_id} (Redis and MongoDB)")
        from ecommerce_pipeline.postgres_models import Product
        from sqlalchemy.inspection import inspect

        with self._pg_session_factory() as session:
            product = session.get(Product, product_id)
            if product is None:
                print(f"Product {product_id} not found (Postgres)")
                return None

            # Build category_fields dict dynamically based on category-specific relationships
            category_fields = {}
            category_model = getattr(product, product.category, None)

            # Dynamically extract fields from the category model
            if category_model:
                mapper = inspect(category_model.__class__)
                for column in mapper.columns:
                    # Skip the foreign key column (product_id)
                    if column.name != 'product_id':
                        value = getattr(category_model, column.name)
                        if isinstance(value, Decimal):
                            value = float(value)
                        category_fields[column.name] = value

            # Handle special cases for relationships (sizes, colors for clothing)
            if product.category == "clothing" and product.clothing:
                category_fields['sizes'] = [size.size for size in product.clothing.sizes]
                category_fields['colors'] = [color.color for color in product.clothing.colors]

            # Build the response dict matching the expected schema
            result = ProductResponse(
                id=product.id,
                name=product.name,
                price=float(product.price),
                stock_quantity=product.stock_quantity,
                category=product.category,
                description=product.description or "",
                category_fields=category_fields,
            )



        # cache product in mongo for next time
        try:
            self._mongo_db["product_catalog"].replace_one(
                {"id": product_id}, result.model_dump(), upsert=True
            )
            print(f"Cache set for product {product_id} (MongoDB)")
        except Exception as e:
            logger.exception("Failed to sync product to MongoDB (best-effort)")

        # cache product in redis for next time
        write_to_redis(product_id, result)

        return result

    def search_products(self, category: str | None = None, q: str | None = None) -> list[ProductResponse]:

        """Search the product catalog with optional filters.

        category: exact match on the category field
        q: case-insensitive substring match on the product name
        Both filters are ANDed together. Returns all products if both are None.
        Returns a list of product dicts (same shape as get_product).
        """

        query = {}
        if category is not None:
            query["category"] = category
        if q is not None:
            query["name"] = {"$regex": q, "$options": "i"}

        products = list(self._mongo_db["product_catalog"].find(query))
        return [ProductResponse(**p) for p in products]

    def get_order(self, order_id: int) -> OrderSnapshotResponse | None:
        """Fetch a single order snapshot by order_id.

        See OrderSnapshotResponse in models/responses.py for the return shape.
        Returns None if not found.

        cache-aside mongo:
        search order in mongo
            if not found, search in postgres (if found, write to mongo)
                if not found, return None
        """

        try:
            order_doc = self._mongo_db["order_snapshots"].find_one({"order_id": order_id})
            if order_doc:
                order_doc.pop('_id', None)
                print(f"Cache hit for order {order_id} (MongoDB)")
                return OrderSnapshotResponse(**order_doc)
        except Exception as e:
            logger.exception("Failed to get order from MongoDB (best-effort)")

        # If not in Mongo, check Postgres
        print(f"Cache miss for order {order_id} (MongoDB)")
        from ecommerce_pipeline.postgres_models import Order, OrderItem
        from sqlalchemy.orm import joinedload

        with self._pg_session_factory() as session:
            # Query order with customer and items/products
            order = session.get(Order, order_id, options=[
                joinedload(Order.customer),
                joinedload(Order.items).joinedload(OrderItem.product)
            ])

            if order is None:
                print(f"Order {order_id} not found (Postgres)")
                return None

            # Construct the response matching OrderSnapshotResponse shape
            customer_embed = OrderCustomerEmbed(
                id=order.customer.id,
                name=order.customer.name,
                email=order.customer.email
            )

            items_list = []
            for item in order.items:
                items_list.append(OrderItemResponse(
                    product_id=item.product_id,
                    product_name=item.product.name,
                    quantity=item.quantity,
                    unit_price=float(item.unit_price)
                ))

            created_at = (
                order.created_at.isoformat()
                if hasattr(order.created_at, "isoformat")
                else str(order.created_at)
            )

            result = OrderSnapshotResponse(
                order_id=int(order.id),
                customer=customer_embed,
                items=items_list,
                total_amount=float(order.total_amount),
                status=str(order.status),
                created_at=created_at,
            )

            # Set in MongoDB (save snapshot)
            print(f"caching order {result.order_id} to MongoDB")
            self.save_order_snapshot(
                result.order_id,
                customer_embed,
                result.items,
                result.total_amount,
                result.status,
                result.created_at,
            )

            return result

    def get_order_history(self, customer_id: int) -> list[OrderSnapshotResponse]:
        """Fetch all order snapshots for a customer, sorted by created_at descending.

        Returns an empty list if the customer has no orders.
        """
        order_docs = list(
            self._mongo_db["order_snapshots"]
            .find({"customer.id": customer_id})
            .sort("created_at", -1)
        )

        return [OrderSnapshotResponse(**doc) for doc in order_docs]

    def revenue_by_category(self, category: str | None = None) -> list[CategoryRevenueResponse]:
        """Compute total revenue per product category.

        Returns [{"category": str, "total_revenue": float}, ...] sorted by
        total_revenue descending.
        """
        from ecommerce_pipeline.postgres_models import OrderItem, Product
        from sqlalchemy import func

        with self._pg_session_factory() as session:
            query = (
                select(
                    Product.category,
                    func.sum(OrderItem.quantity * OrderItem.unit_price).label("total_revenue")
                )
                .join(OrderItem, Product.id == OrderItem.product_id)
                .group_by(Product.category)
                .order_by(func.sum(OrderItem.quantity * OrderItem.unit_price).desc())
            )
            
            if category is not None:
                query = query.where(Product.category == category)
            
            results = session.execute(query).all()
            return [
                CategoryRevenueResponse(
                    category=row[0],
                    total_revenue=float(row[1]) if row[1] is not None else 0.0
                )
                for row in results
            ]


    # ── Phase 2 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to DECR Redis inventory counters after the
    #     Postgres transaction succeeds.
    #   - Optionally, add a fast pre-check: before starting the Postgres
    #     transaction, check the Redis counter. If it shows insufficient
    #     stock, fail fast without hitting Postgres.
    #   - Update scripts/seed.py to initialize inventory counters in Redis.
    #   - Add cache-aside logic to get_product (check Redis first, populate
    #     on miss with a 300-second TTL).


    def invalidate_product_cache(self, product_id: int) -> None:
        """Remove a product's cached entry.

        Call this after updating a product's data so the next read fetches
        fresh data from the primary store. No-op if no entry exists.
        """

        """
        this is being called by test_phase_2.py
        in this project we don't have a update product method...
        """

        try:
            self._redis.delete(f"product:{product_id}")
            print(f"Deleted product {product_id} from cache (Redis) - invalidate_product_cache")
        except Exception as e:
            logger.exception("Failed to invalidate product cache (best-effort)")

    def record_product_view(self, customer_id: int, product_id: int) -> None:
        """Record that a customer viewed a product.

        Maintains a bounded, ordered list of the customer's most recently
        viewed products (most recent first, capped at 10 entries).
        """
        key = f"recently_viewed:{customer_id}"
        # Remove product if it already exists (so it doesn't duplicate)
        self._redis.lrem(key, 1, product_id)
        # Add product to the front of the list (most recent)
        self._redis.lpush(key, product_id)
        # Keep only the most recent 10 items
        self._redis.ltrim(key, 0, 9)

    def get_recently_viewed(self, customer_id: int) -> list[int]:
        """Return up to 10 recently viewed product IDs for a customer.

        Returns IDs as integers, most recently viewed first.
        Returns an empty list if no views have been recorded.
        """
        key = f"recently_viewed:{customer_id}"
        views = self._redis.lrange(key, 0, -1)
        # Convert bytes to integers
        return [int(view) for view in views]

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to MERGE co-purchase edges in Neo4j for every
    #     pair of products in the order, incrementing the edge weight.
    #   - Update scripts/migrate.py to create Neo4j constraints.
    #   - Update scripts/seed.py to build the co-purchase graph from
    #     seed_data/historical_orders.json.

    def seed_recommendation_graph(self, orders: list[dict]) -> None:
        """Build the co-purchase recommendation graph from order history.

        orders: [{"order_id": int, "product_ids": [int, ...]}, ...]

        For each unique pair of products in an order, creates or strengthens
        a co-purchase relationship between them. The strength increases by one
        for every order in which the pair appears together.

        Products not found in the catalog are silently skipped.
        """
        if not self._neo4j:
            logger.warning("Neo4j driver not available; skipping recommendation graph update")
            return

        for order in orders:
            product_ids = order.get("product_ids", [])
            if len(product_ids) < 2:
                # Need at least 2 products to create a pair
                continue

            # Generate all unique pairs of products in this order
            pairs = list(combinations(product_ids, 2))

            # Create or update BOUGHT_TOGETHER relationships for each pair
            with self._neo4j.session() as session:
                for product_id_1, product_id_2 in pairs:
                    # Cypher query to merge nodes and create/increment relationship
                    query = """
                    MERGE (p1:Product {id: $pid1})
                    MERGE (p2:Product {id: $pid2})
                    MERGE (p1)-[r:BOUGHT_TOGETHER]-(p2)
                    ON CREATE SET r.purchases = 1
                    ON MATCH  SET r.purchases = r.purchases + 1
                    """
                    session.run(query, pid1=product_id_1, pid2=product_id_2)


    def get_recommendations(self, product_id: int, limit: int = 5) -> list[RecommendationResponse]:
        """Return product recommendations based on co-purchase patterns.

        Returns [{"product_id": int, "name": str, "purchases": int}, ...]
        sorted by purchases (co-purchase strength) descending.

        Returns an empty list if the product has no co-purchase relationships.
        """
        if not self._neo4j:
            logger.warning("Neo4j driver not available; returning empty recommendations")
            return []

        recommendations = []

        with self._neo4j.session() as session:
            # Query for all products connected via BOUGHT_TOGETHER relationships,
            # sorted by purchases descending, limited to the requested purchases
            query = """
            MATCH (p1:Product {id: $product_id})-[r:BOUGHT_TOGETHER]-(p2:Product)
            RETURN p2.id as product_id, r.purchases as purchases
            ORDER BY purchases DESC
            LIMIT $limit
            """
            result = session.run(query, product_id=product_id, limit=limit)
            rows = result.data()

            # Fetch product details from Postgres for each recommendation
            for row in rows:
                recommended_product_id = row.get("product_id")
                purchases = row.get("purchases", 0)

                # Fetch product name from Postgres
                product = self.get_product(recommended_product_id)
                if product:
                    recommendations.append(RecommendationResponse(
                        product_id=recommended_product_id,
                        name=product.name,
                        purchases=int(purchases) if purchases else 0,
                    ))

        return recommendations
