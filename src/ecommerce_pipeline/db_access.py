"""
DBAccess — the data access layer.

This is the only file you need to implement. The web API is already wired up;
every route calls one method on this class. Your job is to replace each
`raise NotImplementedError(...)` with a real implementation.

Work through the phases in order. Read the corresponding lesson file in
materials/project/ before starting each phase.
"""

import json
import logging
from itertools import combinations

from sqlalchemy import select

logger = logging.getLogger(__name__)


class DBAccess:

    def __init__(
        self,
        pg_session_factory,   # sqlalchemy.orm.sessionmaker bound to Postgres engine
        mongo_db,             # pymongo.database.Database
        redis_client=None,    # redis.Redis | None  (None until Phase 2)
        neo4j_driver=None,    # neo4j.Driver | None (None until Phase 3)
        ) -> None:
        self._pg_session_factory = pg_session_factory
        self._mongo_db = mongo_db
        self._redis = redis_client
        self._neo4j = neo4j_driver

    # ── Phase 1 ───────────────────────────────────────────────────────────────

    def create_order(self, customer_id: int, items: list[dict]) -> dict:
        """Place an order atomically.

        customer_id: int
        items: [{"product_id": int, "quantity": int}, ...]

        Returns a dict with order_id, customer_id, status, total_amount,
        created_at (ISO 8601 string), and a list of items including product_name
        and unit_price.

        Raises ValueError if any product has insufficient stock or customer fields are invalid.
        When that happens, no data is modified in any database.

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
            product_id = int(item["product_id"])
            quantity = int(item["quantity"])
            if quantity <= 0:
                raise ValueError("quantity must be greater than 0")
            qty_by_product_id[product_id] = qty_by_product_id.get(product_id, 0) + quantity

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
                        raise ValueError(f"product not found: {missing[0]}")

                    # Validate stock availability for each product (using aggregated
                    # quantities). Any insufficiency aborts the transaction.
                    for pid, needed_qty in qty_by_product_id.items():
                        if product_by_id[pid].stock_quantity < needed_qty:
                            raise ValueError("Insufficient stock")

                    # Build order line items and compute the total. We build three
                    # parallel structures:
                    # - `order_items`: ORM objects persisted to Postgres
                    # - `items_list`: returned to the caller + MongoDB snapshot (denormalized)
                    total_amount = 0.0
                    order_items: list[OrderItem] = []
                    items_list: list[dict] = []

                    for item in items:
                        pid = int(item["product_id"])
                        qty = int(item["quantity"])
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
                            {
                                "product_id": pid,
                                "product_name": product.name,
                                "quantity": qty,
                                "unit_price": unit_price,
                            }
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
                result = {
                    "order_id": int(order.id),
                    "customer_id": int(order.customer_id),
                    "status": str(order.status),
                    "total_amount": float(order.total_amount),
                    "created_at": created_at,
                    "items": items_list,
                }
            except Exception:
                # Defensive rollback: `session.begin()` will roll back on exceptions,
                # but we also explicitly rollback to keep session state clean.
                session.rollback()
                raise

        # Best-effort: denormalized snapshot in MongoDB for fast reads
        # This happens after the transactional Postgres commit and does not affect
        # the order outcome if it fails.
        try:
            self.save_order_snapshot(
                result["order_id"],
                result["customer_id"],
                result["items"],
                result["total_amount"],
                result["status"],
                result["created_at"],
            )
        except Exception:
            logger.exception("Failed to write order snapshot (best-effort)")
            return None

        # Best-effort: invalidate product cache for all products in the order
        # so that the next read fetches fresh stock quantities from Postgres.
        try:
            for item in items:
                product_id = int(item["product_id"])
                self.invalidate_product_cache(product_id)
        except Exception:
            logger.exception("Failed to invalidate product cache (best-effort)")

        return result

    def save_order_snapshot(
        self,
        order_id: int,
        customer_id: int,
        items: list[dict],
        total_amount: float,
        status: str,
        created_at: str,
        ) -> str | None:
        """Save a denormalized order snapshot for fast read access.

        customer: {"id": int, "name": str, "email": str}
        items: [{"product_id": int, "product_name": str, "quantity": int, "unit_price": float}]

        Embeds all customer and product details as they existed at the time
        of the order, so the snapshot remains accurate even if prices or
        names change later.

        Returns a string identifier for the saved document.

        Called internally by create_order after the transactional write
        commits. Not called directly by routes.
        """

        result = self._mongo_db["order_snapshots"].insert_one(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "items": items,
                "total_amount": total_amount,
                "status": status,
                "created_at": created_at,
            }
        )

        return str(result.inserted_id)

    def get_product(self, product_id: int) -> dict | None:
        """Fetch a product by its integer ID.

        Returns a dict with id, name, price, stock_quantity, category,
        description, and category_fields. Returns None if not found.

        The category_fields shape varies by category:
          electronics: {cpu, ram_gb, storage_gb, screen_inches}
          clothing:    {material, sizes, colors}
          books:       {isbn, author, page_count, genre}
          food:        {weight_g, organic, allergens}
          home:        {dimensions, material, assembly_required}
        """

        # yoray: connection checker if the MongoDB connection is down?...

        product_doc = self._mongo_db["product_catalog"].find_one({"id": product_id})
        if product_doc is None:
            return None
        # Remove MongoDB's _id field
        product_doc.pop('_id', None)
        return product_doc

    def search_products(
        self,
        category: str | None = None,
        q: str | None = None,
        ) -> list[dict]:

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
        for product in products:
            product.pop('_id', None)
        return products

    def get_order(self, order_id: int) -> dict | None:
        """Fetch a single order snapshot by order_id.

        Returns the snapshot dict (order_id, customer_id, items list,
        total_amount, status, created_at) or None if not found.
        """

        order_doc = self._mongo_db["order_snapshots"].find_one({"order_id": order_id})
        if order_doc is None:
            return None
        # Remove MongoDB's _id field
        order_doc.pop('_id', None)
        return order_doc

    def get_order_history(self, customer_id: int) -> list[dict]:
        """Fetch all order snapshots for a customer.

        Returns a list of snapshot dicts sorted by created_at descending.
        Returns an empty list if the customer has no orders.
        """
        order_docs = list(
            self._mongo_db["order_snapshots"]
            .find({"customer_id": customer_id})
            .sort("created_at", -1)
        )
        for order_doc in order_docs:
            order_doc.pop('_id', None)
        return order_docs

    def revenue_by_category(self) -> list[dict]:
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
            
            results = session.execute(query).all()
            return [
                {
                    "category": row[0],
                    "total_revenue": float(row[1]) if row[1] is not None else 0.0
                }
                for row in results
            ]


    # ── Phase 2 ───────────────────────────────────────────────────────────────

    def init_inventory_counters(self) -> None:
        """Seed inventory counters from current stock quantities.

        For each product, write its current stock_quantity to the counter
        store. Called at startup and after seeding products.
        """

        from ecommerce_pipeline.postgres_models import Product

        with self._pg_session_factory() as session:
            # Query all products from Postgres
            query = select(Product)
            products = session.execute(query).scalars().all()

            # For each product, set its stock quantity in Redis
            for product in products:
                self._redis.set(f"inventory:{product.id}", product.stock_quantity)

    def invalidate_product_cache(self, product_id: int) -> None:
        """Remove a product's cached entry.

        Call this after updating a product's data so the next read fetches
        fresh data from the primary store. No-op if no entry exists.
        """

        """
        For example, after an order is placed and stock quantities are updated,
        we should invalidate the cache for the affected products.

        This is a simple cache invalidation strategy. In a real system,
        you might want to use a more sophisticated approach, such as write-through caching or cache expiration.

        Redis delete() method is safe to call on non-existent keys, so it's automatically a no-op if the cache entry doesn't exist.
        """

        self._redis.delete(f"inventory:{product_id}")

    def record_product_view(self, customer_id: int, product_id: int) -> None:
        """Record that a customer viewed a product.

        Maintains a bounded, ordered list of the customer's most recently
        viewed products (most recent first, capped at 10 entries).
        """
        raise NotImplementedError("Phase 2: implement record_product_view")

    def get_recently_viewed(self, customer_id: int) -> list[int]:
        """Return up to 10 recently viewed product IDs for a customer.

        Returns IDs as integers, most recently viewed first.
        Returns an empty list if no views have been recorded.
        """
        raise NotImplementedError("Phase 2: implement get_recently_viewed")

    # ── Phase 3 ───────────────────────────────────────────────────────────────

    def seed_recommendation_graph(self, orders: list[dict]) -> None:
        """Build the co-purchase recommendation graph from order history.

        orders: [{"order_id": int, "product_ids": [int, ...]}, ...]

        For each unique pair of products in an order, creates or strengthens
        a co-purchase relationship between them. The strength increases by one
        for every order in which the pair appears together.

        Products not found in the catalog are silently skipped.
        """
        raise NotImplementedError("Phase 3: implement seed_recommendation_graph")

    def get_recommendations(self, product_id: int, limit: int = 5) -> list[dict]:
        """Return product recommendations based on co-purchase patterns.

        Returns [{"product_id": int, "name": str, "score": int}, ...]
        sorted by score (co-purchase strength) descending.

        Returns an empty list if the product has no co-purchase relationships.
        """
        raise NotImplementedError("Phase 3: implement get_recommendations")
