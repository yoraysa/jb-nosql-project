"""
SQLAlchemy ORM models.

Define your database tables here using the SQLAlchemy 2.0 declarative API.
Every class you define here that inherits from Base will become a table
when `Base.metadata.create_all(engine)` is called at startup.

Useful imports are already provided below. Add more as needed.

Documentation:
    https://docs.sqlalchemy.org/en/20/orm/declarative_tables.html
"""

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, JSON, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    stock_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    electronics: Mapped["ProductElectronics | None"] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
        uselist=False,
    )
    clothing: Mapped["ProductClothing | None"] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
        uselist=False,
    )
    book: Mapped["ProductBooks | None"] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
        uselist=False,
    )
    food: Mapped["ProductFood | None"] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
        uselist=False,
    )
    home: Mapped["ProductHome | None"] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
        uselist=False,
    )

    __table_args__ = (
        CheckConstraint("price >= 0", name="ck_products_price_nonnegative"),
        CheckConstraint("stock_quantity >= 0", name="ck_products_stock_nonnegative"),
    )


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("customers.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True, default="completed")
    total_amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    customer: Mapped["Customer"] = relationship()
    items: Mapped[list["OrderItem"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        CheckConstraint("total_amount >= 0", name="ck_orders_total_nonnegative"),
    )


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("orders.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)

    order: Mapped[Order] = relationship(back_populates="items")
    product: Mapped[Product] = relationship()

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        CheckConstraint("unit_price >= 0", name="ck_order_items_unit_price_nonnegative"),
        Index("ix_order_items_order_id_product_id", "order_id", "product_id"),
    )


class ProductElectronics(Base):
    __tablename__ = "product_electronics"

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        primary_key=True,
    )
    cpu: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ram_gb: Mapped[int | None] = mapped_column(Integer, nullable=True)
    storage_gb: Mapped[int | None] = mapped_column(Integer, nullable=True)
    screen_inches: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)

    product: Mapped[Product] = relationship(back_populates="electronics")


class ProductClothing(Base):
    __tablename__ = "product_clothing"

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        primary_key=True,
    )
    material: Mapped[str | None] = mapped_column(String(255), nullable=True)

    product: Mapped[Product] = relationship(back_populates="clothing")
    sizes: Mapped[list["ClothingSize"]] = relationship(
        back_populates="clothing",
        cascade="all, delete-orphan",
    )
    colors: Mapped[list["ClothingColor"]] = relationship(
        back_populates="clothing",
        cascade="all, delete-orphan",
    )


class ClothingSize(Base):
    __tablename__ = "clothing_sizes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    clothing_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("product_clothing.product_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    size: Mapped[str] = mapped_column(String(50), nullable=False)

    clothing: Mapped[ProductClothing] = relationship(back_populates="sizes")

    __table_args__ = (
        Index("ix_clothing_sizes_clothing_id_size", "clothing_id", "size", unique=True),
    )


class ClothingColor(Base):
    __tablename__ = "clothing_colors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    clothing_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("product_clothing.product_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    color: Mapped[str] = mapped_column(String(50), nullable=False)

    clothing: Mapped[ProductClothing] = relationship(back_populates="colors")

    __table_args__ = (
        Index("ix_clothing_colors_clothing_id_color", "clothing_id", "color", unique=True),
    )


class ProductBooks(Base):
    __tablename__ = "product_books"

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        primary_key=True,
    )
    isbn: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    author: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    page_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    genre: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)

    product: Mapped[Product] = relationship(back_populates="book")


class ProductFood(Base):
    __tablename__ = "product_food"

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        primary_key=True,
    )
    weight_g: Mapped[int | None] = mapped_column(Integer, nullable=True)
    organic: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Boolean stored as 0/1
    allergens: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # Array stored as JSON

    product: Mapped[Product] = relationship(back_populates="food")


class ProductHome(Base):
    __tablename__ = "product_home"

    product_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        primary_key=True,
    )
    dimensions: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # {width, height, depth}
    material: Mapped[str | None] = mapped_column(String(255), nullable=True)
    assembly_required: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Boolean stored as 0/1

    product: Mapped[Product] = relationship(back_populates="home")
