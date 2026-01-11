"""SQLAlchemy ORM models for LCS database."""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Column, String, DateTime, Boolean, Integer, BigInteger,
    Numeric, Date, ForeignKey, Text, UniqueConstraint
)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import relationship

from app.database import Base


class Tenant(Base):
    """Tenant (1C database) model."""

    __tablename__ = "tenants"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    import_path = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)


class CustomerGroup(Base):
    """Customer group model."""

    __tablename__ = "customer_groups"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    code = Column(String(50))
    name = Column(String(255))


class Customer(Base):
    """Customer model."""

    __tablename__ = "customers"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    code = Column(String(50))
    name = Column(String(255))
    accumulated_amount = Column(Numeric(18, 2))
    birth_date = Column(Date)
    is_active = Column(Boolean, default=True)
    group_id = Column(UNIQUEIDENTIFIER)
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class Store(Base):
    """Store (trading point) model."""

    __tablename__ = "stores"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    code = Column(String(50))
    name = Column(String(255))
    manager_id = Column(UNIQUEIDENTIFIER)


class Employee(Base):
    """Employee model."""

    __tablename__ = "employees"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    code = Column(String(50))
    name = Column(String(255))


class Manager(Base):
    """Manager model."""

    __tablename__ = "managers"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    code = Column(String(50))
    name = Column(String(255))


class Product(Base):
    """Product (nomenclature) model."""

    __tablename__ = "products"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    code = Column(String(50))
    name = Column(String(500))
    category = Column(String(255))
    category_confidence = Column(Numeric(3, 2))
    classified_at = Column(DateTime)


class Discount(Base):
    """Discount condition model."""

    __tablename__ = "discounts"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    name = Column(String(255))


class CustomerIdentifier(Base):
    """Customer identifier (loyalty card) model."""

    __tablename__ = "customer_identifiers"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), nullable=False)
    customer_id = Column(UNIQUEIDENTIFIER, nullable=False)
    identifier = Column(String(100), nullable=False)


class Transaction(Base):
    """Transaction (receipt header) model."""

    __tablename__ = "transactions"

    id = Column(UNIQUEIDENTIFIER, primary_key=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), primary_key=True)
    customer_id = Column(UNIQUEIDENTIFIER)
    transaction_date = Column(DateTime, nullable=False)
    transaction_hour = Column(Integer)
    amount = Column(Numeric(18, 2))
    amount_before_discount = Column(Numeric(18, 2))
    discount_percent = Column(Numeric(5, 2))
    store_id = Column(UNIQUEIDENTIFIER)
    employee_id = Column(UNIQUEIDENTIFIER)
    duration_seconds = Column(Integer)


class TransactionItem(Base):
    """Transaction item (receipt line) model."""

    __tablename__ = "transaction_items"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id = Column(UNIQUEIDENTIFIER, nullable=False)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), nullable=False)
    product_id = Column(UNIQUEIDENTIFIER, nullable=False)
    quantity = Column(Numeric(18, 3))
    price = Column(Numeric(18, 2))
    price_before_discount = Column(Numeric(18, 2))
    discount_id = Column(UNIQUEIDENTIFIER)


class BonusMovement(Base):
    """Bonus movement (accrual/redemption) model."""

    __tablename__ = "bonus_movements"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), nullable=False)
    customer_id = Column(UNIQUEIDENTIFIER, nullable=False)
    transaction_id = Column(UNIQUEIDENTIFIER)
    amount = Column(Numeric(18, 2))
    movement_type = Column(String(20), nullable=False)  # 'accrual' / 'redemption'
    movement_date = Column(DateTime)


class BonusBalance(Base):
    """Bonus balance model."""

    __tablename__ = "bonus_balances"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), nullable=False)
    customer_id = Column(UNIQUEIDENTIFIER, nullable=False)
    balance = Column(Numeric(18, 2))
    updated_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("tenant_id", "customer_id"),
    )


class CustomerMetrics(Base):
    """Customer metrics model (denormalized for fast access)."""

    __tablename__ = "customer_metrics"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), nullable=False)
    customer_id = Column(UNIQUEIDENTIFIER, nullable=False)

    # Basic transactional (11 metrics)
    total_orders = Column(Integer)
    total_revenue = Column(Numeric(18, 2))
    total_items = Column(Numeric(18, 3))
    first_order_date = Column(Date)
    last_order_date = Column(Date)
    avg_check = Column(Numeric(18, 2))
    avg_items_per_order = Column(Numeric(18, 2))
    max_check = Column(Numeric(18, 2))
    min_check = Column(Numeric(18, 2))
    std_check = Column(Numeric(18, 2))
    avg_margin = Column(Numeric(18, 2))

    # RFM (5 metrics)
    recency = Column(Integer)
    frequency = Column(Numeric(18, 4))
    monetary = Column(Numeric(18, 2))
    rfm_score = Column(Integer)
    rfm_segment = Column(String(50))

    # Temporal patterns (10 metrics)
    customer_age_days = Column(Integer)
    customer_age_months = Column(Integer)
    avg_days_between = Column(Numeric(18, 2))
    median_days_between = Column(Numeric(18, 2))
    std_days_between = Column(Numeric(18, 2))
    expected_next_order = Column(Date)
    days_overdue = Column(Integer)
    purchase_regularity = Column(Numeric(5, 4))
    active_months = Column(Integer)
    activity_rate = Column(Numeric(5, 4))

    # Lifecycle (8 metrics)
    lifecycle_stage = Column(String(50))
    sleep_days = Column(Integer)
    sleep_factor = Column(Numeric(18, 4))
    is_new = Column(Boolean)
    is_active = Column(Boolean)
    is_sleeping = Column(Boolean)
    is_churned = Column(Boolean)
    cohort = Column(String(7))  # YYYY-MM

    # Customer value (11 metrics)
    clv_historical = Column(Numeric(18, 2))
    clv_predicted = Column(Numeric(18, 2))
    clv_segment = Column(String(50))
    abc_segment = Column(String(1))
    xyz_segment = Column(String(1))
    abc_xyz_segment = Column(String(2))
    profit_contribution = Column(Numeric(8, 4))
    cumulative_percentile = Column(Numeric(5, 2))
    revenue_trend = Column(Numeric(8, 4))
    check_trend = Column(Numeric(8, 4))
    frequency_trend = Column(Numeric(8, 4))

    # Predictive (6 metrics)
    prob_alive = Column(Numeric(5, 4))
    churn_probability = Column(Numeric(5, 4))
    churn_risk_segment = Column(String(50))
    predicted_orders_30d = Column(Numeric(18, 4))
    predicted_orders_90d = Column(Numeric(18, 4))
    predicted_revenue_30d = Column(Numeric(18, 2))

    # Product preferences (5 metrics)
    favorite_category = Column(String(255))
    favorite_sku = Column(String(500))
    category_diversity = Column(Integer)
    sku_diversity = Column(Integer)
    cross_sell_potential = Column(Numeric(5, 4))

    # Metadata
    calculated_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("tenant_id", "customer_id"),
    )


class ImportLog(Base):
    """Import log model."""

    __tablename__ = "import_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id = Column(UNIQUEIDENTIFIER, ForeignKey("tenants.id"), nullable=False)
    file_name = Column(String(255))
    records_count = Column(Integer)
    status = Column(String(50))  # 'success', 'error', 'partial'
    error_message = Column(Text)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
