"""Pydantic schemas for API requests and responses."""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List, Any
from uuid import UUID

from pydantic import BaseModel, Field


# ============================================
# Tenant Schemas
# ============================================

class TenantCreate(BaseModel):
    """Schema for creating a tenant."""
    code: str = Field(..., min_length=1, max_length=50, description="Unique tenant code")
    name: str = Field(..., min_length=1, max_length=255, description="Tenant display name")
    import_path: Optional[str] = Field(None, description="Custom import path")


class TenantResponse(BaseModel):
    """Schema for tenant response."""
    id: str
    code: str
    name: str
    import_path: Optional[str]
    created_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


class TenantList(BaseModel):
    """Schema for list of tenants."""
    items: List[TenantResponse]
    total: int


# ============================================
# Import Schemas
# ============================================

class ImportStatus(BaseModel):
    """Schema for import status."""
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    files: dict[str, Any]
    errors: List[str]


class FileInfo(BaseModel):
    """Schema for file information."""
    filename: str
    exists: bool
    size_bytes: Optional[int]
    modified_at: Optional[datetime]
    line_count: Optional[int]
    schema: Optional[List[str]]


# ============================================
# Customer Schemas
# ============================================

class CustomerResponse(BaseModel):
    """Schema for customer response."""
    id: str
    code: Optional[str]
    name: Optional[str]
    accumulated_amount: Optional[float]
    birth_date: Optional[date]
    is_active: bool
    group_id: Optional[str]

    class Config:
        from_attributes = True


class CustomerList(BaseModel):
    """Schema for list of customers."""
    items: List[CustomerResponse]
    total: int
    page: int
    page_size: int


class CustomerMetricsResponse(BaseModel):
    """Schema for customer metrics response."""
    customer_id: str
    calculated_at: Optional[datetime]

    # Basic transactional
    total_orders: Optional[int]
    total_revenue: Optional[float]
    total_items: Optional[float]
    first_order_date: Optional[date]
    last_order_date: Optional[date]
    avg_check: Optional[float]
    avg_items_per_order: Optional[float]
    max_check: Optional[float]
    min_check: Optional[float]
    std_check: Optional[float]
    avg_margin: Optional[float]

    # RFM
    recency: Optional[int]
    frequency: Optional[float]
    monetary: Optional[float]
    rfm_score: Optional[int]
    rfm_segment: Optional[str]

    # Temporal
    customer_age_days: Optional[int]
    customer_age_months: Optional[int]
    avg_days_between: Optional[float]
    median_days_between: Optional[float]
    std_days_between: Optional[float]
    expected_next_order: Optional[date]
    days_overdue: Optional[int]
    purchase_regularity: Optional[float]
    active_months: Optional[int]
    activity_rate: Optional[float]

    # Lifecycle
    lifecycle_stage: Optional[str]
    sleep_days: Optional[int]
    sleep_factor: Optional[float]
    is_new: Optional[bool]
    is_active: Optional[bool]
    is_sleeping: Optional[bool]
    is_churned: Optional[bool]
    cohort: Optional[str]

    # Value
    clv_historical: Optional[float]
    clv_predicted: Optional[float]
    clv_segment: Optional[str]
    abc_segment: Optional[str]
    xyz_segment: Optional[str]
    abc_xyz_segment: Optional[str]
    profit_contribution: Optional[float]
    cumulative_percentile: Optional[float]
    revenue_trend: Optional[float]
    check_trend: Optional[float]
    frequency_trend: Optional[float]

    # Predictive
    prob_alive: Optional[float]
    churn_probability: Optional[float]
    churn_risk_segment: Optional[str]
    predicted_orders_30d: Optional[float]
    predicted_orders_90d: Optional[float]
    predicted_revenue_30d: Optional[float]

    # Product
    favorite_category: Optional[str]
    favorite_sku: Optional[str]
    category_diversity: Optional[int]
    sku_diversity: Optional[int]
    cross_sell_potential: Optional[float]

    class Config:
        from_attributes = True


class CustomerWithMetrics(CustomerResponse):
    """Schema for customer with metrics."""
    metrics: Optional[CustomerMetricsResponse]


# ============================================
# Metrics Calculation Schemas
# ============================================

class CalculationResult(BaseModel):
    """Schema for metrics calculation result."""
    status: str
    customers: int
    errors: int
    duration_seconds: float


# ============================================
# Dashboard Schemas
# ============================================

class SegmentStats(BaseModel):
    """Schema for segment statistics."""
    segment: str
    count: int
    percentage: float
    total_revenue: float


class DashboardStats(BaseModel):
    """Schema for dashboard statistics."""
    total_customers: int
    active_customers: int
    new_customers: int
    churned_customers: int
    total_revenue: float
    avg_check: float
    rfm_segments: List[SegmentStats]
    lifecycle_segments: List[SegmentStats]
    abc_segments: List[SegmentStats]
