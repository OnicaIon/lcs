"""FastAPI routes for LCS API."""

from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models import Tenant, Customer, CustomerMetrics, ImportLog
from app.services import DataImporter, Parser1C
from app.services.importer import create_tenant
from app.metrics import MetricsCalculator
from app.api.schemas import (
    TenantCreate, TenantResponse, TenantList,
    ImportStatus, FileInfo,
    CustomerResponse, CustomerList, CustomerMetricsResponse, CustomerWithMetrics,
    CalculationResult, DashboardStats, SegmentStats
)

router = APIRouter(prefix="/api", tags=["LCS API"])


# ============================================
# Tenant Endpoints
# ============================================

@router.post("/tenants", response_model=TenantResponse)
def create_new_tenant(
    tenant: TenantCreate,
    db: Session = Depends(get_db)
):
    """Create a new tenant (1C database)."""
    # Check if code already exists
    existing = db.query(Tenant).filter(Tenant.code == tenant.code).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Tenant with code '{tenant.code}' already exists")

    new_tenant = create_tenant(
        db=db,
        code=tenant.code,
        name=tenant.name,
        import_path=tenant.import_path
    )
    return new_tenant


@router.get("/tenants", response_model=TenantList)
def list_tenants(db: Session = Depends(get_db)):
    """List all tenants."""
    tenants = db.query(Tenant).filter(Tenant.is_active == True).all()
    return TenantList(items=tenants, total=len(tenants))


@router.get("/tenants/{tenant_id}", response_model=TenantResponse)
def get_tenant(tenant_id: str, db: Session = Depends(get_db)):
    """Get tenant by ID."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant


# ============================================
# Import Endpoints
# ============================================

@router.get("/tenants/{tenant_id}/files", response_model=List[FileInfo])
def list_import_files(tenant_id: str, db: Session = Depends(get_db)):
    """List available import files for tenant."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    parser = Parser1C(import_path=tenant.import_path)
    files = []

    for filename in parser.FILE_SCHEMAS.keys():
        info = parser.get_file_info(filename)
        files.append(FileInfo(**info))

    return files


@router.post("/tenants/{tenant_id}/import", response_model=ImportStatus)
def import_data(
    tenant_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Start data import from 1C files."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    try:
        importer = DataImporter(db, tenant_id)
        result = importer.import_all()
        return ImportStatus(
            status=result.get("status", "success"),
            started_at=result.get("started_at"),
            finished_at=result.get("finished_at"),
            files=result.get("files", {}),
            errors=result.get("errors", [])
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tenants/{tenant_id}/import/history")
def get_import_history(
    tenant_id: str,
    limit: int = Query(default=10, le=100),
    db: Session = Depends(get_db)
):
    """Get import history for tenant."""
    logs = db.query(ImportLog).filter(
        ImportLog.tenant_id == tenant_id
    ).order_by(ImportLog.started_at.desc()).limit(limit).all()

    return [
        {
            "file_name": log.file_name,
            "records_count": log.records_count,
            "status": log.status,
            "error_message": log.error_message,
            "started_at": log.started_at,
            "finished_at": log.finished_at,
        }
        for log in logs
    ]


# ============================================
# Metrics Endpoints
# ============================================

@router.post("/tenants/{tenant_id}/calculate-metrics", response_model=CalculationResult)
def calculate_metrics(tenant_id: str, db: Session = Depends(get_db)):
    """Calculate all metrics for tenant's customers."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    try:
        calculator = MetricsCalculator(db, tenant_id)
        result = calculator.calculate_all()
        return CalculationResult(
            status=result.get("status", "success"),
            customers=result.get("customers", 0),
            errors=result.get("errors", 0),
            duration_seconds=result.get("duration_seconds", 0)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# Customer Endpoints
# ============================================

@router.get("/tenants/{tenant_id}/customers", response_model=CustomerList)
def list_customers(
    tenant_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, le=500),
    search: Optional[str] = None,
    segment: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List customers with pagination and filtering."""
    query = db.query(Customer).filter(Customer.tenant_id == tenant_id)

    # Search by name
    if search:
        query = query.filter(Customer.name.ilike(f"%{search}%"))

    # Filter by segment (requires join with metrics)
    if segment:
        query = query.join(
            CustomerMetrics,
            (Customer.id == CustomerMetrics.customer_id) &
            (Customer.tenant_id == CustomerMetrics.tenant_id)
        ).filter(CustomerMetrics.rfm_segment == segment)

    total = query.count()
    customers = query.offset((page - 1) * page_size).limit(page_size).all()

    return CustomerList(
        items=customers,
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/tenants/{tenant_id}/customers/{customer_id}", response_model=CustomerWithMetrics)
def get_customer(tenant_id: str, customer_id: str, db: Session = Depends(get_db)):
    """Get customer with metrics."""
    customer = db.query(Customer).filter(
        Customer.id == customer_id,
        Customer.tenant_id == tenant_id
    ).first()

    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    metrics = db.query(CustomerMetrics).filter(
        CustomerMetrics.customer_id == customer_id,
        CustomerMetrics.tenant_id == tenant_id
    ).first()

    response = CustomerWithMetrics(
        id=str(customer.id),
        code=customer.code,
        name=customer.name,
        accumulated_amount=float(customer.accumulated_amount) if customer.accumulated_amount else None,
        birth_date=customer.birth_date,
        is_active=customer.is_active,
        group_id=str(customer.group_id) if customer.group_id else None,
        metrics=CustomerMetricsResponse(
            customer_id=str(metrics.customer_id),
            **{k: getattr(metrics, k) for k in CustomerMetricsResponse.model_fields.keys() if k != 'customer_id'}
        ) if metrics else None
    )

    return response


@router.get("/tenants/{tenant_id}/customers/{customer_id}/metrics", response_model=CustomerMetricsResponse)
def get_customer_metrics(tenant_id: str, customer_id: str, db: Session = Depends(get_db)):
    """Get metrics for a specific customer."""
    metrics = db.query(CustomerMetrics).filter(
        CustomerMetrics.customer_id == customer_id,
        CustomerMetrics.tenant_id == tenant_id
    ).first()

    if not metrics:
        raise HTTPException(status_code=404, detail="Metrics not found for customer")

    return metrics


# ============================================
# Dashboard Endpoints
# ============================================

@router.get("/tenants/{tenant_id}/dashboard", response_model=DashboardStats)
def get_dashboard(tenant_id: str, db: Session = Depends(get_db)):
    """Get dashboard statistics."""
    # Count customers
    total_customers = db.query(func.count(Customer.id)).filter(
        Customer.tenant_id == tenant_id
    ).scalar() or 0

    # Metrics aggregates
    metrics_query = db.query(CustomerMetrics).filter(
        CustomerMetrics.tenant_id == tenant_id
    )

    active_customers = metrics_query.filter(
        CustomerMetrics.is_active == True
    ).count()

    new_customers = metrics_query.filter(
        CustomerMetrics.is_new == True
    ).count()

    churned_customers = metrics_query.filter(
        CustomerMetrics.is_churned == True
    ).count()

    # Revenue
    total_revenue = db.query(func.sum(CustomerMetrics.total_revenue)).filter(
        CustomerMetrics.tenant_id == tenant_id
    ).scalar() or 0

    avg_check = db.query(func.avg(CustomerMetrics.avg_check)).filter(
        CustomerMetrics.tenant_id == tenant_id
    ).scalar() or 0

    # RFM segments
    rfm_segments = []
    rfm_counts = db.query(
        CustomerMetrics.rfm_segment,
        func.count(CustomerMetrics.id),
        func.sum(CustomerMetrics.total_revenue)
    ).filter(
        CustomerMetrics.tenant_id == tenant_id,
        CustomerMetrics.rfm_segment != None
    ).group_by(CustomerMetrics.rfm_segment).all()

    for segment, count, revenue in rfm_counts:
        rfm_segments.append(SegmentStats(
            segment=segment,
            count=count,
            percentage=count / total_customers * 100 if total_customers > 0 else 0,
            total_revenue=float(revenue or 0)
        ))

    # Lifecycle segments
    lifecycle_segments = []
    lifecycle_counts = db.query(
        CustomerMetrics.lifecycle_stage,
        func.count(CustomerMetrics.id),
        func.sum(CustomerMetrics.total_revenue)
    ).filter(
        CustomerMetrics.tenant_id == tenant_id,
        CustomerMetrics.lifecycle_stage != None
    ).group_by(CustomerMetrics.lifecycle_stage).all()

    for stage, count, revenue in lifecycle_counts:
        lifecycle_segments.append(SegmentStats(
            segment=stage,
            count=count,
            percentage=count / total_customers * 100 if total_customers > 0 else 0,
            total_revenue=float(revenue or 0)
        ))

    # ABC segments
    abc_segments = []
    abc_counts = db.query(
        CustomerMetrics.abc_segment,
        func.count(CustomerMetrics.id),
        func.sum(CustomerMetrics.total_revenue)
    ).filter(
        CustomerMetrics.tenant_id == tenant_id,
        CustomerMetrics.abc_segment != None
    ).group_by(CustomerMetrics.abc_segment).all()

    for segment, count, revenue in abc_counts:
        abc_segments.append(SegmentStats(
            segment=segment,
            count=count,
            percentage=count / total_customers * 100 if total_customers > 0 else 0,
            total_revenue=float(revenue or 0)
        ))

    return DashboardStats(
        total_customers=total_customers,
        active_customers=active_customers,
        new_customers=new_customers,
        churned_customers=churned_customers,
        total_revenue=float(total_revenue),
        avg_check=float(avg_check),
        rfm_segments=rfm_segments,
        lifecycle_segments=lifecycle_segments,
        abc_segments=abc_segments
    )


# ============================================
# Health Check
# ============================================

@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "lcs-etl"}
