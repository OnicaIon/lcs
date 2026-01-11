"""FastAPI routes for LCS API."""

from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models import Tenant, Customer, CustomerMetrics, ImportLog
from app.services import DataImporter, Parser1C, ProductClassifier
from app.services.importer import create_tenant
from app.metrics import (
    MetricsCalculator,
    ProductMetricsCalculator,
    DiscountMetricsCalculator,
    TimeMetricsCalculator,
)
from app.api.schemas import (
    TenantCreate, TenantResponse, TenantList,
    ImportStatus, FileInfo,
    CustomerResponse, CustomerList, CustomerMetricsResponse, CustomerWithMetrics,
    CalculationResult, DashboardStats, SegmentStats,
    ClassificationResult, CategoryStats
)

router = APIRouter(prefix="/api", tags=["LCS API"])
admin_router = APIRouter(tags=["Admin"])

# ============================================
# Admin UI
# ============================================

ADMIN_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LCS Admin</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh; padding: 2rem; color: #fff;
        }
        .container { max-width: 900px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 2rem; font-size: 2.5rem; }
        .status { background: #0f3460; border-radius: 12px; padding: 1.5rem; margin-bottom: 2rem; }
        .status h2 { margin-bottom: 1rem; color: #e94560; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 1rem; }
        .stat { background: #1a1a2e; padding: 1rem; border-radius: 8px; text-align: center; }
        .stat-value { font-size: 1.8rem; font-weight: bold; color: #e94560; }
        .stat-label { font-size: 0.9rem; color: #aaa; margin-top: 0.3rem; }
        .actions { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; }
        .action-card {
            background: #0f3460; border-radius: 12px; padding: 1.5rem;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .action-card:hover { transform: translateY(-5px); box-shadow: 0 10px 30px rgba(233,69,96,0.3); }
        .action-card h3 { margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.5rem; }
        .action-card p { color: #aaa; font-size: 0.9rem; margin-bottom: 1rem; }
        button {
            width: 100%; padding: 1rem; border: none; border-radius: 8px;
            font-size: 1rem; font-weight: 600; cursor: pointer;
            transition: background 0.2s;
        }
        .btn-import { background: #4caf50; color: white; }
        .btn-import:hover { background: #45a049; }
        .btn-classify { background: #2196f3; color: white; }
        .btn-classify:hover { background: #1976d2; }
        .btn-metrics { background: #ff9800; color: white; }
        .btn-metrics:hover { background: #f57c00; }
        button:disabled { background: #555; cursor: not-allowed; }
        .log {
            background: #1a1a2e; border-radius: 8px; padding: 1rem;
            margin-top: 2rem; max-height: 300px; overflow-y: auto;
            font-family: monospace; font-size: 0.85rem;
        }
        .log-entry { padding: 0.3rem 0; border-bottom: 1px solid #333; }
        .log-time { color: #666; }
        .log-success { color: #4caf50; }
        .log-error { color: #f44336; }
        .log-info { color: #2196f3; }
        .progress { height: 8px; background: #1a1a2e; border-radius: 4px; margin-top: 0.5rem; overflow: hidden; }
        .progress-bar { height: 100%; background: #e94560; transition: width 0.3s; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏪 LCS Admin Panel</h1>

        <div class="status">
            <h2>📊 Статус системы</h2>
            <div class="status-grid">
                <div class="stat">
                    <div class="stat-value" id="customers">-</div>
                    <div class="stat-label">Клиентов</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="transactions">-</div>
                    <div class="stat-label">Транзакций</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="products">-</div>
                    <div class="stat-label">Товаров</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="classified">-</div>
                    <div class="stat-label">Классифицировано</div>
                </div>
            </div>
            <div class="progress"><div class="progress-bar" id="progress" style="width: 0%"></div></div>
        </div>

        <div class="actions">
            <div class="action-card">
                <h3>🔄 Импорт данных</h3>
                <p>Повторный импорт всех данных из файлов 1С. Обновит клиентов, транзакции и товары.</p>
                <button class="btn-import" onclick="runAction('import')">Запустить импорт</button>
            </div>

            <div class="action-card">
                <h3>🤖 Классификация товаров</h3>
                <p>LLM классификация товаров по категориям с помощью Ollama.</p>
                <button class="btn-classify" onclick="runAction('classify')">Запустить классификацию</button>
            </div>

            <div class="action-card">
                <h3>📊 Пересчёт метрик</h3>
                <p>Пересчитать все 51 метрику для всех клиентов.</p>
                <button class="btn-metrics" onclick="runAction('metrics')">Пересчитать метрики</button>
            </div>
        </div>

        <div class="log" id="log">
            <div class="log-entry log-info">Готов к работе...</div>
        </div>
    </div>

    <script>
        const TENANT_ID = '74f182c9-959c-41aa-9364-84d0eb533f20';

        function log(msg, type = 'info') {
            const logEl = document.getElementById('log');
            const time = new Date().toLocaleTimeString();
            logEl.innerHTML = `<div class="log-entry log-${type}"><span class="log-time">[${time}]</span> ${msg}</div>` + logEl.innerHTML;
        }

        async function loadStats() {
            try {
                const resp = await fetch(`/api/tenants/${TENANT_ID}/stats`);
                if (resp.ok) {
                    const data = await resp.json();
                    document.getElementById('customers').textContent = data.total_customers?.toLocaleString() || '-';
                    document.getElementById('transactions').textContent = data.total_transactions?.toLocaleString() || '-';
                    document.getElementById('products').textContent = data.total_products?.toLocaleString() || '-';
                    document.getElementById('classified').textContent =
                        data.classified_products ? `${data.classified_products} (${data.classification_pct}%)` : '-';
                    document.getElementById('progress').style.width = (data.classification_pct || 0) + '%';
                }
            } catch (e) { console.error(e); }
        }

        async function runAction(action) {
            const endpoints = {
                'import': `/api/tenants/${TENANT_ID}/import`,
                'classify': `/api/tenants/${TENANT_ID}/classify-products`,
                'metrics': `/api/tenants/${TENANT_ID}/calculate-metrics`
            };
            const names = {
                'import': 'Импорт',
                'classify': 'Классификация',
                'metrics': 'Пересчёт метрик'
            };

            log(`Запуск: ${names[action]}...`, 'info');

            try {
                const resp = await fetch(endpoints[action], { method: 'POST' });
                const data = await resp.json();

                if (resp.ok) {
                    log(`${names[action]} завершён: ${JSON.stringify(data)}`, 'success');
                } else {
                    log(`Ошибка: ${data.detail || JSON.stringify(data)}`, 'error');
                }
            } catch (e) {
                log(`Ошибка сети: ${e.message}`, 'error');
            }

            loadStats();
        }

        // Load stats on page load and every 30 seconds
        loadStats();
        setInterval(loadStats, 30000);
    </script>
</body>
</html>
"""

@admin_router.get("/admin", response_class=HTMLResponse)
def admin_page():
    """Admin UI for managing imports and classifications."""
    return ADMIN_HTML


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


@router.get("/tenants/{tenant_id}/stats")
def get_tenant_stats(tenant_id: str, db: Session = Depends(get_db)):
    """Get tenant statistics for admin panel."""
    from app.models import Transaction, Product

    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    total_customers = db.query(func.count(Customer.id)).filter(
        Customer.tenant_id == tenant_id
    ).scalar() or 0

    total_transactions = db.query(func.count(Transaction.id)).filter(
        Transaction.tenant_id == tenant_id
    ).scalar() or 0

    total_products = db.query(func.count(Product.id)).filter(
        Product.tenant_id == tenant_id
    ).scalar() or 0

    classified_products = db.query(func.count(Product.id)).filter(
        Product.tenant_id == tenant_id,
        Product.category.isnot(None)
    ).scalar() or 0

    classification_pct = round(100 * classified_products / total_products, 1) if total_products > 0 else 0

    return {
        "total_customers": total_customers,
        "total_transactions": total_transactions,
        "total_products": total_products,
        "classified_products": classified_products,
        "classification_pct": classification_pct
    }


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
# Classification Endpoints
# ============================================

@router.post("/tenants/{tenant_id}/classify-products", response_model=ClassificationResult)
def classify_products(
    tenant_id: str,
    force: bool = Query(default=False, description="Reclassify all products"),
    db: Session = Depends(get_db)
):
    """Classify products using LLM."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    try:
        classifier = ProductClassifier(db, tenant_id)
        result = classifier.classify_all(force=force)
        return ClassificationResult(
            status=result.get("status", "success"),
            total=result.get("total", 0),
            classified=result.get("classified", 0),
            errors=result.get("errors", 0),
            duration_seconds=result.get("duration_seconds", 0)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tenants/{tenant_id}/product-categories", response_model=List[CategoryStats])
def get_product_categories(tenant_id: str, db: Session = Depends(get_db)):
    """Get product category statistics."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    classifier = ProductClassifier(db, tenant_id)
    stats = classifier.get_category_stats()
    return [CategoryStats(**s) for s in stats]


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
# Product & Category Analytics Endpoints
# ============================================

@router.get("/tenants/{tenant_id}/analytics/products")
def get_product_analytics(tenant_id: str, db: Session = Depends(get_db)):
    """Get comprehensive product and category analytics."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = ProductMetricsCalculator(db, tenant_id)
    return {
        "category_stats": calculator.calc_category_stats(),
        "top_products": calculator.calc_top_products(limit=50),
        "product_abc": calculator.calc_product_abc(),
        "basket_analysis": calculator.calc_basket_analysis(),
    }


@router.get("/tenants/{tenant_id}/analytics/products/categories")
def get_category_analytics(tenant_id: str, db: Session = Depends(get_db)):
    """Get category-level analytics."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = ProductMetricsCalculator(db, tenant_id)
    return {
        "stats": calculator.calc_category_stats(),
        "trends": calculator.calc_category_trends(),
        "penetration": calculator.calc_category_customer_penetration(),
    }


@router.get("/tenants/{tenant_id}/analytics/products/cross-sell")
def get_cross_sell_analytics(tenant_id: str, db: Session = Depends(get_db)):
    """Get cross-sell matrix and recommendations."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = ProductMetricsCalculator(db, tenant_id)
    return {
        "cross_sell_matrix": calculator.calc_cross_sell_matrix(),
        "basket_analysis": calculator.calc_basket_analysis(),
    }


@router.get("/tenants/{tenant_id}/analytics/products/velocity")
def get_product_velocity(tenant_id: str, db: Session = Depends(get_db)):
    """Get product velocity (sales per day)."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = ProductMetricsCalculator(db, tenant_id)
    return calculator.calc_product_velocity()


@router.get("/tenants/{tenant_id}/analytics/products/price-segments")
def get_price_segments(tenant_id: str, db: Session = Depends(get_db)):
    """Get price segment analysis."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = ProductMetricsCalculator(db, tenant_id)
    return calculator.calc_price_segments()


# ============================================
# Discount Analytics Endpoints
# ============================================

@router.get("/tenants/{tenant_id}/analytics/discounts")
def get_discount_analytics(tenant_id: str, db: Session = Depends(get_db)):
    """Get comprehensive discount analytics."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = DiscountMetricsCalculator(db, tenant_id)
    return {
        "overall_stats": calculator.calc_overall_discount_stats(),
        "by_category": calculator.calc_discount_by_category(),
        "brackets": calculator.calc_discount_brackets(),
        "effectiveness": calculator.calc_discount_effectiveness(),
    }


@router.get("/tenants/{tenant_id}/analytics/discounts/trends")
def get_discount_trends(tenant_id: str, db: Session = Depends(get_db)):
    """Get discount trends over time."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = DiscountMetricsCalculator(db, tenant_id)
    return calculator.calc_discount_trends()


@router.get("/tenants/{tenant_id}/analytics/discounts/customers")
def get_customer_discount_behavior(tenant_id: str, db: Session = Depends(get_db)):
    """Get customer discount behavior analysis."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = DiscountMetricsCalculator(db, tenant_id)
    return {
        "behavior_segments": calculator.calc_customer_discount_behavior(),
        "by_rfm_segment": calculator.calc_discount_by_customer_segment(),
    }


@router.get("/tenants/{tenant_id}/analytics/discounts/margin-impact")
def get_margin_impact(tenant_id: str, db: Session = Depends(get_db)):
    """Get discount impact on margins."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = DiscountMetricsCalculator(db, tenant_id)
    return {
        "margin_impact": calculator.calc_margin_impact(),
        "cannibalization": calculator.calc_discount_cannibalization(),
    }


# ============================================
# Time-based Analytics Endpoints
# ============================================

@router.get("/tenants/{tenant_id}/analytics/time")
def get_time_analytics(tenant_id: str, db: Session = Depends(get_db)):
    """Get comprehensive time-based analytics."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = TimeMetricsCalculator(db, tenant_id)
    return {
        "day_of_week": calculator.calc_day_of_week_analysis(),
        "hour_of_day": calculator.calc_hour_of_day_analysis(),
        "seasonality": calculator.calc_seasonality(),
        "peak_periods": calculator.calc_peak_periods(),
    }


@router.get("/tenants/{tenant_id}/analytics/time/trends")
def get_time_trends(tenant_id: str, db: Session = Depends(get_db)):
    """Get sales trends over time."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = TimeMetricsCalculator(db, tenant_id)
    return {
        "monthly": calculator.calc_monthly_trends(),
        "weekly": calculator.calc_weekly_trends(),
        "yoy_comparison": calculator.calc_year_over_year(),
    }


@router.get("/tenants/{tenant_id}/analytics/cohorts")
def get_cohort_analytics(tenant_id: str, db: Session = Depends(get_db)):
    """Get cohort analysis."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    calculator = TimeMetricsCalculator(db, tenant_id)
    return {
        "retention": calculator.calc_cohort_retention(),
        "revenue": calculator.calc_cohort_revenue(),
    }


# ============================================
# Health Check
# ============================================

@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "lcs-etl"}
