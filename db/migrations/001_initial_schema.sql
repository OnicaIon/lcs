-- LCS: Customer Segmentation System
-- Initial database schema for PostgreSQL
-- Multi-tenant architecture

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- TENANTS (1C databases)
-- ============================================
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    import_path VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- ============================================
-- СПРАВОЧНИКИ
-- ============================================

-- Группы клиентов
CREATE TABLE IF NOT EXISTS customer_groups (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    code VARCHAR(50),
    name VARCHAR(255),
    PRIMARY KEY (id, tenant_id)
);

-- Клиенты
CREATE TABLE IF NOT EXISTS customers (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    code VARCHAR(50),
    name VARCHAR(255),
    accumulated_amount DECIMAL(18,2),
    birth_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    group_id UUID,
    last_updated TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, tenant_id)
);

CREATE INDEX IF NOT EXISTS ix_customers_tenant ON customers(tenant_id);
CREATE INDEX IF NOT EXISTS ix_customers_group ON customers(tenant_id, group_id);

-- Торговые точки
CREATE TABLE IF NOT EXISTS stores (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    code VARCHAR(50),
    name VARCHAR(255),
    manager_id UUID,
    PRIMARY KEY (id, tenant_id)
);

-- Сотрудники
CREATE TABLE IF NOT EXISTS employees (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    code VARCHAR(50),
    name VARCHAR(255),
    PRIMARY KEY (id, tenant_id)
);

-- Менеджеры касс
CREATE TABLE IF NOT EXISTS managers (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    code VARCHAR(50),
    name VARCHAR(255),
    PRIMARY KEY (id, tenant_id)
);

-- Номенклатура
CREATE TABLE IF NOT EXISTS products (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    code VARCHAR(50),
    name VARCHAR(500),
    category VARCHAR(255),
    category_confidence DECIMAL(3,2),
    classified_at TIMESTAMP,
    PRIMARY KEY (id, tenant_id)
);

CREATE INDEX IF NOT EXISTS ix_products_tenant ON products(tenant_id);
CREATE INDEX IF NOT EXISTS ix_products_category ON products(tenant_id, category);

-- Скидки / Условия
CREATE TABLE IF NOT EXISTS discounts (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(255),
    PRIMARY KEY (id, tenant_id)
);

-- Идентификаторы клиентов (карты лояльности)
CREATE TABLE IF NOT EXISTS customer_identifiers (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL,
    identifier VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_identifiers_tenant_customer ON customer_identifiers(tenant_id, customer_id);
CREATE INDEX IF NOT EXISTS ix_identifiers_identifier ON customer_identifiers(identifier);

-- ============================================
-- ТРАНЗАКЦИИ
-- ============================================

-- Заголовки чеков
CREATE TABLE IF NOT EXISTS transactions (
    id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID,
    transaction_date TIMESTAMP NOT NULL,
    transaction_hour INT,
    amount DECIMAL(18,2),
    amount_before_discount DECIMAL(18,2),
    discount_percent DECIMAL(5,2),
    store_id UUID,
    employee_id UUID,
    duration_seconds INT,
    PRIMARY KEY (id, tenant_id)
);

CREATE INDEX IF NOT EXISTS ix_transactions_tenant_date ON transactions(tenant_id, transaction_date);
CREATE INDEX IF NOT EXISTS ix_transactions_customer ON transactions(tenant_id, customer_id);
CREATE INDEX IF NOT EXISTS ix_transactions_store ON transactions(tenant_id, store_id);

-- Строки чеков
CREATE TABLE IF NOT EXISTS transaction_items (
    id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    product_id UUID NOT NULL,
    quantity DECIMAL(18,3),
    price DECIMAL(18,2),
    price_before_discount DECIMAL(18,2),
    discount_id UUID
);

CREATE INDEX IF NOT EXISTS ix_items_transaction ON transaction_items(tenant_id, transaction_id);
CREATE INDEX IF NOT EXISTS ix_items_product ON transaction_items(tenant_id, product_id);

-- ============================================
-- БОНУСНАЯ СИСТЕМА
-- ============================================

-- Движения бонусов
CREATE TABLE IF NOT EXISTS bonus_movements (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL,
    transaction_id UUID,
    amount DECIMAL(18,2),
    movement_type VARCHAR(20) NOT NULL, -- 'accrual' / 'redemption'
    movement_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_bonus_tenant_customer ON bonus_movements(tenant_id, customer_id);
CREATE INDEX IF NOT EXISTS ix_bonus_date ON bonus_movements(tenant_id, movement_date);

-- Остатки бонусов
CREATE TABLE IF NOT EXISTS bonus_balances (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL,
    balance DECIMAL(18,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tenant_id, customer_id)
);

-- ============================================
-- МЕТРИКИ КЛИЕНТОВ
-- ============================================

CREATE TABLE IF NOT EXISTS customer_metrics (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL,

    -- Базовые транзакционные (11 метрик)
    total_orders INT,
    total_revenue DECIMAL(18,2),
    total_items DECIMAL(18,3),
    first_order_date DATE,
    last_order_date DATE,
    avg_check DECIMAL(18,2),
    avg_items_per_order DECIMAL(18,2),
    max_check DECIMAL(18,2),
    min_check DECIMAL(18,2),
    std_check DECIMAL(18,2),
    avg_margin DECIMAL(18,2),

    -- RFM (5 метрик)
    recency INT,
    frequency DECIMAL(18,4),
    monetary DECIMAL(18,2),
    rfm_score INT,
    rfm_segment VARCHAR(50),

    -- Временные паттерны (10 метрик)
    customer_age_days INT,
    customer_age_months INT,
    avg_days_between DECIMAL(18,2),
    median_days_between DECIMAL(18,2),
    std_days_between DECIMAL(18,2),
    expected_next_order DATE,
    days_overdue INT,
    purchase_regularity DECIMAL(5,4),
    active_months INT,
    activity_rate DECIMAL(5,4),

    -- Жизненный цикл (8 метрик)
    lifecycle_stage VARCHAR(50),
    sleep_days INT,
    sleep_factor DECIMAL(18,4),
    is_new BOOLEAN,
    is_active BOOLEAN,
    is_sleeping BOOLEAN,
    is_churned BOOLEAN,
    cohort VARCHAR(7), -- YYYY-MM

    -- Ценность клиента (11 метрик)
    clv_historical DECIMAL(18,2),
    clv_predicted DECIMAL(18,2),
    clv_segment VARCHAR(50),
    abc_segment CHAR(1),
    xyz_segment CHAR(1),
    abc_xyz_segment CHAR(2),
    profit_contribution DECIMAL(8,4),
    cumulative_percentile DECIMAL(5,2),
    revenue_trend DECIMAL(8,4),
    check_trend DECIMAL(8,4),
    frequency_trend DECIMAL(8,4),

    -- Предиктивные (6 метрик)
    prob_alive DECIMAL(5,4),
    churn_probability DECIMAL(5,4),
    churn_risk_segment VARCHAR(50),
    predicted_orders_30d DECIMAL(18,4),
    predicted_orders_90d DECIMAL(18,4),
    predicted_revenue_30d DECIMAL(18,2),

    -- Продуктовые предпочтения (5 метрик)
    favorite_category VARCHAR(255),
    favorite_sku VARCHAR(500),
    category_diversity INT,
    sku_diversity INT,
    cross_sell_potential DECIMAL(5,4),

    -- Метаданные
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (tenant_id, customer_id)
);

CREATE INDEX IF NOT EXISTS ix_metrics_tenant ON customer_metrics(tenant_id);
CREATE INDEX IF NOT EXISTS ix_metrics_rfm ON customer_metrics(tenant_id, rfm_segment);
CREATE INDEX IF NOT EXISTS ix_metrics_lifecycle ON customer_metrics(tenant_id, lifecycle_stage);
CREATE INDEX IF NOT EXISTS ix_metrics_abc ON customer_metrics(tenant_id, abc_segment);

-- ============================================
-- АНКЕТЫ КЛИЕНТОВ
-- ============================================

CREATE TABLE IF NOT EXISTS customer_surveys (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL,
    survey_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ИСТОРИЯ ИМПОРТА
-- ============================================

CREATE TABLE IF NOT EXISTS import_logs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    file_name VARCHAR(255),
    records_count INT,
    status VARCHAR(50), -- 'success', 'error', 'partial'
    error_message TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_import_logs_tenant ON import_logs(tenant_id, started_at DESC);
