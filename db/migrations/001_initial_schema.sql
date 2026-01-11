-- LCS: Customer Segmentation System
-- Initial database schema
-- Multi-tenant architecture

USE master;
GO

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'lcs')
BEGIN
    CREATE DATABASE lcs;
END
GO

USE lcs;
GO

-- ============================================
-- TENANTS (1C databases)
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tenants')
CREATE TABLE tenants (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    code NVARCHAR(50) UNIQUE NOT NULL,
    name NVARCHAR(255) NOT NULL,
    import_path NVARCHAR(500),
    created_at DATETIME2 DEFAULT GETDATE(),
    is_active BIT DEFAULT 1
);
GO

-- ============================================
-- СПРАВОЧНИКИ
-- ============================================

-- Группы клиентов
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer_groups')
CREATE TABLE customer_groups (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    code NVARCHAR(50),
    name NVARCHAR(255),
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

-- Клиенты
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers')
CREATE TABLE customers (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    code NVARCHAR(50),
    name NVARCHAR(255),
    accumulated_amount DECIMAL(18,2),
    birth_date DATE,
    is_active BIT DEFAULT 1,
    group_id UNIQUEIDENTIFIER,
    last_updated DATETIME2,
    created_at DATETIME2 DEFAULT GETDATE(),
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_customers_tenant ON customers(tenant_id);
CREATE INDEX ix_customers_group ON customers(tenant_id, group_id);
GO

-- Торговые точки
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stores')
CREATE TABLE stores (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    code NVARCHAR(50),
    name NVARCHAR(255),
    manager_id UNIQUEIDENTIFIER,
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

-- Сотрудники
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'employees')
CREATE TABLE employees (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    code NVARCHAR(50),
    name NVARCHAR(255),
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

-- Менеджеры касс
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'managers')
CREATE TABLE managers (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    code NVARCHAR(50),
    name NVARCHAR(255),
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

-- Номенклатура
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'products')
CREATE TABLE products (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    code NVARCHAR(50),
    name NVARCHAR(500),
    category NVARCHAR(255),
    category_confidence DECIMAL(3,2),
    classified_at DATETIME2,
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_products_tenant ON products(tenant_id);
CREATE INDEX ix_products_category ON products(tenant_id, category);
GO

-- Скидки / Условия
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'discounts')
CREATE TABLE discounts (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    name NVARCHAR(255),
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

-- Идентификаторы клиентов (карты лояльности)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer_identifiers')
CREATE TABLE customer_identifiers (
    id BIGINT IDENTITY PRIMARY KEY,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    customer_id UNIQUEIDENTIFIER NOT NULL,
    identifier NVARCHAR(100) NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_identifiers_tenant_customer ON customer_identifiers(tenant_id, customer_id);
CREATE INDEX ix_identifiers_identifier ON customer_identifiers(identifier);
GO

-- ============================================
-- ТРАНЗАКЦИИ
-- ============================================

-- Заголовки чеков
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'transactions')
CREATE TABLE transactions (
    id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    customer_id UNIQUEIDENTIFIER,
    transaction_date DATETIME2 NOT NULL,
    transaction_hour INT,
    amount DECIMAL(18,2),
    amount_before_discount DECIMAL(18,2),
    discount_percent DECIMAL(5,2),
    store_id UNIQUEIDENTIFIER,
    employee_id UNIQUEIDENTIFIER,
    duration_seconds INT,
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_transactions_tenant_date ON transactions(tenant_id, transaction_date);
CREATE INDEX ix_transactions_customer ON transactions(tenant_id, customer_id);
CREATE INDEX ix_transactions_store ON transactions(tenant_id, store_id);
GO

-- Строки чеков
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'transaction_items')
CREATE TABLE transaction_items (
    id BIGINT IDENTITY PRIMARY KEY,
    transaction_id UNIQUEIDENTIFIER NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    product_id UNIQUEIDENTIFIER NOT NULL,
    quantity DECIMAL(18,3),
    price DECIMAL(18,2),
    price_before_discount DECIMAL(18,2),
    discount_id UNIQUEIDENTIFIER,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_items_transaction ON transaction_items(tenant_id, transaction_id);
CREATE INDEX ix_items_product ON transaction_items(tenant_id, product_id);
GO

-- ============================================
-- БОНУСНАЯ СИСТЕМА
-- ============================================

-- Движения бонусов
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bonus_movements')
CREATE TABLE bonus_movements (
    id BIGINT IDENTITY PRIMARY KEY,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    customer_id UNIQUEIDENTIFIER NOT NULL,
    transaction_id UNIQUEIDENTIFIER,
    amount DECIMAL(18,2),
    movement_type NVARCHAR(20) NOT NULL, -- 'accrual' / 'redemption'
    movement_date DATETIME2,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_bonus_tenant_customer ON bonus_movements(tenant_id, customer_id);
CREATE INDEX ix_bonus_date ON bonus_movements(tenant_id, movement_date);
GO

-- Остатки бонусов
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bonus_balances')
CREATE TABLE bonus_balances (
    id BIGINT IDENTITY PRIMARY KEY,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    customer_id UNIQUEIDENTIFIER NOT NULL,
    balance DECIMAL(18,2),
    updated_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    UNIQUE (tenant_id, customer_id)
);
GO

-- ============================================
-- МЕТРИКИ КЛИЕНТОВ
-- ============================================

-- Рассчитанные метрики (денормализованная таблица для быстрого доступа)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer_metrics')
CREATE TABLE customer_metrics (
    id BIGINT IDENTITY PRIMARY KEY,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    customer_id UNIQUEIDENTIFIER NOT NULL,

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
    rfm_segment NVARCHAR(50),

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
    lifecycle_stage NVARCHAR(50),
    sleep_days INT,
    sleep_factor DECIMAL(18,4),
    is_new BIT,
    is_active BIT,
    is_sleeping BIT,
    is_churned BIT,
    cohort NVARCHAR(7), -- YYYY-MM

    -- Ценность клиента (11 метрик)
    clv_historical DECIMAL(18,2),
    clv_predicted DECIMAL(18,2),
    clv_segment NVARCHAR(50),
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
    churn_risk_segment NVARCHAR(50),
    predicted_orders_30d DECIMAL(18,4),
    predicted_orders_90d DECIMAL(18,4),
    predicted_revenue_30d DECIMAL(18,2),

    -- Продуктовые предпочтения (5 метрик)
    favorite_category NVARCHAR(255),
    favorite_sku NVARCHAR(500),
    category_diversity INT,
    sku_diversity INT,
    cross_sell_potential DECIMAL(5,4),

    -- Метаданные
    calculated_at DATETIME2 DEFAULT GETDATE(),

    FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    UNIQUE (tenant_id, customer_id)
);
GO

CREATE INDEX ix_metrics_tenant ON customer_metrics(tenant_id);
CREATE INDEX ix_metrics_rfm ON customer_metrics(tenant_id, rfm_segment);
CREATE INDEX ix_metrics_lifecycle ON customer_metrics(tenant_id, lifecycle_stage);
CREATE INDEX ix_metrics_abc ON customer_metrics(tenant_id, abc_segment);
GO

-- ============================================
-- АНКЕТЫ КЛИЕНТОВ
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer_surveys')
CREATE TABLE customer_surveys (
    id BIGINT IDENTITY PRIMARY KEY,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    customer_id UNIQUEIDENTIFIER NOT NULL,
    survey_data NVARCHAR(MAX), -- JSON с ответами
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

-- ============================================
-- ИСТОРИЯ ИМПОРТА
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'import_logs')
CREATE TABLE import_logs (
    id BIGINT IDENTITY PRIMARY KEY,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    file_name NVARCHAR(255),
    records_count INT,
    status NVARCHAR(50), -- 'success', 'error', 'partial'
    error_message NVARCHAR(MAX),
    started_at DATETIME2,
    finished_at DATETIME2,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
GO

CREATE INDEX ix_import_logs_tenant ON import_logs(tenant_id, started_at DESC);
GO

PRINT 'LCS database schema created successfully';
GO
