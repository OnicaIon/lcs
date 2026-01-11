"""Product and category metrics calculator."""

from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from decimal import Decimal

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session


class ProductMetricsCalculator:
    """Calculate product and category level metrics."""

    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id
        self.today = date.today()

    def calculate_all(self) -> dict:
        """Calculate all product and category metrics."""
        started_at = datetime.utcnow()

        results = {
            "category_stats": self.calc_category_stats(),
            "top_products": self.calc_top_products(),
            "category_trends": self.calc_category_trends(),
            "basket_analysis": self.calc_basket_analysis(),
            "product_abc": self.calc_product_abc(),
            "cross_sell": self.calc_cross_sell_matrix(),
        }

        # Save aggregated metrics
        self._save_product_metrics(results)

        return {
            "status": "success",
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            **{k: len(v) if isinstance(v, list) else "calculated" for k, v in results.items()}
        }

    def calc_category_stats(self) -> List[Dict]:
        """Calculate statistics by category."""
        query = text("""
            WITH category_data AS (
                SELECT
                    COALESCE(p.category, 'Без категории') as category,
                    COUNT(DISTINCT ti.transaction_id) as transactions,
                    COUNT(DISTINCT t.customer_id) as customers,
                    SUM(ti.quantity) as total_qty,
                    SUM(ti.quantity * ti.price) as revenue,
                    SUM(ti.quantity * ti.price_before_discount) as revenue_before_discount,
                    COUNT(DISTINCT p.id) as products_count,
                    AVG(ti.price) as avg_price,
                    MIN(ti.price) as min_price,
                    MAX(ti.price) as max_price
                FROM transaction_items ti
                JOIN transactions t ON ti.transaction_id = t.id AND ti.tenant_id = t.tenant_id
                JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
                WHERE ti.tenant_id = :tenant_id
                GROUP BY COALESCE(p.category, 'Без категории')
            ),
            totals AS (
                SELECT SUM(revenue) as total_revenue FROM category_data
            )
            SELECT
                cd.*,
                ROUND(100.0 * cd.revenue / NULLIF(t.total_revenue, 0), 2) as revenue_share,
                ROUND(cd.revenue / NULLIF(cd.transactions, 0), 2) as avg_check,
                ROUND(cd.total_qty / NULLIF(cd.transactions, 0), 2) as avg_items_per_transaction,
                ROUND(100.0 * (cd.revenue_before_discount - cd.revenue) /
                      NULLIF(cd.revenue_before_discount, 0), 2) as avg_discount_pct
            FROM category_data cd, totals t
            ORDER BY cd.revenue DESC
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "category": row[0],
                "transactions": row[1],
                "customers": row[2],
                "total_qty": float(row[3] or 0),
                "revenue": float(row[4] or 0),
                "revenue_before_discount": float(row[5] or 0),
                "products_count": row[6],
                "avg_price": float(row[7] or 0),
                "min_price": float(row[8] or 0),
                "max_price": float(row[9] or 0),
                "revenue_share": float(row[10] or 0),
                "avg_check": float(row[11] or 0),
                "avg_items_per_transaction": float(row[12] or 0),
                "avg_discount_pct": float(row[13] or 0),
            }
            for row in rows
        ]

    def calc_top_products(self, limit: int = 100) -> List[Dict]:
        """Calculate top products by revenue and quantity."""
        query = text("""
            SELECT
                p.id,
                p.name,
                p.category,
                COUNT(DISTINCT ti.transaction_id) as transactions,
                COUNT(DISTINCT t.customer_id) as customers,
                SUM(ti.quantity) as total_qty,
                SUM(ti.quantity * ti.price) as revenue,
                AVG(ti.price) as avg_price,
                MIN(t.transaction_date) as first_sale,
                MAX(t.transaction_date) as last_sale
            FROM transaction_items ti
            JOIN transactions t ON ti.transaction_id = t.id AND ti.tenant_id = t.tenant_id
            JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
            WHERE ti.tenant_id = :tenant_id
            GROUP BY p.id, p.name, p.category
            ORDER BY revenue DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id, "limit": limit})
        rows = result.fetchall()

        return [
            {
                "product_id": str(row[0]),
                "name": row[1],
                "category": row[2],
                "transactions": row[3],
                "customers": row[4],
                "total_qty": float(row[5] or 0),
                "revenue": float(row[6] or 0),
                "avg_price": float(row[7] or 0),
                "first_sale": row[8],
                "last_sale": row[9],
                "days_active": (row[9] - row[8]).days if row[8] and row[9] else 0,
            }
            for row in rows
        ]

    def calc_category_trends(self, months: int = 6) -> List[Dict]:
        """Calculate category revenue trends by month."""
        query = text("""
            SELECT
                COALESCE(p.category, 'Без категории') as category,
                DATE_TRUNC('month', t.transaction_date) as month,
                COUNT(DISTINCT ti.transaction_id) as transactions,
                COUNT(DISTINCT t.customer_id) as customers,
                SUM(ti.quantity) as total_qty,
                SUM(ti.quantity * ti.price) as revenue
            FROM transaction_items ti
            JOIN transactions t ON ti.transaction_id = t.id AND ti.tenant_id = t.tenant_id
            JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
            WHERE ti.tenant_id = :tenant_id
              AND t.transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL ':months months'
            GROUP BY COALESCE(p.category, 'Без категории'), DATE_TRUNC('month', t.transaction_date)
            ORDER BY category, month
        """.replace(":months", str(months)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "category": row[0],
                "month": row[1].strftime("%Y-%m") if row[1] else None,
                "transactions": row[2],
                "customers": row[3],
                "total_qty": float(row[4] or 0),
                "revenue": float(row[5] or 0),
            }
            for row in rows
        ]

    def calc_basket_analysis(self) -> Dict:
        """Calculate basket composition metrics."""
        # Average basket metrics
        basket_query = text("""
            SELECT
                AVG(item_count) as avg_items,
                AVG(category_count) as avg_categories,
                AVG(basket_value) as avg_basket_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY item_count) as median_items,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY basket_value) as median_basket_value
            FROM (
                SELECT
                    t.id,
                    SUM(ti.quantity) as item_count,
                    COUNT(DISTINCT p.category) as category_count,
                    SUM(ti.quantity * ti.price) as basket_value
                FROM transactions t
                JOIN transaction_items ti ON t.id = ti.transaction_id AND t.tenant_id = ti.tenant_id
                JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
                WHERE t.tenant_id = :tenant_id
                GROUP BY t.id
            ) basket_data
        """)

        result = self.db.execute(basket_query, {"tenant_id": self.tenant_id})
        row = result.fetchone()

        # Basket size distribution
        dist_query = text("""
            SELECT
                CASE
                    WHEN item_count = 1 THEN '1 товар'
                    WHEN item_count BETWEEN 2 AND 3 THEN '2-3 товара'
                    WHEN item_count BETWEEN 4 AND 5 THEN '4-5 товаров'
                    WHEN item_count BETWEEN 6 AND 10 THEN '6-10 товаров'
                    ELSE '10+ товаров'
                END as basket_size,
                COUNT(*) as transactions,
                SUM(basket_value) as total_revenue
            FROM (
                SELECT
                    t.id,
                    SUM(ti.quantity) as item_count,
                    SUM(ti.quantity * ti.price) as basket_value
                FROM transactions t
                JOIN transaction_items ti ON t.id = ti.transaction_id AND t.tenant_id = ti.tenant_id
                WHERE t.tenant_id = :tenant_id
                GROUP BY t.id
            ) basket_data
            GROUP BY 1
            ORDER BY MIN(item_count)
        """)

        dist_result = self.db.execute(dist_query, {"tenant_id": self.tenant_id})
        distribution = [
            {"basket_size": r[0], "transactions": r[1], "revenue": float(r[2] or 0)}
            for r in dist_result.fetchall()
        ]

        return {
            "avg_items_per_basket": float(row[0] or 0) if row else 0,
            "avg_categories_per_basket": float(row[1] or 0) if row else 0,
            "avg_basket_value": float(row[2] or 0) if row else 0,
            "median_items_per_basket": float(row[3] or 0) if row else 0,
            "median_basket_value": float(row[4] or 0) if row else 0,
            "distribution": distribution,
        }

    def calc_product_abc(self) -> Dict:
        """Calculate ABC analysis for products."""
        query = text("""
            WITH product_revenue AS (
                SELECT
                    p.id,
                    p.name,
                    p.category,
                    SUM(ti.quantity * ti.price) as revenue
                FROM transaction_items ti
                JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
                WHERE ti.tenant_id = :tenant_id
                GROUP BY p.id, p.name, p.category
            ),
            ranked AS (
                SELECT
                    *,
                    SUM(revenue) OVER () as total_revenue,
                    SUM(revenue) OVER (ORDER BY revenue DESC) as cumulative_revenue
                FROM product_revenue
            )
            SELECT
                id, name, category, revenue,
                ROUND(100.0 * cumulative_revenue / total_revenue, 2) as cumulative_pct,
                CASE
                    WHEN cumulative_revenue <= total_revenue * 0.8 THEN 'A'
                    WHEN cumulative_revenue <= total_revenue * 0.95 THEN 'B'
                    ELSE 'C'
                END as abc_class
            FROM ranked
            ORDER BY revenue DESC
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        products = [
            {
                "product_id": str(row[0]),
                "name": row[1],
                "category": row[2],
                "revenue": float(row[3] or 0),
                "cumulative_pct": float(row[4] or 0),
                "abc_class": row[5],
            }
            for row in rows
        ]

        # Summary by class
        summary = {"A": 0, "B": 0, "C": 0}
        revenue_by_class = {"A": 0.0, "B": 0.0, "C": 0.0}
        for p in products:
            summary[p["abc_class"]] += 1
            revenue_by_class[p["abc_class"]] += p["revenue"]

        return {
            "products": products[:500],  # Limit for storage
            "summary": {
                "A": {"count": summary["A"], "revenue": revenue_by_class["A"]},
                "B": {"count": summary["B"], "revenue": revenue_by_class["B"]},
                "C": {"count": summary["C"], "revenue": revenue_by_class["C"]},
            }
        }

    def calc_cross_sell_matrix(self, min_support: int = 10) -> List[Dict]:
        """Calculate category cross-sell matrix (frequently bought together)."""
        query = text("""
            WITH basket_categories AS (
                SELECT
                    t.id as transaction_id,
                    COALESCE(p.category, 'Без категории') as category
                FROM transactions t
                JOIN transaction_items ti ON t.id = ti.transaction_id AND t.tenant_id = ti.tenant_id
                JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
                WHERE t.tenant_id = :tenant_id
                GROUP BY t.id, COALESCE(p.category, 'Без категории')
            ),
            category_pairs AS (
                SELECT
                    a.category as category1,
                    b.category as category2,
                    COUNT(DISTINCT a.transaction_id) as co_occurrences
                FROM basket_categories a
                JOIN basket_categories b ON a.transaction_id = b.transaction_id
                WHERE a.category < b.category
                GROUP BY a.category, b.category
                HAVING COUNT(DISTINCT a.transaction_id) >= :min_support
            ),
            category_counts AS (
                SELECT category, COUNT(DISTINCT transaction_id) as total
                FROM basket_categories
                GROUP BY category
            )
            SELECT
                cp.category1,
                cp.category2,
                cp.co_occurrences,
                c1.total as cat1_total,
                c2.total as cat2_total,
                ROUND(100.0 * cp.co_occurrences / c1.total, 2) as lift_from_cat1,
                ROUND(100.0 * cp.co_occurrences / c2.total, 2) as lift_from_cat2
            FROM category_pairs cp
            JOIN category_counts c1 ON cp.category1 = c1.category
            JOIN category_counts c2 ON cp.category2 = c2.category
            ORDER BY cp.co_occurrences DESC
            LIMIT 50
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id, "min_support": min_support})
        rows = result.fetchall()

        return [
            {
                "category1": row[0],
                "category2": row[1],
                "co_occurrences": row[2],
                "cat1_total": row[3],
                "cat2_total": row[4],
                "lift_from_cat1": float(row[5] or 0),
                "lift_from_cat2": float(row[6] or 0),
            }
            for row in rows
        ]

    def calc_category_customer_penetration(self) -> List[Dict]:
        """Calculate what percentage of customers bought each category."""
        query = text("""
            WITH total_customers AS (
                SELECT COUNT(DISTINCT customer_id) as total
                FROM transactions
                WHERE tenant_id = :tenant_id AND customer_id IS NOT NULL
            ),
            category_customers AS (
                SELECT
                    COALESCE(p.category, 'Без категории') as category,
                    COUNT(DISTINCT t.customer_id) as customers
                FROM transactions t
                JOIN transaction_items ti ON t.id = ti.transaction_id AND t.tenant_id = ti.tenant_id
                JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
                WHERE t.tenant_id = :tenant_id AND t.customer_id IS NOT NULL
                GROUP BY COALESCE(p.category, 'Без категории')
            )
            SELECT
                cc.category,
                cc.customers,
                tc.total,
                ROUND(100.0 * cc.customers / tc.total, 2) as penetration_pct
            FROM category_customers cc, total_customers tc
            ORDER BY cc.customers DESC
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "category": row[0],
                "customers": row[1],
                "total_customers": row[2],
                "penetration_pct": float(row[3] or 0),
            }
            for row in rows
        ]

    def calc_new_products_performance(self, days: int = 30) -> List[Dict]:
        """Analyze performance of recently added products."""
        query = text("""
            SELECT
                p.id,
                p.name,
                p.category,
                MIN(t.transaction_date) as first_sale,
                COUNT(DISTINCT ti.transaction_id) as transactions,
                COUNT(DISTINCT t.customer_id) as customers,
                SUM(ti.quantity) as total_qty,
                SUM(ti.quantity * ti.price) as revenue
            FROM products p
            JOIN transaction_items ti ON p.id = ti.product_id AND p.tenant_id = ti.tenant_id
            JOIN transactions t ON ti.transaction_id = t.id AND ti.tenant_id = t.tenant_id
            WHERE p.tenant_id = :tenant_id
            GROUP BY p.id, p.name, p.category
            HAVING MIN(t.transaction_date) >= CURRENT_DATE - INTERVAL ':days days'
            ORDER BY revenue DESC
            LIMIT 50
        """.replace(":days", str(days)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "product_id": str(row[0]),
                "name": row[1],
                "category": row[2],
                "first_sale": row[3],
                "transactions": row[4],
                "customers": row[5],
                "total_qty": float(row[6] or 0),
                "revenue": float(row[7] or 0),
            }
            for row in rows
        ]

    def calc_price_segments(self) -> Dict:
        """Analyze products by price segments."""
        query = text("""
            WITH product_prices AS (
                SELECT
                    p.id,
                    p.name,
                    p.category,
                    AVG(ti.price) as avg_price,
                    SUM(ti.quantity) as total_qty,
                    SUM(ti.quantity * ti.price) as revenue
                FROM products p
                JOIN transaction_items ti ON p.id = ti.product_id AND p.tenant_id = ti.tenant_id
                WHERE p.tenant_id = :tenant_id
                GROUP BY p.id, p.name, p.category
            ),
            price_stats AS (
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avg_price) as q1,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_price) as median,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_price) as q3
                FROM product_prices
            )
            SELECT
                CASE
                    WHEN pp.avg_price <= ps.q1 THEN 'Эконом'
                    WHEN pp.avg_price <= ps.median THEN 'Средний-'
                    WHEN pp.avg_price <= ps.q3 THEN 'Средний+'
                    ELSE 'Премиум'
                END as price_segment,
                COUNT(*) as products_count,
                SUM(pp.total_qty) as total_qty,
                SUM(pp.revenue) as revenue,
                AVG(pp.avg_price) as avg_price
            FROM product_prices pp, price_stats ps
            GROUP BY 1
            ORDER BY MIN(pp.avg_price)
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return {
            "segments": [
                {
                    "segment": row[0],
                    "products_count": row[1],
                    "total_qty": float(row[2] or 0),
                    "revenue": float(row[3] or 0),
                    "avg_price": float(row[4] or 0),
                }
                for row in rows
            ]
        }

    def calc_product_velocity(self, limit: int = 50) -> List[Dict]:
        """Calculate product velocity (sales per day since first sale)."""
        query = text("""
            SELECT
                p.id,
                p.name,
                p.category,
                MIN(t.transaction_date) as first_sale,
                MAX(t.transaction_date) as last_sale,
                SUM(ti.quantity) as total_qty,
                SUM(ti.quantity * ti.price) as revenue,
                CASE
                    WHEN MAX(t.transaction_date) = MIN(t.transaction_date) THEN SUM(ti.quantity)
                    ELSE SUM(ti.quantity) / NULLIF(EXTRACT(DAY FROM MAX(t.transaction_date) - MIN(t.transaction_date)), 0)
                END as velocity_per_day
            FROM products p
            JOIN transaction_items ti ON p.id = ti.product_id AND p.tenant_id = ti.tenant_id
            JOIN transactions t ON ti.transaction_id = t.id AND ti.tenant_id = t.tenant_id
            WHERE p.tenant_id = :tenant_id
            GROUP BY p.id, p.name, p.category
            HAVING COUNT(DISTINCT t.id) >= 5
            ORDER BY velocity_per_day DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id, "limit": limit})
        rows = result.fetchall()

        return [
            {
                "product_id": str(row[0]),
                "name": row[1],
                "category": row[2],
                "first_sale": row[3],
                "last_sale": row[4],
                "total_qty": float(row[5] or 0),
                "revenue": float(row[6] or 0),
                "velocity_per_day": float(row[7] or 0),
            }
            for row in rows
        ]

    def _save_product_metrics(self, results: dict) -> None:
        """Save aggregated product metrics to database."""
        import json

        # Store as JSON in a metrics table
        for metric_name, data in results.items():
            query = text("""
                INSERT INTO product_metrics (tenant_id, metric_name, metric_data, calculated_at)
                VALUES (:tenant_id, :metric_name, :metric_data, :calculated_at)
                ON CONFLICT (tenant_id, metric_name)
                DO UPDATE SET metric_data = :metric_data, calculated_at = :calculated_at
            """)

            try:
                self.db.execute(query, {
                    "tenant_id": self.tenant_id,
                    "metric_name": metric_name,
                    "metric_data": json.dumps(data, default=str),
                    "calculated_at": datetime.utcnow(),
                })
            except Exception:
                # Table might not exist yet, skip saving
                pass

        self.db.commit()
