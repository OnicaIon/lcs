"""Discount analytics metrics calculator."""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session


class DiscountMetricsCalculator:
    """Calculate discount-related metrics and analytics."""

    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id
        self.today = date.today()

    def calculate_all(self) -> dict:
        """Calculate all discount metrics."""
        started_at = datetime.utcnow()

        results = {
            "overall_stats": self.calc_overall_discount_stats(),
            "by_category": self.calc_discount_by_category(),
            "by_customer_segment": self.calc_discount_by_customer_segment(),
            "discount_brackets": self.calc_discount_brackets(),
            "discount_trends": self.calc_discount_trends(),
            "discount_effectiveness": self.calc_discount_effectiveness(),
            "customer_discount_behavior": self.calc_customer_discount_behavior(),
            "product_discount_analysis": self.calc_product_discount_analysis(),
            "margin_impact": self.calc_margin_impact(),
        }

        return {
            "status": "success",
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            "metrics_calculated": len(results),
        }

    def calc_overall_discount_stats(self) -> Dict:
        """Calculate overall discount statistics."""
        query = text("""
            SELECT
                COUNT(*) as total_transactions,
                SUM(CASE WHEN amount < amount_before_discount THEN 1 ELSE 0 END) as discounted_transactions,
                SUM(amount) as total_revenue,
                SUM(amount_before_discount) as total_revenue_before_discount,
                SUM(amount_before_discount - amount) as total_discount_amount,
                AVG(CASE WHEN amount_before_discount > 0
                    THEN 100.0 * (amount_before_discount - amount) / amount_before_discount
                    ELSE 0 END) as avg_discount_pct,
                MAX(CASE WHEN amount_before_discount > 0
                    THEN 100.0 * (amount_before_discount - amount) / amount_before_discount
                    ELSE 0 END) as max_discount_pct
            FROM transactions
            WHERE tenant_id = :tenant_id
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        row = result.fetchone()

        if not row:
            return {}

        return {
            "total_transactions": row[0] or 0,
            "discounted_transactions": row[1] or 0,
            "discount_rate": round(100.0 * (row[1] or 0) / (row[0] or 1), 2),
            "total_revenue": float(row[2] or 0),
            "total_revenue_before_discount": float(row[3] or 0),
            "total_discount_amount": float(row[4] or 0),
            "avg_discount_pct": round(float(row[5] or 0), 2),
            "max_discount_pct": round(float(row[6] or 0), 2),
            "discount_to_revenue_ratio": round(
                100.0 * float(row[4] or 0) / float(row[3] or 1), 2
            ),
        }

    def calc_discount_by_category(self) -> List[Dict]:
        """Calculate discount metrics by product category."""
        query = text("""
            SELECT
                COALESCE(p.category, 'Без категории') as category,
                COUNT(DISTINCT ti.transaction_id) as transactions,
                SUM(ti.quantity * ti.price) as revenue,
                SUM(ti.quantity * ti.price_before_discount) as revenue_before_discount,
                SUM(ti.quantity * (ti.price_before_discount - ti.price)) as discount_amount,
                AVG(CASE WHEN ti.price_before_discount > 0
                    THEN 100.0 * (ti.price_before_discount - ti.price) / ti.price_before_discount
                    ELSE 0 END) as avg_discount_pct,
                SUM(CASE WHEN ti.price < ti.price_before_discount THEN ti.quantity ELSE 0 END) as discounted_items,
                SUM(ti.quantity) as total_items
            FROM transaction_items ti
            JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
            WHERE ti.tenant_id = :tenant_id
            GROUP BY COALESCE(p.category, 'Без категории')
            ORDER BY discount_amount DESC
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "category": row[0],
                "transactions": row[1],
                "revenue": float(row[2] or 0),
                "revenue_before_discount": float(row[3] or 0),
                "discount_amount": float(row[4] or 0),
                "avg_discount_pct": round(float(row[5] or 0), 2),
                "discounted_items": int(float(row[6] or 0)),
                "total_items": int(float(row[7] or 0)),
                "discount_item_rate": round(100.0 * float(row[6] or 0) / float(row[7] or 1), 2),
            }
            for row in rows
        ]

    def calc_discount_by_customer_segment(self) -> List[Dict]:
        """Calculate discount usage by RFM customer segments."""
        query = text("""
            SELECT
                COALESCE(cm.rfm_segment, 'Неопределён') as segment,
                COUNT(DISTINCT t.id) as transactions,
                COUNT(DISTINCT t.customer_id) as customers,
                SUM(t.amount) as revenue,
                SUM(t.amount_before_discount - t.amount) as discount_amount,
                AVG(CASE WHEN t.amount_before_discount > 0
                    THEN 100.0 * (t.amount_before_discount - t.amount) / t.amount_before_discount
                    ELSE 0 END) as avg_discount_pct,
                AVG(t.amount) as avg_check,
                AVG(t.amount_before_discount) as avg_check_before_discount
            FROM transactions t
            LEFT JOIN customer_metrics cm ON t.customer_id = cm.customer_id AND t.tenant_id = cm.tenant_id
            WHERE t.tenant_id = :tenant_id AND t.customer_id IS NOT NULL
            GROUP BY COALESCE(cm.rfm_segment, 'Неопределён')
            ORDER BY revenue DESC
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "segment": row[0],
                "transactions": row[1],
                "customers": row[2],
                "revenue": float(row[3] or 0),
                "discount_amount": float(row[4] or 0),
                "avg_discount_pct": round(float(row[5] or 0), 2),
                "avg_check": float(row[6] or 0),
                "avg_check_before_discount": float(row[7] or 0),
            }
            for row in rows
        ]

    def calc_discount_brackets(self) -> List[Dict]:
        """Analyze transactions by discount percentage brackets."""
        query = text("""
            WITH discount_data AS (
                SELECT
                    t.id,
                    t.amount,
                    t.amount_before_discount,
                    CASE WHEN t.amount_before_discount > 0
                        THEN 100.0 * (t.amount_before_discount - t.amount) / t.amount_before_discount
                        ELSE 0 END as discount_pct
                FROM transactions t
                WHERE t.tenant_id = :tenant_id
            )
            SELECT
                CASE
                    WHEN discount_pct = 0 THEN '0% (без скидки)'
                    WHEN discount_pct <= 5 THEN '1-5%'
                    WHEN discount_pct <= 10 THEN '6-10%'
                    WHEN discount_pct <= 15 THEN '11-15%'
                    WHEN discount_pct <= 20 THEN '16-20%'
                    WHEN discount_pct <= 30 THEN '21-30%'
                    WHEN discount_pct <= 50 THEN '31-50%'
                    ELSE '50%+'
                END as discount_bracket,
                COUNT(*) as transactions,
                SUM(amount) as revenue,
                SUM(amount_before_discount - amount) as discount_given,
                AVG(amount) as avg_check,
                AVG(discount_pct) as avg_discount_in_bracket
            FROM discount_data
            GROUP BY 1
            ORDER BY MIN(discount_pct)
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "bracket": row[0],
                "transactions": row[1],
                "revenue": float(row[2] or 0),
                "discount_given": float(row[3] or 0),
                "avg_check": float(row[4] or 0),
                "avg_discount_in_bracket": round(float(row[5] or 0), 2),
            }
            for row in rows
        ]

    def calc_discount_trends(self, months: int = 12) -> List[Dict]:
        """Calculate discount trends over time."""
        query = text("""
            SELECT
                DATE_TRUNC('month', transaction_date) as month,
                COUNT(*) as transactions,
                SUM(CASE WHEN amount < amount_before_discount THEN 1 ELSE 0 END) as discounted_transactions,
                SUM(amount) as revenue,
                SUM(amount_before_discount - amount) as discount_amount,
                AVG(CASE WHEN amount_before_discount > 0
                    THEN 100.0 * (amount_before_discount - amount) / amount_before_discount
                    ELSE 0 END) as avg_discount_pct
            FROM transactions
            WHERE tenant_id = :tenant_id
              AND transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL ':months months'
            GROUP BY DATE_TRUNC('month', transaction_date)
            ORDER BY month
        """.replace(":months", str(months)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "month": row[0].strftime("%Y-%m") if row[0] else None,
                "transactions": row[1],
                "discounted_transactions": row[2],
                "discount_rate": round(100.0 * (row[2] or 0) / (row[1] or 1), 2),
                "revenue": float(row[3] or 0),
                "discount_amount": float(row[4] or 0),
                "avg_discount_pct": round(float(row[5] or 0), 2),
            }
            for row in rows
        ]

    def calc_discount_effectiveness(self) -> Dict:
        """Calculate discount effectiveness metrics."""
        # Compare metrics for discounted vs non-discounted transactions
        query = text("""
            WITH classified AS (
                SELECT
                    CASE WHEN amount < amount_before_discount THEN 'discounted' ELSE 'full_price' END as type,
                    id,
                    customer_id,
                    amount,
                    amount_before_discount,
                    (SELECT SUM(quantity) FROM transaction_items ti WHERE ti.transaction_id = transactions.id) as items
                FROM transactions
                WHERE tenant_id = :tenant_id
            )
            SELECT
                type,
                COUNT(*) as transactions,
                COUNT(DISTINCT customer_id) as customers,
                AVG(amount) as avg_check,
                AVG(items) as avg_items,
                SUM(amount) as total_revenue
            FROM classified
            GROUP BY type
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        data = {}
        for row in rows:
            data[row[0]] = {
                "transactions": row[1],
                "customers": row[2],
                "avg_check": float(row[3] or 0),
                "avg_items": float(row[4] or 0),
                "total_revenue": float(row[5] or 0),
            }

        # Calculate lift
        discounted = data.get("discounted", {})
        full_price = data.get("full_price", {})

        return {
            "discounted": discounted,
            "full_price": full_price,
            "check_lift": round(
                (discounted.get("avg_check", 0) - full_price.get("avg_check", 0))
                / full_price.get("avg_check", 1) * 100, 2
            ) if full_price.get("avg_check") else 0,
            "items_lift": round(
                (discounted.get("avg_items", 0) - full_price.get("avg_items", 0))
                / full_price.get("avg_items", 1) * 100, 2
            ) if full_price.get("avg_items") else 0,
        }

    def calc_customer_discount_behavior(self) -> List[Dict]:
        """Analyze customer behavior based on discount usage."""
        query = text("""
            WITH customer_discounts AS (
                SELECT
                    customer_id,
                    COUNT(*) as total_transactions,
                    SUM(CASE WHEN amount < amount_before_discount THEN 1 ELSE 0 END) as discounted_transactions,
                    SUM(amount) as total_revenue,
                    SUM(amount_before_discount - amount) as total_discount,
                    AVG(CASE WHEN amount_before_discount > 0
                        THEN 100.0 * (amount_before_discount - amount) / amount_before_discount
                        ELSE 0 END) as avg_discount_pct
                FROM transactions
                WHERE tenant_id = :tenant_id AND customer_id IS NOT NULL
                GROUP BY customer_id
            )
            SELECT
                CASE
                    WHEN discounted_transactions = 0 THEN 'Никогда не использует скидки'
                    WHEN 100.0 * discounted_transactions / total_transactions < 25 THEN 'Редко (< 25%)'
                    WHEN 100.0 * discounted_transactions / total_transactions < 50 THEN 'Иногда (25-50%)'
                    WHEN 100.0 * discounted_transactions / total_transactions < 75 THEN 'Часто (50-75%)'
                    ELSE 'Всегда (75%+)'
                END as discount_behavior,
                COUNT(*) as customers,
                AVG(total_transactions) as avg_transactions,
                AVG(total_revenue) as avg_revenue,
                AVG(total_discount) as avg_discount_received,
                AVG(avg_discount_pct) as avg_discount_pct
            FROM customer_discounts
            GROUP BY 1
            ORDER BY AVG(avg_discount_pct)
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "behavior": row[0],
                "customers": row[1],
                "avg_transactions": round(float(row[2] or 0), 1),
                "avg_revenue": float(row[3] or 0),
                "avg_discount_received": float(row[4] or 0),
                "avg_discount_pct": round(float(row[5] or 0), 2),
            }
            for row in rows
        ]

    def calc_product_discount_analysis(self, limit: int = 50) -> List[Dict]:
        """Analyze discount patterns by product."""
        query = text("""
            SELECT
                p.id,
                p.name,
                p.category,
                COUNT(DISTINCT ti.transaction_id) as transactions,
                SUM(ti.quantity) as total_qty,
                SUM(ti.quantity * ti.price) as revenue,
                SUM(ti.quantity * ti.price_before_discount) as revenue_before_discount,
                SUM(ti.quantity * (ti.price_before_discount - ti.price)) as discount_amount,
                AVG(CASE WHEN ti.price_before_discount > 0
                    THEN 100.0 * (ti.price_before_discount - ti.price) / ti.price_before_discount
                    ELSE 0 END) as avg_discount_pct,
                SUM(CASE WHEN ti.price < ti.price_before_discount THEN 1 ELSE 0 END) as discounted_sales,
                COUNT(*) as total_sales
            FROM transaction_items ti
            JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
            WHERE ti.tenant_id = :tenant_id
            GROUP BY p.id, p.name, p.category
            HAVING SUM(ti.quantity * (ti.price_before_discount - ti.price)) > 0
            ORDER BY discount_amount DESC
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
                "total_qty": float(row[4] or 0),
                "revenue": float(row[5] or 0),
                "revenue_before_discount": float(row[6] or 0),
                "discount_amount": float(row[7] or 0),
                "avg_discount_pct": round(float(row[8] or 0), 2),
                "discount_rate": round(100.0 * (row[9] or 0) / (row[10] or 1), 2),
            }
            for row in rows
        ]

    def calc_margin_impact(self, assumed_margin_pct: float = 30.0) -> Dict:
        """Calculate impact of discounts on margins."""
        query = text("""
            SELECT
                SUM(amount) as total_revenue,
                SUM(amount_before_discount) as total_revenue_before_discount,
                SUM(amount_before_discount - amount) as total_discount
            FROM transactions
            WHERE tenant_id = :tenant_id
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        row = result.fetchone()

        if not row:
            return {}

        revenue = float(row[0] or 0)
        revenue_before = float(row[1] or 0)
        discount = float(row[2] or 0)

        # Estimated margins
        estimated_margin_before = revenue_before * assumed_margin_pct / 100
        estimated_margin_after = revenue * assumed_margin_pct / 100 - discount
        margin_erosion = estimated_margin_before - estimated_margin_after

        return {
            "revenue": revenue,
            "revenue_before_discount": revenue_before,
            "total_discount": discount,
            "assumed_margin_pct": assumed_margin_pct,
            "estimated_margin_before_discount": round(estimated_margin_before, 2),
            "estimated_margin_after_discount": round(estimated_margin_after, 2),
            "margin_erosion": round(margin_erosion, 2),
            "margin_erosion_pct": round(
                100.0 * margin_erosion / estimated_margin_before, 2
            ) if estimated_margin_before > 0 else 0,
            "effective_margin_pct": round(
                100.0 * estimated_margin_after / revenue, 2
            ) if revenue > 0 else 0,
        }

    def calc_discount_cannibalization(self) -> Dict:
        """Analyze if discounts are cannibalizing full-price sales."""
        # Compare customer behavior before and after using discounts
        query = text("""
            WITH customer_first_discount AS (
                SELECT
                    customer_id,
                    MIN(transaction_date) as first_discount_date
                FROM transactions
                WHERE tenant_id = :tenant_id
                  AND customer_id IS NOT NULL
                  AND amount < amount_before_discount
                GROUP BY customer_id
            ),
            customer_behavior AS (
                SELECT
                    t.customer_id,
                    cfd.first_discount_date,
                    SUM(CASE WHEN t.transaction_date < cfd.first_discount_date THEN 1 ELSE 0 END) as orders_before,
                    SUM(CASE WHEN t.transaction_date >= cfd.first_discount_date THEN 1 ELSE 0 END) as orders_after,
                    SUM(CASE WHEN t.transaction_date < cfd.first_discount_date THEN t.amount ELSE 0 END) as revenue_before,
                    SUM(CASE WHEN t.transaction_date >= cfd.first_discount_date THEN t.amount ELSE 0 END) as revenue_after,
                    AVG(CASE WHEN t.transaction_date < cfd.first_discount_date THEN t.amount END) as avg_check_before,
                    AVG(CASE WHEN t.transaction_date >= cfd.first_discount_date THEN t.amount END) as avg_check_after
                FROM transactions t
                JOIN customer_first_discount cfd ON t.customer_id = cfd.customer_id
                WHERE t.tenant_id = :tenant_id
                GROUP BY t.customer_id, cfd.first_discount_date
                HAVING SUM(CASE WHEN t.transaction_date < cfd.first_discount_date THEN 1 ELSE 0 END) > 0
            )
            SELECT
                COUNT(*) as customers_analyzed,
                AVG(avg_check_before) as avg_check_before,
                AVG(avg_check_after) as avg_check_after,
                AVG(orders_before) as avg_orders_before,
                AVG(orders_after) as avg_orders_after
            FROM customer_behavior
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        row = result.fetchone()

        if not row or not row[0]:
            return {"status": "insufficient_data"}

        check_before = float(row[1] or 0)
        check_after = float(row[2] or 0)
        orders_before = float(row[3] or 0)
        orders_after = float(row[4] or 0)

        return {
            "customers_analyzed": row[0],
            "avg_check_before_discount": round(check_before, 2),
            "avg_check_after_discount": round(check_after, 2),
            "check_change_pct": round(
                100.0 * (check_after - check_before) / check_before, 2
            ) if check_before > 0 else 0,
            "avg_orders_before": round(orders_before, 1),
            "avg_orders_after": round(orders_after, 1),
            "orders_change_pct": round(
                100.0 * (orders_after - orders_before) / orders_before, 2
            ) if orders_before > 0 else 0,
        }
