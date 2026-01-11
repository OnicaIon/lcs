"""Time-based and cohort analytics."""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session


class TimeMetricsCalculator:
    """Calculate time-based metrics and cohort analysis."""

    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id
        self.today = date.today()

    def calculate_all(self) -> dict:
        """Calculate all time-based metrics."""
        started_at = datetime.utcnow()

        results = {
            "day_of_week": self.calc_day_of_week_analysis(),
            "hour_of_day": self.calc_hour_of_day_analysis(),
            "monthly_trends": self.calc_monthly_trends(),
            "weekly_trends": self.calc_weekly_trends(),
            "seasonality": self.calc_seasonality(),
            "cohort_retention": self.calc_cohort_retention(),
            "cohort_revenue": self.calc_cohort_revenue(),
            "yoy_comparison": self.calc_year_over_year(),
            "peak_periods": self.calc_peak_periods(),
        }

        return {
            "status": "success",
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            "metrics_calculated": len(results),
        }

    def calc_day_of_week_analysis(self) -> List[Dict]:
        """Analyze sales by day of week."""
        query = text("""
            SELECT
                EXTRACT(DOW FROM transaction_date) as dow,
                TO_CHAR(transaction_date, 'Day') as day_name,
                COUNT(*) as transactions,
                COUNT(DISTINCT customer_id) as customers,
                SUM(amount) as revenue,
                AVG(amount) as avg_check,
                SUM(amount_before_discount - amount) as discount_amount
            FROM transactions
            WHERE tenant_id = :tenant_id
            GROUP BY EXTRACT(DOW FROM transaction_date), TO_CHAR(transaction_date, 'Day')
            ORDER BY EXTRACT(DOW FROM transaction_date)
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        day_names_ru = {
            0: "Воскресенье", 1: "Понедельник", 2: "Вторник",
            3: "Среда", 4: "Четверг", 5: "Пятница", 6: "Суббота"
        }

        return [
            {
                "day_of_week": int(row[0]),
                "day_name": day_names_ru.get(int(row[0]), row[1].strip()),
                "transactions": row[2],
                "customers": row[3],
                "revenue": float(row[4] or 0),
                "avg_check": float(row[5] or 0),
                "discount_amount": float(row[6] or 0),
            }
            for row in rows
        ]

    def calc_hour_of_day_analysis(self) -> List[Dict]:
        """Analyze sales by hour of day."""
        query = text("""
            SELECT
                EXTRACT(HOUR FROM transaction_date) as hour,
                COUNT(*) as transactions,
                COUNT(DISTINCT customer_id) as customers,
                SUM(amount) as revenue,
                AVG(amount) as avg_check
            FROM transactions
            WHERE tenant_id = :tenant_id
            GROUP BY EXTRACT(HOUR FROM transaction_date)
            ORDER BY EXTRACT(HOUR FROM transaction_date)
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "hour": int(row[0]),
                "hour_label": f"{int(row[0]):02d}:00",
                "transactions": row[1],
                "customers": row[2],
                "revenue": float(row[3] or 0),
                "avg_check": float(row[4] or 0),
            }
            for row in rows
        ]

    def calc_monthly_trends(self, months: int = 24) -> List[Dict]:
        """Calculate monthly sales trends."""
        query = text("""
            SELECT
                DATE_TRUNC('month', transaction_date) as month,
                COUNT(*) as transactions,
                COUNT(DISTINCT customer_id) as customers,
                COUNT(DISTINCT customer_id) FILTER (WHERE customer_id IN (
                    SELECT customer_id FROM transactions t2
                    WHERE t2.tenant_id = transactions.tenant_id
                    GROUP BY customer_id
                    HAVING MIN(transaction_date) >= DATE_TRUNC('month', transactions.transaction_date)
                       AND MIN(transaction_date) < DATE_TRUNC('month', transactions.transaction_date) + INTERVAL '1 month'
                )) as new_customers,
                SUM(amount) as revenue,
                AVG(amount) as avg_check,
                SUM(amount_before_discount - amount) as discount_amount
            FROM transactions
            WHERE tenant_id = :tenant_id
              AND transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL ':months months'
            GROUP BY DATE_TRUNC('month', transaction_date)
            ORDER BY month
        """.replace(":months", str(months)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        data = []
        prev_revenue = None
        for row in rows:
            revenue = float(row[4] or 0)
            mom_growth = None
            if prev_revenue is not None and prev_revenue > 0:
                mom_growth = round(100.0 * (revenue - prev_revenue) / prev_revenue, 2)

            data.append({
                "month": row[0].strftime("%Y-%m") if row[0] else None,
                "transactions": row[1],
                "customers": row[2],
                "new_customers": row[3] or 0,
                "revenue": revenue,
                "avg_check": float(row[5] or 0),
                "discount_amount": float(row[6] or 0),
                "mom_growth_pct": mom_growth,
            })
            prev_revenue = revenue

        return data

    def calc_weekly_trends(self, weeks: int = 52) -> List[Dict]:
        """Calculate weekly sales trends."""
        query = text("""
            SELECT
                DATE_TRUNC('week', transaction_date) as week,
                COUNT(*) as transactions,
                COUNT(DISTINCT customer_id) as customers,
                SUM(amount) as revenue,
                AVG(amount) as avg_check
            FROM transactions
            WHERE tenant_id = :tenant_id
              AND transaction_date >= CURRENT_DATE - INTERVAL ':weeks weeks'
            GROUP BY DATE_TRUNC('week', transaction_date)
            ORDER BY week
        """.replace(":weeks", str(weeks)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        data = []
        prev_revenue = None
        for row in rows:
            revenue = float(row[3] or 0)
            wow_growth = None
            if prev_revenue is not None and prev_revenue > 0:
                wow_growth = round(100.0 * (revenue - prev_revenue) / prev_revenue, 2)

            data.append({
                "week": row[0].strftime("%Y-W%W") if row[0] else None,
                "week_start": row[0].strftime("%Y-%m-%d") if row[0] else None,
                "transactions": row[1],
                "customers": row[2],
                "revenue": revenue,
                "avg_check": float(row[4] or 0),
                "wow_growth_pct": wow_growth,
            })
            prev_revenue = revenue

        return data

    def calc_seasonality(self) -> Dict:
        """Analyze seasonal patterns."""
        query = text("""
            SELECT
                EXTRACT(MONTH FROM transaction_date) as month_num,
                TO_CHAR(transaction_date, 'Month') as month_name,
                COUNT(*) as transactions,
                SUM(amount) as revenue,
                AVG(amount) as avg_check
            FROM transactions
            WHERE tenant_id = :tenant_id
            GROUP BY EXTRACT(MONTH FROM transaction_date), TO_CHAR(transaction_date, 'Month')
            ORDER BY EXTRACT(MONTH FROM transaction_date)
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        month_names_ru = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }

        monthly_data = []
        total_revenue = sum(float(r[3] or 0) for r in rows)
        avg_monthly_revenue = total_revenue / 12 if total_revenue > 0 else 0

        for row in rows:
            revenue = float(row[3] or 0)
            seasonality_index = (revenue / avg_monthly_revenue * 100) if avg_monthly_revenue > 0 else 100

            monthly_data.append({
                "month_num": int(row[0]),
                "month_name": month_names_ru.get(int(row[0]), row[1].strip()),
                "transactions": row[2],
                "revenue": revenue,
                "avg_check": float(row[4] or 0),
                "seasonality_index": round(seasonality_index, 1),
            })

        # Identify peak and low seasons
        sorted_by_revenue = sorted(monthly_data, key=lambda x: x["revenue"], reverse=True)
        peak_months = [m["month_name"] for m in sorted_by_revenue[:3]]
        low_months = [m["month_name"] for m in sorted_by_revenue[-3:]]

        return {
            "monthly_data": monthly_data,
            "peak_months": peak_months,
            "low_months": low_months,
            "seasonality_variation": round(
                (max(m["revenue"] for m in monthly_data) - min(m["revenue"] for m in monthly_data))
                / avg_monthly_revenue * 100, 1
            ) if avg_monthly_revenue > 0 and monthly_data else 0,
        }

    def calc_cohort_retention(self, cohorts: int = 12) -> List[Dict]:
        """Calculate customer cohort retention."""
        query = text("""
            WITH cohorts AS (
                SELECT
                    customer_id,
                    DATE_TRUNC('month', MIN(transaction_date)) as cohort_month
                FROM transactions
                WHERE tenant_id = :tenant_id AND customer_id IS NOT NULL
                GROUP BY customer_id
            ),
            activity AS (
                SELECT
                    c.customer_id,
                    c.cohort_month,
                    DATE_TRUNC('month', t.transaction_date) as activity_month,
                    EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', t.transaction_date), c.cohort_month)) * 12 +
                    EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', t.transaction_date), c.cohort_month)) as month_number
                FROM cohorts c
                JOIN transactions t ON c.customer_id = t.customer_id AND t.tenant_id = :tenant_id
                WHERE c.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL ':cohorts months'
            )
            SELECT
                cohort_month,
                month_number,
                COUNT(DISTINCT customer_id) as active_customers
            FROM activity
            WHERE month_number <= 12
            GROUP BY cohort_month, month_number
            ORDER BY cohort_month, month_number
        """.replace(":cohorts", str(cohorts)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        # Organize into cohort format
        cohorts_data = {}
        for row in rows:
            cohort = row[0].strftime("%Y-%m") if row[0] else "Unknown"
            month_num = int(row[1])
            customers = row[2]

            if cohort not in cohorts_data:
                cohorts_data[cohort] = {"cohort": cohort, "months": {}}

            cohorts_data[cohort]["months"][month_num] = customers

        # Calculate retention rates
        result_list = []
        for cohort, data in sorted(cohorts_data.items()):
            initial = data["months"].get(0, 0)
            if initial > 0:
                retention = {
                    "cohort": cohort,
                    "initial_customers": initial,
                    "retention": {
                        f"month_{m}": round(100.0 * data["months"].get(m, 0) / initial, 1)
                        for m in range(1, 13) if m in data["months"]
                    }
                }
                result_list.append(retention)

        return result_list

    def calc_cohort_revenue(self, cohorts: int = 12) -> List[Dict]:
        """Calculate revenue by customer cohort."""
        query = text("""
            WITH cohorts AS (
                SELECT
                    customer_id,
                    DATE_TRUNC('month', MIN(transaction_date)) as cohort_month
                FROM transactions
                WHERE tenant_id = :tenant_id AND customer_id IS NOT NULL
                GROUP BY customer_id
            )
            SELECT
                c.cohort_month,
                COUNT(DISTINCT c.customer_id) as cohort_size,
                SUM(t.amount) as total_revenue,
                AVG(t.amount) as avg_check,
                COUNT(t.id) as total_transactions,
                SUM(t.amount) / COUNT(DISTINCT c.customer_id) as revenue_per_customer
            FROM cohorts c
            JOIN transactions t ON c.customer_id = t.customer_id AND t.tenant_id = :tenant_id
            WHERE c.cohort_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL ':cohorts months'
            GROUP BY c.cohort_month
            ORDER BY c.cohort_month
        """.replace(":cohorts", str(cohorts)))

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "cohort": row[0].strftime("%Y-%m") if row[0] else None,
                "cohort_size": row[1],
                "total_revenue": float(row[2] or 0),
                "avg_check": float(row[3] or 0),
                "total_transactions": row[4],
                "revenue_per_customer": float(row[5] or 0),
                "orders_per_customer": round(row[4] / row[1], 2) if row[1] > 0 else 0,
            }
            for row in rows
        ]

    def calc_year_over_year(self) -> Dict:
        """Calculate year-over-year comparison."""
        query = text("""
            WITH yearly AS (
                SELECT
                    EXTRACT(YEAR FROM transaction_date) as year,
                    COUNT(*) as transactions,
                    COUNT(DISTINCT customer_id) as customers,
                    SUM(amount) as revenue,
                    AVG(amount) as avg_check
                FROM transactions
                WHERE tenant_id = :tenant_id
                GROUP BY EXTRACT(YEAR FROM transaction_date)
                ORDER BY year
            )
            SELECT
                y1.year as current_year,
                y1.transactions,
                y1.customers,
                y1.revenue,
                y1.avg_check,
                y2.revenue as prev_year_revenue,
                y2.transactions as prev_year_transactions,
                y2.customers as prev_year_customers
            FROM yearly y1
            LEFT JOIN yearly y2 ON y1.year = y2.year + 1
            ORDER BY y1.year
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        return [
            {
                "year": int(row[0]),
                "transactions": row[1],
                "customers": row[2],
                "revenue": float(row[3] or 0),
                "avg_check": float(row[4] or 0),
                "prev_year_revenue": float(row[5] or 0) if row[5] else None,
                "yoy_revenue_growth": round(
                    100.0 * (float(row[3] or 0) - float(row[5] or 0)) / float(row[5] or 1), 2
                ) if row[5] else None,
                "yoy_transactions_growth": round(
                    100.0 * (row[1] - (row[6] or 0)) / (row[6] or 1), 2
                ) if row[6] else None,
                "yoy_customers_growth": round(
                    100.0 * (row[2] - (row[7] or 0)) / (row[7] or 1), 2
                ) if row[7] else None,
            }
            for row in rows
        ]

    def calc_peak_periods(self) -> Dict:
        """Identify peak sales periods."""
        # Daily analysis for last 365 days
        query = text("""
            SELECT
                transaction_date::date as day,
                COUNT(*) as transactions,
                SUM(amount) as revenue
            FROM transactions
            WHERE tenant_id = :tenant_id
              AND transaction_date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY transaction_date::date
            ORDER BY revenue DESC
            LIMIT 20
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        top_days = [
            {
                "date": row[0].strftime("%Y-%m-%d") if row[0] else None,
                "day_of_week": row[0].strftime("%A") if row[0] else None,
                "transactions": row[1],
                "revenue": float(row[2] or 0),
            }
            for row in result.fetchall()
        ]

        # Hourly peaks
        hour_query = text("""
            SELECT
                EXTRACT(HOUR FROM transaction_date) as hour,
                SUM(amount) as revenue,
                COUNT(*) as transactions
            FROM transactions
            WHERE tenant_id = :tenant_id
            GROUP BY EXTRACT(HOUR FROM transaction_date)
            ORDER BY revenue DESC
            LIMIT 5
        """)

        hour_result = self.db.execute(hour_query, {"tenant_id": self.tenant_id})
        peak_hours = [
            {
                "hour": f"{int(row[0]):02d}:00",
                "revenue": float(row[1] or 0),
                "transactions": row[2],
            }
            for row in hour_result.fetchall()
        ]

        return {
            "top_revenue_days": top_days,
            "peak_hours": peak_hours,
        }
