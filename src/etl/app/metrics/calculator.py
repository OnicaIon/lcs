"""Main metrics calculator - orchestrates all metric calculations."""

from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional
import statistics

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import CustomerMetrics, Customer, Transaction

settings = get_settings()


class MetricsCalculator:
    """Calculator for all 51 customer metrics."""

    # RFM segment mapping
    RFM_SEGMENTS = {
        (5, 5, 5): "Чемпионы",
        (5, 5, 4): "Чемпионы",
        (5, 4, 5): "Чемпионы",
        (4, 5, 5): "Лояльные",
        (5, 4, 4): "Лояльные",
        (4, 5, 4): "Лояльные",
        (4, 4, 5): "Лояльные",
        (4, 4, 4): "Лояльные",
        (5, 3, 3): "Потенциальные лояльные",
        (4, 3, 3): "Потенциальные лояльные",
        (3, 3, 3): "Требуют внимания",
        (3, 3, 4): "Требуют внимания",
        (3, 4, 3): "Требуют внимания",
        (2, 3, 3): "Засыпающие",
        (2, 2, 3): "Засыпающие",
        (2, 3, 2): "Засыпающие",
        (3, 2, 2): "Засыпающие",
        (2, 2, 2): "В зоне риска",
        (1, 2, 2): "В зоне риска",
        (2, 1, 2): "В зоне риска",
        (2, 2, 1): "В зоне риска",
        (1, 1, 2): "Уходящие",
        (1, 2, 1): "Уходящие",
        (2, 1, 1): "Уходящие",
        (1, 1, 1): "Потерянные",
        (5, 1, 1): "Новые",
        (5, 1, 2): "Новые",
        (4, 1, 1): "Новые",
        (4, 1, 2): "Новые",
    }

    def __init__(self, db: Session, tenant_id: str):
        """Initialize calculator.

        Args:
            db: Database session
            tenant_id: Tenant UUID
        """
        self.db = db
        self.tenant_id = tenant_id
        self.today = date.today()
        self.margin_percent = settings.margin_percent

    def calculate_all(self) -> dict:
        """Calculate all metrics for all customers.

        Returns:
            Statistics about calculation
        """
        started_at = datetime.utcnow()

        # Load transaction data into pandas for efficient calculations
        df = self._load_transaction_data()

        if df.empty:
            return {"status": "no_data", "customers": 0}

        # Group by customer
        customer_ids = df["customer_id"].unique()
        calculated = 0
        errors = 0

        for customer_id in customer_ids:
            if not customer_id:
                continue

            try:
                customer_df = df[df["customer_id"] == customer_id]
                metrics = self._calculate_customer_metrics(customer_id, customer_df, df)
                self._save_metrics(customer_id, metrics)
                calculated += 1

                # Commit in batches
                if calculated % 100 == 0:
                    self.db.commit()

            except Exception as e:
                errors += 1
                print(f"Error calculating metrics for {customer_id}: {e}")

        self.db.commit()

        return {
            "status": "success",
            "customers": calculated,
            "errors": errors,
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
        }

    def _load_transaction_data(self) -> pd.DataFrame:
        """Load all transactions for tenant into DataFrame."""
        query = text("""
            SELECT
                t.customer_id,
                t.id as transaction_id,
                t.transaction_date,
                t.amount,
                t.amount_before_discount,
                (SELECT SUM(ti.quantity) FROM transaction_items ti
                 WHERE ti.transaction_id = t.id AND ti.tenant_id = :tenant_id) as items_count
            FROM transactions t
            WHERE t.tenant_id = :tenant_id
              AND t.customer_id IS NOT NULL
            ORDER BY t.customer_id, t.transaction_date
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        rows = result.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "customer_id", "transaction_id", "transaction_date",
            "amount", "amount_before_discount", "items_count"
        ])

        # Convert types
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        df["amount_before_discount"] = pd.to_numeric(
            df["amount_before_discount"], errors="coerce"
        ).fillna(df["amount"])
        df["items_count"] = pd.to_numeric(df["items_count"], errors="coerce").fillna(0)

        return df

    def _calculate_customer_metrics(
        self,
        customer_id: str,
        customer_df: pd.DataFrame,
        all_df: pd.DataFrame
    ) -> dict:
        """Calculate all metrics for a single customer.

        Args:
            customer_id: Customer UUID
            customer_df: DataFrame with customer's transactions
            all_df: DataFrame with all transactions (for percentiles)

        Returns:
            Dictionary with all metrics
        """
        metrics = {}

        # 1. Basic transactional metrics (11)
        metrics.update(self._calc_basic_metrics(customer_df))

        # 2. RFM metrics (5)
        metrics.update(self._calc_rfm_metrics(customer_df, all_df))

        # 3. Temporal patterns (10)
        metrics.update(self._calc_temporal_metrics(customer_df))

        # 4. Lifecycle (8)
        metrics.update(self._calc_lifecycle_metrics(customer_df, metrics))

        # 5. Customer value (11)
        metrics.update(self._calc_value_metrics(customer_df, all_df, metrics))

        # 6. Predictive (6) - simplified without lifetimes for MVP
        metrics.update(self._calc_predictive_metrics(customer_df, metrics))

        # 7. Product preferences (5) - requires separate query
        metrics.update(self._calc_product_metrics(customer_id))

        return metrics

    def _calc_basic_metrics(self, df: pd.DataFrame) -> dict:
        """Calculate basic transactional metrics."""
        amounts = df["amount"].values
        items = df["items_count"].values

        return {
            "total_orders": len(df),
            "total_revenue": float(amounts.sum()),
            "total_items": float(items.sum()),
            "first_order_date": df["transaction_date"].min().date(),
            "last_order_date": df["transaction_date"].max().date(),
            "avg_check": float(amounts.mean()) if len(amounts) > 0 else 0,
            "avg_items_per_order": float(items.mean()) if len(items) > 0 else 0,
            "max_check": float(amounts.max()) if len(amounts) > 0 else 0,
            "min_check": float(amounts.min()) if len(amounts) > 0 else 0,
            "std_check": float(amounts.std()) if len(amounts) > 1 else 0,
            "avg_margin": float(amounts.sum() * self.margin_percent) / len(df) if len(df) > 0 else 0,
        }

    def _calc_rfm_metrics(self, df: pd.DataFrame, all_df: pd.DataFrame) -> dict:
        """Calculate RFM metrics."""
        last_order = df["transaction_date"].max()
        recency = (pd.Timestamp(self.today) - last_order).days

        # Frequency: orders per month over customer lifetime
        first_order = df["transaction_date"].min()
        months = max(1, (last_order - first_order).days / 30)
        frequency = len(df) / months

        # Monetary: total revenue
        monetary = float(df["amount"].sum())

        # Calculate RFM scores (1-5) using quintiles from all customers
        r_score = self._calc_score(recency, all_df, "recency", reverse=True)
        f_score = self._calc_score(frequency, all_df, "frequency")
        m_score = self._calc_score(monetary, all_df, "monetary")

        rfm_score = r_score * 100 + f_score * 10 + m_score
        rfm_segment = self._get_rfm_segment(r_score, f_score, m_score)

        return {
            "recency": recency,
            "frequency": frequency,
            "monetary": monetary,
            "rfm_score": rfm_score,
            "rfm_segment": rfm_segment,
        }

    def _calc_score(
        self,
        value: float,
        all_df: pd.DataFrame,
        metric: str,
        reverse: bool = False
    ) -> int:
        """Calculate quintile score (1-5) for a metric."""
        # For MVP, use simple thresholds
        # In production, calculate actual quintiles from all_df

        if metric == "recency":
            # Days since last purchase
            thresholds = [7, 30, 90, 180]
        elif metric == "frequency":
            # Orders per month
            thresholds = [0.1, 0.25, 0.5, 1.0]
        else:  # monetary
            # Total spend
            thresholds = [1000, 5000, 15000, 50000]

        score = 1
        for i, threshold in enumerate(thresholds):
            if value > threshold:
                score = i + 2 if not reverse else len(thresholds) - i
            else:
                if reverse:
                    score = len(thresholds) - i + 1
                break

        return min(5, max(1, score))

    def _get_rfm_segment(self, r: int, f: int, m: int) -> str:
        """Get RFM segment name from scores."""
        key = (r, f, m)
        if key in self.RFM_SEGMENTS:
            return self.RFM_SEGMENTS[key]

        # Default segments based on R score
        if r >= 4:
            if f >= 3:
                return "Лояльные"
            return "Новые"
        elif r >= 2:
            return "Засыпающие"
        else:
            return "Потерянные"

    def _calc_temporal_metrics(self, df: pd.DataFrame) -> dict:
        """Calculate temporal pattern metrics."""
        dates = df["transaction_date"].sort_values()
        first_order = dates.min()
        last_order = dates.max()

        # Customer age
        age_days = (pd.Timestamp(self.today) - first_order).days
        age_months = age_days // 30

        # Days between purchases
        if len(dates) > 1:
            diffs = dates.diff().dropna().dt.days.values
            avg_days = float(np.mean(diffs))
            median_days = float(np.median(diffs))
            std_days = float(np.std(diffs)) if len(diffs) > 1 else 0
        else:
            avg_days = age_days
            median_days = age_days
            std_days = 0

        # Expected next order
        expected_next = last_order + timedelta(days=avg_days)
        days_overdue = max(0, (pd.Timestamp(self.today) - expected_next).days)

        # Purchase regularity (0-1, based on coefficient of variation)
        regularity = 1 / (1 + std_days / avg_days) if avg_days > 0 else 0

        # Active months
        months_set = set(dates.dt.to_period("M"))
        active_months = len(months_set)
        total_months = max(1, age_months)
        activity_rate = active_months / total_months

        return {
            "customer_age_days": age_days,
            "customer_age_months": age_months,
            "avg_days_between": avg_days,
            "median_days_between": median_days,
            "std_days_between": std_days,
            "expected_next_order": expected_next.date(),
            "days_overdue": days_overdue,
            "purchase_regularity": regularity,
            "active_months": active_months,
            "activity_rate": activity_rate,
        }

    def _calc_lifecycle_metrics(self, df: pd.DataFrame, prev_metrics: dict) -> dict:
        """Calculate lifecycle metrics."""
        recency = prev_metrics.get("recency", 999)
        avg_days = prev_metrics.get("avg_days_between", 30)
        age_days = prev_metrics.get("customer_age_days", 0)

        # Sleep factor
        sleep_factor = recency / avg_days if avg_days > 0 else recency / 30

        # Lifecycle stage
        is_new = age_days <= settings.new_customer_days
        is_churned = sleep_factor >= settings.churned_threshold
        is_sleeping = sleep_factor >= settings.sleeping_threshold and not is_churned
        is_active = not is_new and not is_sleeping and not is_churned

        if is_new:
            stage = "Новый"
        elif is_active:
            stage = "Активный"
        elif is_sleeping:
            stage = "Засыпающий"
        elif is_churned:
            stage = "Потерянный"
        else:
            stage = "Неопределён"

        # Cohort (month of first purchase)
        first_order = prev_metrics.get("first_order_date")
        cohort = first_order.strftime("%Y-%m") if first_order else None

        # Sleep days
        sleep_days = recency - avg_days if recency > avg_days else 0

        return {
            "lifecycle_stage": stage,
            "sleep_days": int(sleep_days),
            "sleep_factor": sleep_factor,
            "is_new": is_new,
            "is_active": is_active,
            "is_sleeping": is_sleeping,
            "is_churned": is_churned,
            "cohort": cohort,
        }

    def _calc_value_metrics(
        self,
        df: pd.DataFrame,
        all_df: pd.DataFrame,
        prev_metrics: dict
    ) -> dict:
        """Calculate customer value metrics."""
        revenue = prev_metrics.get("total_revenue", 0)
        total_revenue = float(all_df["amount"].sum())

        # Historical CLV
        clv_historical = revenue

        # Predicted CLV (simplified - actual uses lifetimes library)
        frequency = prev_metrics.get("frequency", 0)
        avg_check = prev_metrics.get("avg_check", 0)
        prob_alive = 1 - min(1, prev_metrics.get("sleep_factor", 0) / 3)
        clv_predicted = avg_check * frequency * 12 * prob_alive  # Annual prediction

        # CLV segment
        if clv_predicted >= 50000:
            clv_segment = "VIP"
        elif clv_predicted >= 20000:
            clv_segment = "Высокий"
        elif clv_predicted >= 5000:
            clv_segment = "Средний"
        else:
            clv_segment = "Низкий"

        # ABC segment (by revenue)
        profit_contribution = revenue / total_revenue if total_revenue > 0 else 0

        # Calculate cumulative percentile
        customer_revenues = all_df.groupby("customer_id")["amount"].sum().sort_values(ascending=False)
        cumsum = customer_revenues.cumsum() / customer_revenues.sum()

        if revenue >= customer_revenues.quantile(0.8):
            abc_segment = "A"
        elif revenue >= customer_revenues.quantile(0.5):
            abc_segment = "B"
        else:
            abc_segment = "C"

        # XYZ segment (by purchase stability)
        regularity = prev_metrics.get("purchase_regularity", 0)
        if regularity >= 0.7:
            xyz_segment = "X"
        elif regularity >= 0.4:
            xyz_segment = "Y"
        else:
            xyz_segment = "Z"

        # Trends (compare last 3 months vs previous 3 months)
        trends = self._calc_trends(df)

        return {
            "clv_historical": clv_historical,
            "clv_predicted": clv_predicted,
            "clv_segment": clv_segment,
            "abc_segment": abc_segment,
            "xyz_segment": xyz_segment,
            "abc_xyz_segment": abc_segment + xyz_segment,
            "profit_contribution": profit_contribution,
            "cumulative_percentile": profit_contribution * 100,
            "revenue_trend": trends["revenue"],
            "check_trend": trends["check"],
            "frequency_trend": trends["frequency"],
        }

    def _calc_trends(self, df: pd.DataFrame) -> dict:
        """Calculate trends comparing recent vs previous period."""
        now = pd.Timestamp(self.today)
        recent_start = now - timedelta(days=90)
        prev_start = now - timedelta(days=180)

        recent = df[df["transaction_date"] >= recent_start]
        prev = df[(df["transaction_date"] >= prev_start) & (df["transaction_date"] < recent_start)]

        def trend(recent_val, prev_val):
            if prev_val == 0:
                return 1.0 if recent_val > 0 else 0.0
            return (recent_val - prev_val) / prev_val

        recent_revenue = float(recent["amount"].sum())
        prev_revenue = float(prev["amount"].sum())
        recent_check = float(recent["amount"].mean()) if len(recent) > 0 else 0
        prev_check = float(prev["amount"].mean()) if len(prev) > 0 else 0
        recent_freq = len(recent) / 3  # Orders per month
        prev_freq = len(prev) / 3

        return {
            "revenue": trend(recent_revenue, prev_revenue),
            "check": trend(recent_check, prev_check),
            "frequency": trend(recent_freq, prev_freq),
        }

    def _calc_predictive_metrics(self, df: pd.DataFrame, prev_metrics: dict) -> dict:
        """Calculate predictive metrics (simplified for MVP)."""
        sleep_factor = prev_metrics.get("sleep_factor", 0)
        frequency = prev_metrics.get("frequency", 0)
        avg_check = prev_metrics.get("avg_check", 0)

        # Probability alive (simplified)
        prob_alive = max(0, 1 - min(1, sleep_factor / 3))

        # Churn probability
        churn_probability = 1 - prob_alive

        # Churn risk segment
        if churn_probability >= 0.7:
            churn_risk = "Высокий"
        elif churn_probability >= 0.3:
            churn_risk = "Средний"
        else:
            churn_risk = "Низкий"

        # Predicted orders
        predicted_30d = frequency * prob_alive
        predicted_90d = frequency * 3 * prob_alive

        # Predicted revenue
        predicted_revenue_30d = predicted_30d * avg_check

        return {
            "prob_alive": prob_alive,
            "churn_probability": churn_probability,
            "churn_risk_segment": churn_risk,
            "predicted_orders_30d": predicted_30d,
            "predicted_orders_90d": predicted_90d,
            "predicted_revenue_30d": predicted_revenue_30d,
        }

    def _calc_product_metrics(self, customer_id: str) -> dict:
        """Calculate product preference metrics."""
        # Query for product purchases
        query = text("""
            SELECT
                p.name as product_name,
                p.category,
                SUM(ti.quantity) as total_qty,
                COUNT(*) as purchase_count
            FROM transaction_items ti
            JOIN transactions t ON ti.transaction_id = t.id AND ti.tenant_id = t.tenant_id
            JOIN products p ON ti.product_id = p.id AND ti.tenant_id = p.tenant_id
            WHERE t.customer_id = :customer_id
              AND t.tenant_id = :tenant_id
            GROUP BY p.id, p.name, p.category
            ORDER BY total_qty DESC
        """)

        result = self.db.execute(query, {
            "customer_id": customer_id,
            "tenant_id": self.tenant_id
        })
        rows = result.fetchall()

        if not rows:
            return {
                "favorite_category": None,
                "favorite_sku": None,
                "category_diversity": 0,
                "sku_diversity": 0,
                "cross_sell_potential": 0,
            }

        # Favorite SKU
        favorite_sku = rows[0][0] if rows else None

        # Categories
        categories = set()
        category_counts = {}
        for row in rows:
            cat = row[1]
            if cat:
                categories.add(cat)
                category_counts[cat] = category_counts.get(cat, 0) + row[2]

        # Favorite category
        favorite_category = max(category_counts, key=category_counts.get) if category_counts else None

        # Diversity
        sku_diversity = len(rows)
        category_diversity = len(categories)

        # Cross-sell potential (based on category diversity vs average)
        # Simplified: higher diversity = higher potential
        cross_sell_potential = min(1, category_diversity / 5)

        return {
            "favorite_category": favorite_category,
            "favorite_sku": favorite_sku,
            "category_diversity": category_diversity,
            "sku_diversity": sku_diversity,
            "cross_sell_potential": cross_sell_potential,
        }

    def _save_metrics(self, customer_id: str, metrics: dict) -> None:
        """Save calculated metrics to database."""
        # Check if metrics exist
        existing = self.db.query(CustomerMetrics).filter(
            CustomerMetrics.tenant_id == self.tenant_id,
            CustomerMetrics.customer_id == customer_id
        ).first()

        if existing:
            for key, value in metrics.items():
                setattr(existing, key, value)
            existing.calculated_at = datetime.utcnow()
        else:
            customer_metrics = CustomerMetrics(
                tenant_id=self.tenant_id,
                customer_id=customer_id,
                calculated_at=datetime.utcnow(),
                **metrics
            )
            self.db.add(customer_metrics)
