"""Data importer service for loading 1C data into database with bulk insert."""

import os
from datetime import datetime
from typing import Optional
from uuid import uuid4

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Tenant, ImportLog
from app.services.parser import Parser1C

settings = get_settings()


class DataImporter:
    """Service for importing 1C data into database using bulk insert."""

    BASE_IMPORT_PATH = "/mnt/u/BI"

    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = str(tenant_id)
        self.tenant = self._get_tenant()
        self.parser = Parser1C(import_path=self._get_import_path())
        self.stats = {}
        self.engine = db.get_bind()

    def _get_tenant(self) -> Tenant:
        tenant = self.db.query(Tenant).filter(Tenant.id == self.tenant_id).first()
        if not tenant:
            raise ValueError(f"Tenant not found: {self.tenant_id}")
        return tenant

    def _get_import_path(self) -> str:
        if self.tenant.import_path:
            return self.tenant.import_path
        return os.path.join(self.BASE_IMPORT_PATH, self.tenant.code)

    def import_all(self) -> dict:
        """Import all available 1C files using bulk insert."""
        self.stats = {
            "started_at": datetime.utcnow(),
            "files": {},
            "errors": [],
        }

        import_order = [
            ("ГруппыКлиентов.txt", self._import_customer_groups),
            ("Менеджеры.txt", self._import_managers),
            ("Сотрудники.txt", self._import_employees),
            ("ТорговыеТочки.txt", self._import_stores),
            ("Клиенты.txt", self._import_customers),
            ("Идентификаторы.txt", self._import_identifiers),
            ("Номенклатура.txt", self._import_products),
            ("Скидки.txt", self._import_discounts),
            ("ПродажаЗаголовок.txt", self._import_transactions),
            ("ПродажаСтроки.txt", self._import_transaction_items),
            ("НачисленныеБонусы.txt", self._import_bonus_accruals),
            ("СписанныеБонусы.txt", self._import_bonus_redemptions),
            ("ОстаткиНаБонусномСчете.txt", self._import_bonus_balances),
        ]

        for filename, import_func in import_order:
            try:
                print(f"Importing {filename}...")
                count = import_func(filename)
                self.stats["files"][filename] = {"status": "success", "records": count}
                self._log_import(filename, count, "success")
                print(f"  -> {count} records imported")
            except FileNotFoundError:
                self.stats["files"][filename] = {"status": "skipped", "reason": "file not found"}
                print(f"  -> skipped (file not found)")
            except Exception as e:
                self.db.rollback()  # Rollback on error
                self.stats["files"][filename] = {"status": "error", "error": str(e)}
                self.stats["errors"].append(f"{filename}: {str(e)}")
                self._log_import(filename, 0, "error", str(e))
                print(f"  -> error: {e}")

        self.stats["finished_at"] = datetime.utcnow()
        try:
            self.db.commit()
        except:
            self.db.rollback()
        return self.stats

    def _log_import(self, filename: str, count: int, status: str, error: str = None):
        try:
            log = ImportLog(
                tenant_id=self.tenant_id,
                file_name=filename,
                records_count=count,
                status=status,
                error_message=error,
                started_at=self.stats["started_at"],
                finished_at=datetime.utcnow(),
            )
            self.db.add(log)
            self.db.commit()
        except:
            self.db.rollback()

    def _parse_to_dataframe(self, filename: str) -> pd.DataFrame:
        """Parse file and return as DataFrame."""
        records = list(self.parser.parse_file(filename))
        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    def _execute_upsert(self, df: pd.DataFrame, table: str, columns: list,
                        conflict_cols: list, uuid_cols: list = None,
                        numeric_cols: list = None, bool_cols: list = None,
                        timestamp_cols: list = None, date_cols: list = None):
        """Execute upsert with proper type casting."""
        if df.empty:
            return 0

        uuid_cols = uuid_cols or []
        numeric_cols = numeric_cols or []
        bool_cols = bool_cols or []
        timestamp_cols = timestamp_cols or []
        date_cols = date_cols or []

        # Filter columns that exist in dataframe
        existing_cols = [c for c in columns if c in df.columns]
        df = df[existing_cols].copy()

        # Add tenant_id
        df['tenant_id'] = self.tenant_id
        existing_cols.append('tenant_id')
        uuid_cols.append('tenant_id')

        # Replace NaN/None with None for proper NULL handling
        df = df.where(pd.notnull(df), None)

        # Create temp table
        temp_table = f"temp_{table}_{uuid4().hex[:8]}"

        # Convert all to string for safe transfer
        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

        df.to_sql(temp_table, self.engine, if_exists='replace', index=False)

        # Build SELECT with casts
        select_parts = []
        for col in existing_cols:
            if col in uuid_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::uuid AS {col}')
            elif col in numeric_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::numeric AS {col}')
            elif col in bool_cols:
                select_parts.append(f'CASE WHEN {col} IN (\'True\', \'1\', \'true\') THEN true WHEN {col} IN (\'False\', \'0\', \'false\') THEN false ELSE NULL END AS {col}')
            elif col in timestamp_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::timestamp AS {col}')
            elif col in date_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::date AS {col}')
            else:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\') AS {col}')

        select_str = ', '.join(select_parts)
        col_str = ', '.join(existing_cols)
        conflict_str = ', '.join(conflict_cols)

        update_cols = [c for c in existing_cols if c not in conflict_cols]
        if update_cols:
            update_str = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            sql = f"""
                INSERT INTO {table} ({col_str})
                SELECT {select_str} FROM {temp_table}
                ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
            """
        else:
            sql = f"""
                INSERT INTO {table} ({col_str})
                SELECT {select_str} FROM {temp_table}
                ON CONFLICT ({conflict_str}) DO NOTHING
            """

        self.db.execute(text(sql))
        self.db.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        self.db.commit()
        return len(df)

    def _execute_insert(self, df: pd.DataFrame, table: str, columns: list,
                        uuid_cols: list = None, numeric_cols: list = None,
                        timestamp_cols: list = None):
        """Execute simple insert with type casting."""
        if df.empty:
            return 0

        uuid_cols = uuid_cols or []
        numeric_cols = numeric_cols or []
        timestamp_cols = timestamp_cols or []

        existing_cols = [c for c in columns if c in df.columns]
        df = df[existing_cols].copy()

        df['tenant_id'] = self.tenant_id
        existing_cols.append('tenant_id')
        uuid_cols.append('tenant_id')

        df = df.where(pd.notnull(df), None)

        temp_table = f"temp_{table}_{uuid4().hex[:8]}"

        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

        df.to_sql(temp_table, self.engine, if_exists='replace', index=False)

        select_parts = []
        for col in existing_cols:
            if col in uuid_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::uuid AS {col}')
            elif col in numeric_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::numeric AS {col}')
            elif col in timestamp_cols:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\')::timestamp AS {col}')
            else:
                select_parts.append(f'NULLIF(NULLIF({col}, \'None\'), \'nan\') AS {col}')

        select_str = ', '.join(select_parts)
        col_str = ', '.join(existing_cols)

        sql = f"INSERT INTO {table} ({col_str}) SELECT {select_str} FROM {temp_table}"
        self.db.execute(text(sql))
        self.db.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        self.db.commit()
        return len(df)

    # Import methods for each entity
    def _import_customer_groups(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'customer_groups', ['id', 'name'],
            ['id', 'tenant_id'], uuid_cols=['id']
        )

    def _import_managers(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'managers', ['id', 'name'],
            ['id', 'tenant_id'], uuid_cols=['id']
        )

    def _import_employees(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'employees', ['id', 'name'],
            ['id', 'tenant_id'], uuid_cols=['id']
        )

    def _import_stores(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'stores', ['id', 'name', 'manager_id'],
            ['id', 'tenant_id'], uuid_cols=['id', 'manager_id']
        )

    def _import_customers(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'customers',
            ['id', 'name', 'accumulated_amount', 'birth_date', 'is_active', 'group_id', 'last_updated'],
            ['id', 'tenant_id'],
            uuid_cols=['id', 'group_id'],
            numeric_cols=['accumulated_amount'],
            bool_cols=['is_active'],
            date_cols=['birth_date'],
            timestamp_cols=['last_updated']
        )

    def _import_identifiers(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['customer_id'].notna() & df['identifier'].notna()]

        # Delete existing
        self.db.execute(text(f"DELETE FROM customer_identifiers WHERE tenant_id = '{self.tenant_id}'"))
        self.db.commit()

        return self._execute_insert(
            df, 'customer_identifiers', ['customer_id', 'identifier'],
            uuid_cols=['customer_id']
        )

    def _import_products(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'products', ['id', 'name'],
            ['id', 'tenant_id'], uuid_cols=['id']
        )

    def _import_discounts(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]
        return self._execute_upsert(
            df, 'discounts', ['id', 'name'],
            ['id', 'tenant_id'], uuid_cols=['id']
        )

    def _import_transactions(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['id'].notna()]

        # Rename columns
        if 'full_date' in df.columns:
            df = df.rename(columns={'full_date': 'transaction_date'})
        if 'hour' in df.columns:
            df = df.rename(columns={'hour': 'transaction_hour'})
        if 'duration' in df.columns:
            df = df.rename(columns={'duration': 'duration_seconds'})

        return self._execute_upsert(
            df, 'transactions',
            ['id', 'customer_id', 'transaction_date', 'transaction_hour',
             'amount', 'amount_before_discount', 'discount_percent',
             'store_id', 'employee_id', 'duration_seconds'],
            ['id', 'tenant_id'],
            uuid_cols=['id', 'customer_id', 'store_id', 'employee_id'],
            numeric_cols=['amount', 'amount_before_discount', 'discount_percent', 'transaction_hour', 'duration_seconds'],
            timestamp_cols=['transaction_date']
        )

    def _import_transaction_items(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['transaction_id'].notna() & df['product_id'].notna()]

        # Delete existing
        self.db.execute(text(f"DELETE FROM transaction_items WHERE tenant_id = '{self.tenant_id}'"))
        self.db.commit()

        # Process in chunks
        chunk_size = 50000
        total = 0
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size].copy()
            count = self._execute_insert(
                chunk, 'transaction_items',
                ['transaction_id', 'product_id', 'quantity', 'price', 'price_before_discount', 'discount_id'],
                uuid_cols=['transaction_id', 'product_id', 'discount_id'],
                numeric_cols=['quantity', 'price', 'price_before_discount']
            )
            total += count
            print(f"    Inserted {total}/{len(df)} items...")
        return total

    def _import_bonus_accruals(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['customer_id'].notna()]
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'movement_date'})
        df['movement_type'] = 'accrual'

        return self._execute_insert(
            df, 'bonus_movements',
            ['customer_id', 'transaction_id', 'amount', 'movement_date', 'movement_type'],
            uuid_cols=['customer_id', 'transaction_id'],
            numeric_cols=['amount'],
            timestamp_cols=['movement_date']
        )

    def _import_bonus_redemptions(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['customer_id'].notna()]
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'movement_date'})
        df['movement_type'] = 'redemption'

        return self._execute_insert(
            df, 'bonus_movements',
            ['customer_id', 'transaction_id', 'amount', 'movement_date', 'movement_type'],
            uuid_cols=['customer_id', 'transaction_id'],
            numeric_cols=['amount'],
            timestamp_cols=['movement_date']
        )

    def _import_bonus_balances(self, filename: str) -> int:
        df = self._parse_to_dataframe(filename)
        df = df[df['customer_id'].notna()]
        return self._execute_upsert(
            df, 'bonus_balances', ['customer_id', 'balance'],
            ['tenant_id', 'customer_id'],
            uuid_cols=['customer_id'],
            numeric_cols=['balance']
        )


def create_tenant(db: Session, code: str, name: str, import_path: Optional[str] = None) -> Tenant:
    """Create a new tenant."""
    if not import_path:
        import_path = os.path.join(DataImporter.BASE_IMPORT_PATH, code)

    tenant = Tenant(
        id=uuid4(),
        code=code,
        name=name,
        import_path=import_path,
        is_active=True,
    )
    db.add(tenant)
    db.commit()
    db.refresh(tenant)
    return tenant
