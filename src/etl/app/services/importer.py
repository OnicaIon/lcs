"""Data importer service for loading 1C data into database."""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy.orm import Session
from sqlalchemy.dialects.mssql import insert as mssql_insert

from app.config import get_settings
from app.models import (
    Tenant, Customer, CustomerGroup, Store, Employee, Manager,
    Product, Discount, CustomerIdentifier, Transaction, TransactionItem,
    BonusMovement, BonusBalance, ImportLog
)
from app.services.parser import Parser1C

settings = get_settings()


class DataImporter:
    """Service for importing 1C data into database."""

    # Base directory for all tenant imports (Windows path)
    BASE_IMPORT_PATH = "/mnt/u/BI"  # U:\BI in WSL

    def __init__(self, db: Session, tenant_id: str):
        """Initialize importer.

        Args:
            db: Database session
            tenant_id: Tenant UUID
        """
        self.db = db
        self.tenant_id = tenant_id
        self.tenant = self._get_tenant()
        self.parser = Parser1C(import_path=self._get_import_path())
        self.stats = {}

    def _get_tenant(self) -> Tenant:
        """Get tenant by ID."""
        tenant = self.db.query(Tenant).filter(Tenant.id == self.tenant_id).first()
        if not tenant:
            raise ValueError(f"Tenant not found: {self.tenant_id}")
        return tenant

    def _get_import_path(self) -> str:
        """Get import path for tenant.

        Returns:
            Path to tenant's import directory
        """
        # If tenant has custom path, use it
        if self.tenant.import_path:
            return self.tenant.import_path

        # Otherwise, use base path + tenant code
        return os.path.join(self.BASE_IMPORT_PATH, self.tenant.code)

    def import_all(self) -> dict:
        """Import all available 1C files.

        Returns:
            Import statistics
        """
        self.stats = {
            "started_at": datetime.utcnow(),
            "files": {},
            "errors": [],
        }

        # Import order matters due to dependencies
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
                count = import_func(filename)
                self.stats["files"][filename] = {
                    "status": "success",
                    "records": count
                }
                self._log_import(filename, count, "success")
            except FileNotFoundError:
                self.stats["files"][filename] = {
                    "status": "skipped",
                    "reason": "file not found"
                }
            except Exception as e:
                self.stats["files"][filename] = {
                    "status": "error",
                    "error": str(e)
                }
                self.stats["errors"].append(f"{filename}: {str(e)}")
                self._log_import(filename, 0, "error", str(e))

        self.stats["finished_at"] = datetime.utcnow()
        self.db.commit()

        return self.stats

    def _log_import(
        self, filename: str, count: int, status: str, error: str = None
    ) -> None:
        """Log import operation."""
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

    def _import_customer_groups(self, filename: str) -> int:
        """Import customer groups."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            group = CustomerGroup(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
            )
            self.db.merge(group)
            count += 1

        self.db.flush()
        return count

    def _import_managers(self, filename: str) -> int:
        """Import managers."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            manager = Manager(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
            )
            self.db.merge(manager)
            count += 1

        self.db.flush()
        return count

    def _import_employees(self, filename: str) -> int:
        """Import employees."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            employee = Employee(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
            )
            self.db.merge(employee)
            count += 1

        self.db.flush()
        return count

    def _import_stores(self, filename: str) -> int:
        """Import stores (trading points)."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            store = Store(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
                manager_id=record.get("manager_id"),
            )
            self.db.merge(store)
            count += 1

        self.db.flush()
        return count

    def _import_customers(self, filename: str) -> int:
        """Import customers."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            customer = Customer(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
                accumulated_amount=record.get("accumulated_amount"),
                birth_date=record.get("birth_date"),
                is_active=record.get("is_active", True),
                group_id=record.get("group_id"),
                last_updated=record.get("last_updated"),
            )
            self.db.merge(customer)
            count += 1

        self.db.flush()
        return count

    def _import_identifiers(self, filename: str) -> int:
        """Import customer identifiers (loyalty cards)."""
        count = 0
        for record in self.parser.parse_file(filename):
            customer_id = record.get("customer_id")
            identifier = record.get("identifier")

            if not customer_id or not identifier:
                continue

            # Check if identifier already exists
            existing = self.db.query(CustomerIdentifier).filter(
                CustomerIdentifier.tenant_id == self.tenant_id,
                CustomerIdentifier.customer_id == customer_id,
                CustomerIdentifier.identifier == identifier,
            ).first()

            if not existing:
                ident = CustomerIdentifier(
                    tenant_id=self.tenant_id,
                    customer_id=customer_id,
                    identifier=identifier,
                )
                self.db.add(ident)
                count += 1

        self.db.flush()
        return count

    def _import_products(self, filename: str) -> int:
        """Import products (nomenclature)."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            product = Product(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
            )
            self.db.merge(product)
            count += 1

        self.db.flush()
        return count

    def _import_discounts(self, filename: str) -> int:
        """Import discounts."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            discount = Discount(
                id=record["id"],
                tenant_id=self.tenant_id,
                name=record.get("name"),
            )
            self.db.merge(discount)
            count += 1

        self.db.flush()
        return count

    def _import_transactions(self, filename: str) -> int:
        """Import transaction headers."""
        count = 0
        for record in self.parser.parse_file(filename):
            if not record.get("id"):
                continue

            transaction = Transaction(
                id=record["id"],
                tenant_id=self.tenant_id,
                customer_id=record.get("customer_id"),
                transaction_date=record.get("full_date"),
                transaction_hour=record.get("hour"),
                amount=record.get("amount"),
                amount_before_discount=record.get("amount_before_discount"),
                discount_percent=record.get("discount_percent"),
                store_id=record.get("store_id"),
                employee_id=record.get("employee_id"),
                duration_seconds=record.get("duration"),
            )
            self.db.merge(transaction)
            count += 1

            # Commit in batches for large files
            if count % 10000 == 0:
                self.db.flush()

        self.db.flush()
        return count

    def _import_transaction_items(self, filename: str) -> int:
        """Import transaction items (receipt lines)."""
        count = 0
        batch = []
        batch_size = 5000

        for record in self.parser.parse_file(filename):
            transaction_id = record.get("transaction_id")
            product_id = record.get("product_id")

            if not transaction_id or not product_id:
                continue

            item = TransactionItem(
                transaction_id=transaction_id,
                tenant_id=self.tenant_id,
                product_id=product_id,
                quantity=record.get("quantity"),
                price=record.get("price"),
                price_before_discount=record.get("price_before_discount"),
                discount_id=record.get("discount_id"),
            )
            self.db.add(item)
            count += 1

            if count % batch_size == 0:
                self.db.flush()

        self.db.flush()
        return count

    def _import_bonus_accruals(self, filename: str) -> int:
        """Import bonus accruals."""
        count = 0
        for record in self.parser.parse_file(filename):
            customer_id = record.get("customer_id")
            if not customer_id:
                continue

            movement = BonusMovement(
                tenant_id=self.tenant_id,
                customer_id=customer_id,
                transaction_id=record.get("transaction_id"),
                amount=record.get("amount"),
                movement_type="accrual",
                movement_date=record.get("date"),
            )
            self.db.add(movement)
            count += 1

        self.db.flush()
        return count

    def _import_bonus_redemptions(self, filename: str) -> int:
        """Import bonus redemptions."""
        count = 0
        for record in self.parser.parse_file(filename):
            customer_id = record.get("customer_id")
            if not customer_id:
                continue

            movement = BonusMovement(
                tenant_id=self.tenant_id,
                customer_id=customer_id,
                transaction_id=record.get("transaction_id"),
                amount=record.get("amount"),
                movement_type="redemption",
                movement_date=record.get("date"),
            )
            self.db.add(movement)
            count += 1

        self.db.flush()
        return count

    def _import_bonus_balances(self, filename: str) -> int:
        """Import bonus balances."""
        count = 0
        for record in self.parser.parse_file(filename):
            customer_id = record.get("customer_id")
            if not customer_id:
                continue

            balance = BonusBalance(
                tenant_id=self.tenant_id,
                customer_id=customer_id,
                balance=record.get("balance"),
            )
            self.db.merge(balance)
            count += 1

        self.db.flush()
        return count


def create_tenant(
    db: Session,
    code: str,
    name: str,
    import_path: Optional[str] = None
) -> Tenant:
    """Create a new tenant.

    Args:
        db: Database session
        code: Unique tenant code (used for import directory)
        name: Tenant display name
        import_path: Custom import path (optional)

    Returns:
        Created tenant
    """
    # Default import path: BASE_IMPORT_PATH / code
    if not import_path:
        import_path = os.path.join(DataImporter.BASE_IMPORT_PATH, code)

    tenant = Tenant(
        id=str(uuid4()),
        code=code,
        name=name,
        import_path=import_path,
        is_active=True,
    )
    db.add(tenant)
    db.commit()
    db.refresh(tenant)

    return tenant
