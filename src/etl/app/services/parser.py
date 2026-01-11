"""Parser for 1C export files."""

import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Generator, Optional, Any
from uuid import UUID

from app.config import get_settings

settings = get_settings()


class Parser1C:
    """Parser for 1C export files (CSV format, windows-1251 encoding)."""

    # File name to column mapping based on 1C export structure
    FILE_SCHEMAS = {
        "Менеджеры.txt": ["id", "name"],
        "Клиенты.txt": [
            "id", "name", "accumulated_amount", "accumulated_amount_dup",
            "birth_date", "is_active", "last_updated", "group_id"
        ],
        "ГруппыКлиентов.txt": ["id", "name"],
        "Сотрудники.txt": ["id", "name"],
        "ТорговыеТочки.txt": ["id", "name", "manager_id"],
        "Номенклатура.txt": ["id", "name"],
        "Скидки.txt": ["id", "name"],
        "Идентификаторы.txt": ["customer_id", "identifier"],
        "ПродажаЗаголовок.txt": [
            "full_date", "date", "hour", "store_id", "amount",
            "amount_before_discount", "employee_id", "customer_id",
            "discount_percent", "id", "duration"
        ],
        "ПродажаСтроки.txt": [
            "transaction_id", "product_id", "quantity",
            "price_before_discount", "price", "discount_id"
        ],
        "НачисленныеБонусы.txt": [
            "customer_id", "transaction_id", "amount", "date"
        ],
        "СписанныеБонусы.txt": [
            "customer_id", "transaction_id", "amount", "date"
        ],
        "ОстаткиНаБонусномСчете.txt": ["customer_id", "balance"],
    }

    def __init__(self, import_path: str = None, encoding: str = None):
        """Initialize parser.

        Args:
            import_path: Path to directory with 1C export files
            encoding: File encoding (default: windows-1251)
        """
        self.import_path = Path(import_path or settings.import_path)
        self.encoding = encoding or settings.file_encoding

    def parse_file(
        self, filename: str
    ) -> Generator[dict[str, Any], None, None]:
        """Parse a single 1C export file.

        Args:
            filename: Name of the file to parse

        Yields:
            Dictionaries with parsed data
        """
        filepath = self.import_path / filename

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        schema = self.FILE_SCHEMAS.get(filename)
        if not schema:
            raise ValueError(f"Unknown file format: {filename}")

        with open(filepath, "r", encoding=self.encoding) as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    record = self._parse_line(line, schema, filename)
                    if record:
                        yield record
                except Exception as e:
                    print(f"Error parsing {filename}:{line_num}: {e}")
                    continue

    def _parse_line(
        self, line: str, schema: list[str], filename: str
    ) -> Optional[dict[str, Any]]:
        """Parse a single line according to schema.

        Args:
            line: Raw line from file
            schema: List of column names
            filename: Name of file (for context)

        Returns:
            Parsed dictionary or None if invalid
        """
        # Split by semicolon, remove trailing empty values
        parts = line.rstrip(";").split(";")

        # Ensure we have enough parts
        if len(parts) < len(schema):
            parts.extend([""] * (len(schema) - len(parts)))

        record = {}
        for i, field_name in enumerate(schema):
            value = parts[i].strip() if i < len(parts) else ""

            # Skip duplicate fields
            if field_name.endswith("_dup"):
                continue

            # Parse value based on field type
            record[field_name] = self._parse_value(value, field_name)

        return record

    def _parse_value(self, value: str, field_name: str) -> Any:
        """Parse value based on field name conventions.

        Args:
            value: Raw string value
            field_name: Name of the field

        Returns:
            Parsed value
        """
        if not value or value.lower() in ("null", "неопределено"):
            return None

        # UUID fields
        if field_name == "id" or field_name.endswith("_id"):
            return self._parse_uuid(value)

        # Date fields
        if "date" in field_name.lower():
            return self._parse_datetime(value)

        # Numeric fields
        if field_name in (
            "amount", "balance", "price", "quantity",
            "accumulated_amount", "discount_percent",
            "amount_before_discount", "price_before_discount"
        ):
            return self._parse_decimal(value)

        # Integer fields
        if field_name in ("hour", "duration"):
            return self._parse_int(value)

        # Boolean fields
        if field_name in ("is_active",):
            return self._parse_bool(value)

        # String fields
        return value

    def _parse_uuid(self, value: str) -> Optional[str]:
        """Parse UUID value."""
        try:
            # Validate UUID format
            UUID(value)
            return value
        except (ValueError, TypeError):
            return None

    def _parse_datetime(self, value: str) -> Optional[datetime]:
        """Parse datetime value from 1C format."""
        if not value:
            return None

        # Try different 1C date formats
        formats = [
            "%d.%m.%Y %H:%M:%S",  # Full datetime
            "%d.%m.%Y",           # Date only
            "%Y%m%d%H%M%S",       # Compact format
            "%Y-%m-%d %H:%M:%S",  # ISO format
            "%Y-%m-%d",           # ISO date only
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

        return None

    def _parse_decimal(self, value: str) -> Optional[Decimal]:
        """Parse decimal value."""
        try:
            # Handle Russian decimal separator
            value = value.replace(",", ".").replace(" ", "")
            return Decimal(value)
        except (InvalidOperation, ValueError):
            return None

    def _parse_int(self, value: str) -> Optional[int]:
        """Parse integer value."""
        try:
            return int(float(value.replace(",", ".").replace(" ", "")))
        except (ValueError, TypeError):
            return None

    def _parse_bool(self, value: str) -> bool:
        """Parse boolean value from 1C."""
        return value.lower() in ("да", "истина", "true", "1", "yes")

    def list_available_files(self) -> list[str]:
        """List available 1C export files in import directory.

        Returns:
            List of file names that can be parsed
        """
        if not self.import_path.exists():
            return []

        available = []
        for filename in self.FILE_SCHEMAS.keys():
            if (self.import_path / filename).exists():
                available.append(filename)

        return available

    def get_file_info(self, filename: str) -> dict:
        """Get information about a file.

        Args:
            filename: Name of file

        Returns:
            Dictionary with file info
        """
        filepath = self.import_path / filename

        if not filepath.exists():
            return {"exists": False, "filename": filename}

        stat = filepath.stat()
        line_count = sum(1 for _ in open(filepath, "r", encoding=self.encoding))

        return {
            "exists": True,
            "filename": filename,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime),
            "line_count": line_count,
            "schema": self.FILE_SCHEMAS.get(filename, []),
        }
