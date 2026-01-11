"""LLM-based product classifier using Ollama."""

import json
import re
from datetime import datetime
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings

settings = get_settings()


class ProductClassifier:
    """Classify products into categories using LLM."""

    CATEGORIES = [
        "Обувь спортивная",
        "Обувь повседневная",
        "Одежда спортивная",
        "Одежда повседневная",
        "Сумки и рюкзаки",
        "Тренажёры",
        "Спортинвентарь",
        "Мячи",
        "Велосипеды",
        "Ролики и скейты",
        "Спортивное питание",
        "Защита и экипировка",
        "Аксессуары",
        "Другое",
    ]

    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id
        self.ollama_url = settings.ollama_url
        self.model = settings.llm_model
        self.batch_size = 10  # Products per LLM request (optimized for small models)

    def classify_all(self, force: bool = False) -> dict:
        """Classify all unclassified products.

        Args:
            force: If True, reclassify all products

        Returns:
            Statistics about classification
        """
        started_at = datetime.utcnow()

        # Get products to classify
        if force:
            query = text("""
                SELECT id, name FROM products
                WHERE tenant_id = :tenant_id
                ORDER BY name
            """)
        else:
            query = text("""
                SELECT id, name FROM products
                WHERE tenant_id = :tenant_id
                  AND (category IS NULL OR category = '')
                ORDER BY name
            """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        products = result.fetchall()

        if not products:
            return {"status": "no_products", "classified": 0}

        total = len(products)
        classified = 0
        errors = 0

        print(f"Classifying {total} products...")

        # Process in batches
        for i in range(0, total, self.batch_size):
            batch = products[i:i + self.batch_size]

            try:
                classifications = self._classify_batch(batch)
                self._save_classifications(classifications)
                self.db.commit()  # Commit each batch for visibility
                classified += len(classifications)
                print(f"  Processed {min(i + self.batch_size, total)}/{total}")
            except Exception as e:
                self.db.rollback()
                errors += len(batch)
                print(f"  Error classifying batch: {e}")

        return {
            "status": "success",
            "total": total,
            "classified": classified,
            "errors": errors,
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
        }

    def _classify_batch(self, products: list) -> list:
        """Classify a batch of products using LLM."""
        # Build product list for prompt
        product_list = "\n".join(
            f"{i+1}. {p[1]}" for i, p in enumerate(products)
        )

        # Category list for prompt
        cats = ", ".join(self.CATEGORIES)

        prompt = f"""Classify sports store products. Reply ONLY JSON array.
Categories: {cats}

Products:
{product_list}

JSON: [{{"i":1,"c":"Обувь спортивная"}},{{"i":2,"c":"Спортинвентарь"}}]"""

        # Call Ollama
        response = self._call_ollama(prompt)

        # Parse response
        classifications = self._parse_response(response, products)

        return classifications

    def _call_ollama(self, prompt: str) -> str:
        """Call Ollama API."""
        url = f"{self.ollama_url}/api/generate"

        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 2000,
            }
        }

        with httpx.Client(timeout=120.0) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            return data.get("response", "")

    def _parse_response(self, response: str, products: list) -> list:
        """Parse LLM response into classifications."""
        classifications = []

        # Try to extract JSON from response
        try:
            # Find JSON array in response
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
            else:
                data = json.loads(response)

            for item in data:
                # Support both short (i/c) and long (index/category) keys
                idx = item.get("i", item.get("index", 0)) - 1
                category = item.get("c", item.get("category", "Другое"))

                # Validate category
                if category not in self.CATEGORIES:
                    category = self._find_closest_category(category)

                if 0 <= idx < len(products):
                    classifications.append({
                        "id": str(products[idx][0]),
                        "name": products[idx][1],
                        "category": category,
                        "confidence": 0.85,
                    })

        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"  Parse error: {e}")
            # Fallback: assign "Другое" to all
            for p in products:
                classifications.append({
                    "id": str(p[0]),
                    "name": p[1],
                    "category": "Другое",
                    "confidence": 0.5,
                })

        return classifications

    def _find_closest_category(self, category: str) -> str:
        """Find closest matching category."""
        category_lower = category.lower()

        for cat in self.CATEGORIES:
            if cat.lower() in category_lower or category_lower in cat.lower():
                return cat

        # Common mappings to categories
        mappings = {
            # Обувь спортивная
            "adidas": "Обувь спортивная", "nike": "Обувь спортивная", "sneaker": "Обувь спортивная",
            "кроссов": "Обувь спортивная", "running": "Обувь спортивная", "training": "Обувь спортивная",
            # Обувь повседневная
            "boot": "Обувь повседневная", "ghete": "Обувь повседневная", "papuc": "Обувь повседневная",
            "shoe": "Обувь повседневная", "sandal": "Обувь повседневная",
            # Одежда спортивная
            "tricou": "Одежда спортивная", "t-shirt": "Одежда спортивная", "short": "Одежда спортивная",
            "legging": "Одежда спортивная", "hoodie": "Одежда спортивная", "hanorac": "Одежда спортивная",
            "bustiera": "Одежда спортивная", "swim": "Одежда спортивная", "costume": "Одежда спортивная",
            # Одежда повседневная
            "jacket": "Одежда повседневная", "jacheta": "Одежда повседневная", "pant": "Одежда повседневная",
            "sock": "Одежда повседневная", "underwear": "Одежда повседневная",
            # Сумки и рюкзаки
            "bag": "Сумки и рюкзаки", "rucsac": "Сумки и рюкзаки", "backpack": "Сумки и рюкзаки",
            "geanta": "Сумки и рюкзаки",
            # Тренажёры
            "stepper": "Тренажёры", "treadmill": "Тренажёры", "trainer": "Тренажёры",
            "bench": "Тренажёры", "banca": "Тренажёры",
            # Спортинвентарь
            "dumbbell": "Спортинвентарь", "kettlebell": "Спортинвентарь", "rope": "Спортинвентарь",
            "yoga": "Спортинвентарь", "saltea": "Спортинвентарь", "racket": "Спортинвентарь",
            "boxing": "Спортинвентарь", "glove": "Спортинвентарь", "band": "Спортинвентарь",
            # Мячи
            "ball": "Мячи", "minge": "Мячи", "football": "Мячи", "basketball": "Мячи",
            # Велосипеды
            "bike": "Велосипеды", "biciclet": "Велосипеды", "cycling": "Велосипеды",
            # Ролики и скейты
            "skate": "Ролики и скейты", "patine": "Ролики и скейты", "roller": "Ролики и скейты",
            "skateboard": "Ролики и скейты", "scuter": "Ролики и скейты",
            # Спортивное питание
            "protein": "Спортивное питание", "bcaa": "Спортивное питание", "creatine": "Спортивное питание",
            "vitamin": "Спортивное питание", "carnitine": "Спортивное питание", "supplement": "Спортивное питание",
            "oshee": "Спортивное питание", "caffeine": "Спортивное питание", "spirulina": "Спортивное питание",
            # Защита и экипировка
            "helmet": "Защита и экипировка", "casca": "Защита и экипировка", "protec": "Защита и экипировка",
            "goggle": "Защита и экипировка", "ochelari": "Защита и экипировка",
            # Аксессуары
            "cap": "Аксессуары", "hat": "Аксессуары", "sapca": "Аксессуары",
            "headband": "Аксессуары", "belt": "Аксессуары", "watch": "Аксессуары",
            "bottle": "Аксессуары", "towel": "Аксессуары",
        }

        for key, value in mappings.items():
            if key in category_lower:
                return value

        return "Другое"

    def _save_classifications(self, classifications: list) -> None:
        """Save classifications to database."""
        for item in classifications:
            query = text("""
                UPDATE products
                SET category = :category,
                    category_confidence = :confidence,
                    classified_at = :classified_at
                WHERE id = :id AND tenant_id = :tenant_id
            """)

            self.db.execute(query, {
                "id": item["id"],
                "tenant_id": self.tenant_id,
                "category": item["category"],
                "confidence": item["confidence"],
                "classified_at": datetime.utcnow(),
            })

    def get_category_stats(self) -> list:
        """Get statistics by category."""
        query = text("""
            SELECT
                COALESCE(category, 'Не классифицировано') as category,
                COUNT(*) as count
            FROM products
            WHERE tenant_id = :tenant_id
            GROUP BY category
            ORDER BY count DESC
        """)

        result = self.db.execute(query, {"tenant_id": self.tenant_id})
        return [{"category": r[0], "count": r[1]} for r in result.fetchall()]
