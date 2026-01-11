"""Application configuration."""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_url: str = "mssql+pyodbc://sa:YourStrong@Passw0rd@localhost:1433/lcs?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

    # Ollama LLM
    ollama_url: str = "http://localhost:11434"
    llm_model: str = "qwen2.5:32b-instruct-q4_K_M"

    # Import settings
    import_path: str = "/data/import"
    file_encoding: str = "windows-1251"

    # Business logic
    margin_percent: float = 0.20  # Default margin for cost calculation

    # RFM settings
    rfm_recency_days: int = 365  # Period for RFM calculation
    new_customer_days: int = 30  # Customer is "new" if first order within N days
    sleeping_threshold: float = 1.5  # Sleep factor threshold
    churned_threshold: float = 3.0  # Churn factor threshold

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
