"""LCS ETL Service - Main FastAPI application."""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import router
from app.database import init_db
from app.config import get_settings

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    print("Starting LCS ETL Service...")
    try:
        init_db()
        print("Database connection established")
    except Exception as e:
        print(f"Warning: Could not connect to database: {e}")

    yield

    # Shutdown
    print("Shutting down LCS ETL Service...")


# Create FastAPI application
app = FastAPI(
    title="LCS - Customer Segmentation System",
    description="""
    ## Система сегментации клиентов

    API для управления клиентской аналитикой:

    - **Тенанты**: Управление базами 1С
    - **Импорт**: Загрузка данных из файлов 1С
    - **Метрики**: Расчёт 51 метрики клиентов
    - **Клиенты**: Просмотр клиентов и их метрик
    - **Дашборд**: Сводная статистика

    ### Метрики клиентов

    | Группа | Количество |
    |--------|------------|
    | Базовые транзакционные | 11 |
    | RFM | 5 |
    | Временные паттерны | 10 |
    | Жизненный цикл | 8 |
    | Ценность клиента | 11 |
    | Предиктивные | 6 |
    | Продуктовые предпочтения | 5 |
    | **Итого** | **51** |
    """,
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(router)


# Root endpoint
@app.get("/")
def root():
    """Root endpoint with service info."""
    return {
        "service": "LCS - Customer Segmentation System",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
