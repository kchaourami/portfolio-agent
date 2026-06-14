from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    # Chemins
    DB_PATH: str = "data/portfolio.duckdb"
    DBT_PROJECT_DIR: str = "dbt_project"

    # API keys (optionnelles pour le MVP)
    INSEE_API_KEY: str = ""
    FMP_API_KEY: str = ""
    ANTHROPIC_API_KEY: str = ""

    # Portfolio (portefeuille par défaut pour les tests)
    DEFAULT_TICKERS: list[str] = [
        "BNP.PA", "MC.PA", "SAN.PA", "AIR.PA", "OR.PA",
        "CW8.PA", "PAEEM.PA"  # ETF PEA
    ]
    BENCHMARK_TICKER: str = "^FCHI"  # CAC 40

    # Alertes — seuils par défaut
    ALERT_PRICE_DROP_PCT: float = -0.03    # -3%
    ALERT_DRAWDOWN_PCT: float = -0.05      # -5%
    ALERT_VOLUME_RATIO: float = 2.0        # 2x volume moyen
    ALERT_UNDERPERF_PCT: float = -0.03     # -3% vs CAC 40

    class Config:
        env_file = ".env"

settings = Settings()