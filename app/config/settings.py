from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    # Chemins
    DB_PATH: str = "data/portfolio.duckdb"
    DBT_PROJECT_DIR: str = "dbt_project"

    # API keys 
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-2.5-flash"

    # Portfolio 
    DEFAULT_TICKERS: list[str] = [
        "BNP.PA", "MC.PA", "SAN.PA", "AIR.PA", "OR.PA",
        "CW8.PA", "PAEEM.PA"  
    ]
    BENCHMARK_TICKER: str = "^FCHI"  # CAC 40

    # Alertes — seuils par défaut
    ALERT_PRICE_DROP_PCT: float = -0.03    
    ALERT_DRAWDOWN_PCT: float = -0.05     
    ALERT_VOLUME_RATIO: float = 2.0        
    ALERT_UNDERPERF_PCT: float = -0.03     
    ALERT_VOLATILITY_PCT: float = 0.02              
    ALERT_SECTOR_CONCENTRATION_PCT: float = 0.30    

    class Config:
        env_file = ".env"

settings = Settings()