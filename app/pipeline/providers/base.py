from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
import pandas as pd

@dataclass
class RawPrice:
    date: date
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str

class MarketDataProvider(ABC):
    #Interface que tous les providers doivent implémenter.

    @abstractmethod
    def fetch_prices(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        #Retourne un DataFrame normalisé avec colonnes RawPrice.
        ...

    @abstractmethod
    def fetch_latest(self, tickers: list[str]) -> pd.DataFrame:
        #Prix de cloture du dernier jour disponible.
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        #Nom du provider — pour logging et traçabilité.
        ...