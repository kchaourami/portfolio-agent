from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
import logging

import pandas as pd
import yfinance as yf

from app.config.settings import settings
from app.pipeline.providers.base import MarketDataProvider
from app.pipeline.providers.yahoo_provider import YahooFinanceProvider

logger = logging.getLogger(__name__)


Metadata = dict[str, str | None]


STATIC_METADATA: dict[str, Metadata] = {
    "CW8.PA": {
        "isin": "LU1681043599",
        "company_name": "Amundi MSCI World",
        "asset_type": "etf",
        "sector": None,
    },
    "PAEEM.PA": {
        "isin": "LU1681045370",
        "company_name": "Amundi MSCI Emerging Markets",
        "asset_type": "etf",
        "sector": None,
    },
    "^FCHI": {
        "isin": "",
        "company_name": "CAC 40",
        "asset_type": "index",
        "sector": None,
    },
}


class DataCollector:
    """Pipeline de collecte et normalisation des données de marché."""

    def __init__(self, provider: MarketDataProvider | None = None) -> None:
        self.provider = provider or YahooFinanceProvider()

    def collect_market_data(
        self,
        tickers: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        include_benchmark: bool = True,
    ) -> pd.DataFrame:
        """Collecte les prix et retourne le schéma commun du projet."""
        selected_tickers = self._build_ticker_list(
            tickers=tickers,
            include_benchmark=include_benchmark,
        )

        end_date = end or date.today()
        start_date = start or end_date - timedelta(days=365)

        raw_prices = self.provider.fetch_prices(
            tickers=selected_tickers,
            start=start_date,
            end=end_date,
        )

        if raw_prices.empty:
            logger.warning("Aucune donnée brute collectée par le pipeline Data")
            return self._empty_market_frame()

        market_data = self._normalize_prices(raw_prices)
        market_data = self._enrich_with_metadata(market_data)
        market_data = self._compute_daily_returns(market_data)

        logger.info(
            "Pipeline Data terminé | provider=%s | lignes=%s | tickers=%s",
            self.provider.name,
            len(market_data),
            market_data["ticker"].nunique(),
        )

        return market_data

    def collect_latest_market_data(
        self,
        tickers: list[str] | None = None,
        include_benchmark: bool = True,
    ) -> pd.DataFrame:
        """Collecte le dernier prix disponible et normalise la sortie."""
        selected_tickers = self._build_ticker_list(
            tickers=tickers,
            include_benchmark=include_benchmark,
        )

        raw_latest = self.provider.fetch_latest(selected_tickers)

        if raw_latest.empty:
            logger.warning("Aucun dernier prix collecté par le pipeline Data")
            return self._empty_market_frame()

        market_data = self._normalize_prices(raw_latest)
        market_data = self._enrich_with_metadata(market_data)

        # Intentionnel : sur une seule ligne par ticker, on ne peut pas calculer
        # un rendement journalier fiable sans le prix de clôture précédent.
        market_data["daily_return"] = pd.NA

        return market_data[
            [
                "date",
                "ticker",
                "isin",
                "company_name",
                "asset_type",
                "sector",
                "close_price",
                "volume",
                "daily_return",
                "source",
            ]
        ]

    def _normalize_prices(self, raw_prices: pd.DataFrame) -> pd.DataFrame:
        """Convertit les prix bruts vers le schéma commun minimal."""
        expected_columns = {"date", "ticker", "close", "volume", "source"}
        missing_columns = expected_columns - set(raw_prices.columns)

        if missing_columns:
            raise ValueError(f"Colonnes manquantes dans raw_prices: {missing_columns}")

        df = raw_prices.copy()

        before_drop = len(df)
        df = df.dropna(subset=["date", "ticker", "close"])
        dropped_rows = before_drop - len(df)

        if dropped_rows > 0:
            logger.warning(
                "%s lignes supprimées pendant la normalisation à cause de valeurs critiques manquantes",
                dropped_rows,
            )

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ticker"] = df["ticker"].astype(str)
        df["close_price"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = (
            pd.to_numeric(df["volume"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        df["source"] = df["source"].astype(str)

        before_close_drop = len(df)
        df = df.dropna(subset=["close_price"])
        close_dropped = before_close_drop - len(df)

        if close_dropped > 0:
            logger.warning(
                "%s lignes supprimées car close_price non numérique",
                close_dropped,
            )

        return df[
            [
                "date",
                "ticker",
                "close_price",
                "volume",
                "source",
            ]
        ].sort_values(["ticker", "date"]).reset_index(drop=True)

    def _enrich_with_metadata(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """Ajoute ISIN, nom société, type actif et secteur."""
        df = market_data.copy()
        tickers = sorted(df["ticker"].unique())

        metadata_by_ticker = self._fetch_metadata_parallel(tickers)

        df["isin"] = df["ticker"].map(
            lambda ticker: metadata_by_ticker.get(ticker, {}).get("isin", "")
        )
        df["company_name"] = df["ticker"].map(
            lambda ticker: metadata_by_ticker.get(ticker, {}).get("company_name", ticker)
        )
        df["asset_type"] = df["ticker"].map(
            lambda ticker: metadata_by_ticker.get(ticker, {}).get("asset_type", "action")
        )
        df["sector"] = df["ticker"].map(
            lambda ticker: metadata_by_ticker.get(ticker, {}).get("sector")
        )

        missing_isin = (
            df.loc[
                df["isin"].eq("") & ~df["asset_type"].eq("index"),
                "ticker",
            ]
            .unique()
            .tolist()
        )
        if missing_isin:
            logger.warning("ISIN manquant pour tickers=%s", missing_isin)

        missing_sector = (
            df.loc[
                df["sector"].isna() & df["asset_type"].eq("action"),
                "ticker",
            ]
            .unique()
            .tolist()
        )
        if missing_sector:
            logger.warning("Secteur manquant pour actions=%s", missing_sector)

        return df[
            [
                "date",
                "ticker",
                "isin",
                "company_name",
                "asset_type",
                "sector",
                "close_price",
                "volume",
                "source",
            ]
        ]

    def _compute_daily_returns(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """Calcule le rendement journalier par ticker."""
        df = market_data.copy()
        df = df.sort_values(["ticker", "date"])

        df["daily_return"] = (
            df.groupby("ticker")["close_price"]
            .pct_change()
        )

        first_rows = df.groupby("ticker").head(1).index
        if len(first_rows) > 0:
            logger.info(
                "daily_return vide pour la première date de chaque ticker, comportement attendu"
            )

        return df[
            [
                "date",
                "ticker",
                "isin",
                "company_name",
                "asset_type",
                "sector",
                "close_price",
                "volume",
                "daily_return",
                "source",
            ]
        ].reset_index(drop=True)

    def _fetch_metadata_parallel(self, tickers: list[str]) -> dict[str, Metadata]:
        """Récupère les métadonnées en parallèle pour limiter la latence."""
        metadata_by_ticker: dict[str, Metadata] = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._fetch_ticker_metadata, ticker): ticker
                for ticker in tickers
            }

            for future in as_completed(futures):
                ticker = futures[future]

                try:
                    metadata_by_ticker[ticker] = future.result()
                except Exception as exc:
                    logger.warning(
                        "Metadata fetch failed for %s: %s",
                        ticker,
                        exc,
                    )
                    metadata_by_ticker[ticker] = self._fallback_metadata(ticker)

        return metadata_by_ticker

    @staticmethod
    def _fetch_ticker_metadata(ticker: str) -> Metadata:
        """Récupère des métadonnées simples depuis le fallback statique ou yfinance."""
        if ticker in STATIC_METADATA:
            return STATIC_METADATA[ticker]

        try:
            info = yf.Ticker(ticker).get_info()
        except Exception as exc:
            logger.warning(
                "Impossible de récupérer les métadonnées yfinance pour %s: %s",
                ticker,
                exc,
            )
            return DataCollector._fallback_metadata(ticker)

        quote_type = str(info.get("quoteType", "")).lower()

        if ticker.startswith("^"):
            asset_type = "index"
        elif "etf" in quote_type:
            asset_type = "etf"
        else:
            asset_type = "action"

        return {
            "isin": str(info.get("isin") or ""),
            "company_name": str(
                info.get("longName")
                or info.get("shortName")
                or ticker
            ),
            "asset_type": asset_type,
            "sector": info.get("sector"),
        }

    @staticmethod
    def _fallback_metadata(ticker: str) -> Metadata:
        """Retourne des métadonnées minimales si yfinance échoue."""
        if ticker.startswith("^"):
            asset_type = "index"
        elif ticker.upper().endswith(".PA") and ticker.upper() in {"CW8.PA", "PAEEM.PA"}:
            asset_type = "etf"
        else:
            asset_type = "action"

        return {
            "isin": "",
            "company_name": ticker,
            "asset_type": asset_type,
            "sector": None,
        }

    @staticmethod
    def _build_ticker_list(
        tickers: list[str] | None,
        include_benchmark: bool,
    ) -> list[str]:
        """Construit la liste de tickers sans doublons."""
        selected_tickers = list(tickers) if tickers else settings.DEFAULT_TICKERS.copy()

        if include_benchmark:
            selected_tickers.append(settings.BENCHMARK_TICKER)

        return list(dict.fromkeys(selected_tickers))

    @staticmethod
    def _empty_market_frame() -> pd.DataFrame:
        """Retourne un DataFrame vide au schéma commun."""
        return pd.DataFrame(
            columns=[
                "date",
                "ticker",
                "isin",
                "company_name",
                "asset_type",
                "sector",
                "close_price",
                "volume",
                "daily_return",
                "source",
            ]
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    collector = DataCollector()
    df_market = collector.collect_market_data()

    print(df_market.head(20))
    print(df_market.tail(20))
    print(df_market.dtypes)