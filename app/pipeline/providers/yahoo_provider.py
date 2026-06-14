from datetime import date, timedelta
import logging

import pandas as pd
import yfinance as yf

from app.pipeline.providers.base import MarketDataProvider

logger = logging.getLogger(__name__)


class YahooFinanceProvider(MarketDataProvider):
    """Provider yfinance pour les données de marché MVP."""

    @property
    def name(self) -> str:
        """Nom du provider."""
        return "yfinance"

    def fetch_prices(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """Récupère les prix historiques OHLCV depuis yfinance."""
        if not tickers:
            logger.warning("Aucun ticker fourni à YahooFinanceProvider.fetch_prices")
            return self._empty_prices_frame()

        logger.info(
            "Collecte yfinance démarrée | tickers=%s | start=%s | end=%s",
            tickers,
            start,
            end,
        )

        try:
            data = yf.download(
                tickers=tickers,
                start=start.isoformat(),
                end=end.isoformat(),
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            logger.exception("Erreur pendant l'appel yfinance: %s", exc)
            raise

        if data.empty:
            logger.warning(
                "yfinance a retourné un DataFrame vide | tickers=%s | start=%s | end=%s",
                tickers,
                start,
                end,
            )
            return self._empty_prices_frame()

        frames: list[pd.DataFrame] = []

        for ticker in tickers:
            ticker_frame = self._extract_ticker_frame(
                data=data,
                ticker=ticker,
                tickers_count=len(tickers),
            )

            if ticker_frame.empty:
                logger.warning("Aucune donnée récupérée pour le ticker %s", ticker)
                continue

            ticker_frame = ticker_frame.reset_index()
            ticker_frame.columns = [
                str(col).lower().replace(" ", "_")
                for col in ticker_frame.columns
            ]

            date_col = "date" if "date" in ticker_frame.columns else ticker_frame.columns[0]

            required_columns = ["open", "high", "low", "close", "volume"]
            missing_columns = [
                col for col in required_columns
                if col not in ticker_frame.columns
            ]

            if missing_columns:
                logger.warning(
                    "Colonnes manquantes pour %s dans yfinance: %s",
                    ticker,
                    missing_columns,
                )
                continue

            normalized = ticker_frame[
                [date_col, "open", "high", "low", "close", "volume"]
            ].copy()

            normalized = normalized.rename(columns={date_col: "date"})
            normalized["ticker"] = ticker
            normalized["source"] = self.name

            before_drop = len(normalized)
            normalized = normalized.dropna(subset=["date", "close"])
            dropped_rows = before_drop - len(normalized)

            if dropped_rows > 0:
                logger.warning(
                    "%s lignes supprimées pour %s à cause de date/close manquant",
                    dropped_rows,
                    ticker,
                )

            normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
            normalized["volume"] = (
                pd.to_numeric(normalized["volume"], errors="coerce")
                .fillna(0)
                .astype("int64")
            )

            frames.append(
                normalized[
                    [
                        "date",
                        "ticker",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "source",
                    ]
                ]
            )

        if not frames:
            logger.warning("Aucune donnée exploitable après normalisation yfinance")
            return self._empty_prices_frame()

        result = pd.concat(frames, ignore_index=True)
        result = result.sort_values(["ticker", "date"]).reset_index(drop=True)

        logger.info("Collecte yfinance terminée | lignes=%s", len(result))
        return result

    def fetch_latest(self, tickers: list[str]) -> pd.DataFrame:
        """Récupère le dernier prix disponible pour chaque ticker."""
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=10)

        prices = self.fetch_prices(
            tickers=tickers,
            start=start,
            end=end,
        )

        if prices.empty:
            logger.warning("Aucun dernier prix disponible pour tickers=%s", tickers)
            return prices

        latest = (
            prices.sort_values(["ticker", "date"])
            .groupby("ticker", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )

        logger.info("Derniers prix récupérés | lignes=%s", len(latest))
        return latest

    @staticmethod
    def _extract_ticker_frame(
        data: pd.DataFrame,
        ticker: str,
        tickers_count: int,
    ) -> pd.DataFrame:
        """Extrait les colonnes d'un ticker depuis la réponse yfinance."""
        if tickers_count == 1:
            return data.copy()

        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.get_level_values(0):
                return pd.DataFrame()
            return data[ticker].copy()

        return data.copy()

    @staticmethod
    def _empty_prices_frame() -> pd.DataFrame:
        """Retourne un DataFrame vide avec le schéma brut attendu."""
        return pd.DataFrame(
            columns=[
                "date",
                "ticker",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source",
            ]
        )