"""
Role :
- Initialiser le schéma des tables (raw + marts)
- Écrire les données collectées (prices, macro)
- Lire les données pour les agents (risk, analyste)
- stockage

Tables gérées :
  raw_prices : sortie de DataCollector
  raw_macro  : sortie de MacroCollector 
  portfolio  : positions de l'utilisateur
  alerts     : alertes générées par l'Agent Risk
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd

from app.config.settings import settings

logger = logging.getLogger(__name__)

# Schéma SQL des tables

DDL_RAW_PRICES = """
CREATE TABLE IF NOT EXISTS raw_prices (
    date         DATE        NOT NULL,
    ticker       VARCHAR     NOT NULL,
    isin         VARCHAR,
    company_name VARCHAR,
    asset_type   VARCHAR,
    sector       VARCHAR,
    close_price  DOUBLE      NOT NULL,
    volume       BIGINT,
    daily_return DOUBLE,
    source       VARCHAR     NOT NULL,
    inserted_at  TIMESTAMP   DEFAULT current_timestamp,
    PRIMARY KEY (date, ticker)
)
"""

DDL_RAW_MACRO = """
CREATE TABLE IF NOT EXISTS raw_macro (
    date        DATE        NOT NULL,
    series_key  VARCHAR     NOT NULL,
    value       DOUBLE      NOT NULL,
    source      VARCHAR     NOT NULL,
    fetched_at  DATE,
    inserted_at TIMESTAMP   DEFAULT current_timestamp,
    PRIMARY KEY (date, series_key)
)
"""

DDL_PORTFOLIO = """
CREATE TABLE IF NOT EXISTS portfolio (
    ticker        VARCHAR  NOT NULL,
    quantity      DOUBLE   NOT NULL,
    purchase_price DOUBLE  NOT NULL,
    purchase_date DATE,
    label         VARCHAR,
    inserted_at   TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (ticker)
)
"""

DDL_ALERTS = """
CREATE TABLE IF NOT EXISTS alerts (
    alert_id    VARCHAR     NOT NULL,
    ticker      VARCHAR,
    alert_type  VARCHAR     NOT NULL,
    severity    VARCHAR     NOT NULL,
    value       DOUBLE,
    threshold   DOUBLE,
    message     VARCHAR,
    triggered_at TIMESTAMP  NOT NULL,
    is_read     BOOLEAN     DEFAULT false,
    PRIMARY KEY (alert_id)
)
"""

DDL_SYNTHESES = """
CREATE TABLE IF NOT EXISTS syntheses (
    synthesis_id  VARCHAR     NOT NULL,
    generated_at  TIMESTAMP   NOT NULL,
    content       VARCHAR     NOT NULL,
    alert_count   INTEGER,
    macro_regime  VARCHAR,
    model         VARCHAR,
    inserted_at   TIMESTAMP   DEFAULT current_timestamp,
    PRIMARY KEY (synthesis_id)
)
"""

DDL_DECISIONS = """
CREATE TABLE IF NOT EXISTS decisions (
    decision_id      VARCHAR   NOT NULL,
    generated_at     TIMESTAMP NOT NULL,
    ticker           VARCHAR   NOT NULL,
    decision         VARCHAR   NOT NULL,
    confidence_score INTEGER,
    risk_score       INTEGER,
    momentum_score   INTEGER,
    macro_score      INTEGER,
    reasons          VARCHAR,
    review_condition VARCHAR,
    inserted_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (decision_id)
)
"""

ALL_DDL = [DDL_RAW_PRICES, DDL_RAW_MACRO, DDL_PORTFOLIO, DDL_ALERTS, DDL_SYNTHESES, DDL_DECISIONS]

# Repository

class DuckDBRepository:

    #Couche d'accès aux données DuckDB.

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path or settings.DB_PATH
        self._conn: duckdb.DuckDBPyConnection | None = None

    # Connexion

    def connect(self) -> None:

        #Ouvre la connexion DuckDB.

        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(self._db_path)
        logger.info("DuckDB connecté | path=%s", self._db_path)

    def close(self) -> None:

        #Fermer la connexion.

        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB connexion fermée")

    def initialize(self) -> None:
        
        #Crée toutes les tables si elles n'existent pas encore.

        self._ensure_connected()
        for ddl in ALL_DDL:
            self._conn.execute(ddl)
        logger.info("Schéma DuckDB initialisé (%d tables)", len(ALL_DDL))

    # Écriture — raw_prices

    def upsert_prices(self, df: pd.DataFrame) -> int:

        #Insère ou met à jour les prix dans raw_prices.

        self._ensure_connected()

        if df.empty:
            logger.warning("upsert_prices appelé avec un DataFrame vide")
            return 0

        self._validate_prices_schema(df)

        prices_df = df.copy()

        # Colonnes optionnelles attendues par raw_prices
        if "isin" not in prices_df.columns:
            prices_df["isin"] = ""
        if "company_name" not in prices_df.columns:
            prices_df["company_name"] = prices_df["ticker"]
        if "asset_type" not in prices_df.columns:
            prices_df["asset_type"] = "action"
        if "sector" not in prices_df.columns:
            prices_df["sector"] = None
        if "daily_return" not in prices_df.columns:
            prices_df["daily_return"] = pd.NA

        # Normalisation explicite des types pour DuckDB
        prices_df["date"] = pd.to_datetime(prices_df["date"]).dt.date
        prices_df["ticker"] = prices_df["ticker"].astype("object")
        prices_df["isin"] = prices_df["isin"].fillna("").astype("object")
        prices_df["company_name"] = prices_df["company_name"].fillna("").astype("object")
        prices_df["asset_type"] = prices_df["asset_type"].fillna("action").astype("object")
        prices_df["sector"] = prices_df["sector"].where(prices_df["sector"].notna(), None).astype("object")
        prices_df["close_price"] = pd.to_numeric(prices_df["close_price"], errors="coerce")
        prices_df["volume"] = pd.to_numeric(prices_df["volume"], errors="coerce").fillna(0).astype("int64")
        prices_df["daily_return"] = pd.to_numeric(prices_df["daily_return"], errors="coerce")
        prices_df["source"] = prices_df["source"].fillna("unknown").astype("object")

        prices_df = prices_df[
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

        # Supprimer les lignes sans prix exploitable
        before_drop = len(prices_df)
        prices_df = prices_df.dropna(subset=["date", "ticker", "close_price"])
        dropped = before_drop - len(prices_df)

        if dropped > 0:
            logger.warning(
                "%s lignes supprimées avant insertion DuckDB car invalides",
                dropped,
            )

        pairs_df = prices_df[["date", "ticker"]].drop_duplicates()

        self._conn.register("prices_df", prices_df)
        self._conn.register("pairs_df", pairs_df)

        try:
            # DuckDB : utiliser DELETE USING au lieu de WHERE (date, ticker) IN (...)
            self._conn.execute("""
                DELETE FROM raw_prices
                USING pairs_df
                WHERE raw_prices.date = pairs_df.date
                AND raw_prices.ticker = pairs_df.ticker
            """)

            self._conn.execute("""
                INSERT INTO raw_prices
                    (date, ticker, isin, company_name, asset_type,
                    sector, close_price, volume, daily_return, source)
                SELECT
                    date,
                    ticker,
                    isin,
                    company_name,
                    asset_type,
                    sector,
                    close_price,
                    volume,
                    daily_return,
                    source
                FROM prices_df
            """)
        finally:
            self._conn.unregister("prices_df")
            self._conn.unregister("pairs_df")

        count = len(prices_df)
        logger.info("upsert_prices terminé | lignes=%d", count)
        return count    

    # Écriture — raw_macro

    def upsert_macro(self, df: pd.DataFrame) -> int:
        
        #Insère ou met à jour les données macro dans raw_macro.

        self._ensure_connected()

        if df.empty:
            logger.warning("upsert_macro appelé avec un DataFrame vide")
            return 0

        required = {"date", "series_key", "value", "source"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans df macro : {missing}")

        self._conn.execute("""
            DELETE FROM raw_macro
            USING df
            WHERE raw_macro.date = df.date
              AND raw_macro.series_key = df.series_key
        """)

        self._conn.execute("""
            INSERT INTO raw_macro (date, series_key, value, source, fetched_at)
            SELECT date, series_key, value, source, fetched_at
            FROM df
        """)

        count = len(df)
        logger.info("upsert_macro terminé | lignes=%d", count)
        return count

    # Lecture — raw_prices

    def fetch_prices(
        self,
        ticker: str | None = None,
        start: date | None = None,
        end: date | None = None,
        asset_type: str | None = None,
    ) -> pd.DataFrame:
        
        #Lit les prix depuis raw_prices avec filtres optionnels.

        self._ensure_connected()

        conditions: list[str] = []
        params: dict = {}

        if ticker:
            conditions.append("ticker = $ticker")
            params["ticker"] = ticker
        if start:
            conditions.append("date >= $start")
            params["start"] = start
        if end:
            conditions.append("date <= $end")
            params["end"] = end
        if asset_type:
            conditions.append("asset_type = $asset_type")
            params["asset_type"] = asset_type

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f"""
            SELECT date, ticker, isin, company_name, asset_type,
                   sector, close_price, volume, daily_return, source
            FROM raw_prices
            {where}
            ORDER BY ticker, date
        """

        df = self._conn.execute(query, params).df()
        logger.debug("fetch_prices | lignes=%d | filtres=%s", len(df), params)
        return df

    def fetch_latest_prices(self) -> pd.DataFrame:
        
        #Retourne le dernier prix connu pour chaque ticker. Utile pour calculer la valeur courante du portefeuille.
        
        self._ensure_connected()

        query = """
            SELECT DISTINCT ON (ticker)
                date, ticker, isin, company_name, asset_type,
                sector, close_price, volume, daily_return, source
            FROM raw_prices
            ORDER BY ticker, date DESC
        """
        df = self._conn.execute(query).df()
        logger.debug("fetch_latest_prices | lignes=%d", len(df))
        return df

    def fetch_benchmark(
        self,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        
        #Retourne l'historique du benchmark (CAC 40 par défaut).

        return self.fetch_prices(
            ticker=settings.BENCHMARK_TICKER,
            start=start,
            end=end,
        )

    # Lecture — raw_macro

    def fetch_macro(
        self,
        series_key: str | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        
        #Lit les données macro avec filtres optionnels.

        self._ensure_connected()

        conditions: list[str] = []
        params: dict = {}

        if series_key:
            conditions.append("series_key = $series_key")
            params["series_key"] = series_key
        if start:
            conditions.append("date >= $start")
            params["start"] = start
        if end:
            conditions.append("date <= $end")
            params["end"] = end

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f"""
            SELECT date, series_key, value, source, fetched_at
            FROM raw_macro
            {where}
            ORDER BY series_key, date
        """
        return self._conn.execute(query, params).df()

    def fetch_latest_macro(self) -> pd.DataFrame:

        #Retourne la dernière valeur connue de chaque série macro.

        self._ensure_connected()

        query = """
            SELECT DISTINCT ON (series_key)
                date, series_key, value, source
            FROM raw_macro
            ORDER BY series_key, date DESC
        """
        return self._conn.execute(query).df()

    # Portefeuille

    def upsert_portfolio(self, df: pd.DataFrame) -> int:
        
        #Insère ou remplace les positions du portefeuille.

        self._ensure_connected()

        required = {"ticker", "quantity", "purchase_price"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans df portfolio : {missing}")

        # Ajouter les colonnes optionnelles si absentes
        if "purchase_date" not in df.columns:
            df = df.copy()
            df["purchase_date"] = None
        if "label" not in df.columns:
            df = df.copy()
            df["label"] = None

        self._conn.execute("""
            DELETE FROM portfolio WHERE ticker IN (SELECT ticker FROM df)
        """)
        self._conn.execute("""
            INSERT INTO portfolio (ticker, quantity, purchase_price, purchase_date, label)
            SELECT ticker, quantity, purchase_price, purchase_date, label
            FROM df
        """)

        count = len(df)
        logger.info("upsert_portfolio terminé | lignes=%d", count)
        return count

    def fetch_portfolio(self) -> pd.DataFrame:

        #Retourne toutes les positions du portefeuille.

        self._ensure_connected()
        return self._conn.execute(
            "SELECT * FROM portfolio ORDER BY ticker"
        ).df()

    # Alertes

    def insert_alerts(self, df: pd.DataFrame) -> int:
        
        #Insère de nouvelles alertes (sans écraser les existantes).

        self._ensure_connected()

        if df.empty:
            return 0

        required = {"alert_id", "alert_type", "severity", "triggered_at"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans df alerts : {missing}")

        # INSERT OR IGNORE via filtre sur alert_id existants
        self._conn.execute("""
            INSERT INTO alerts
                (alert_id, ticker, alert_type, severity,
                 value, threshold, message, triggered_at)
            SELECT alert_id, ticker, alert_type, severity,
                   value, threshold, message, triggered_at
            FROM df
            WHERE alert_id NOT IN (SELECT alert_id FROM alerts)
        """)

        count = len(df)
        logger.info("insert_alerts terminé | lignes=%d", count)
        return count

    def fetch_alerts(self, unread_only: bool = False) -> pd.DataFrame:

        #Retourne les alertes, optionnellement filtrées sur non-lues.

        self._ensure_connected()

        where = "WHERE is_read = false" if unread_only else ""
        return self._conn.execute(f"""
            SELECT * FROM alerts
            {where}
            ORDER BY triggered_at DESC
        """).df()

    def mark_alerts_read(self, alert_ids: list[str]) -> None:

        #Marque une liste d'alertes comme lues.

        self._ensure_connected()

        if not alert_ids:
            return

        ids_df = pd.DataFrame({"alert_id": alert_ids})
        self._conn.execute("""
            UPDATE alerts
            SET is_read = true
            WHERE alert_id IN (SELECT alert_id FROM ids_df)
        """)

    # Syntheses (Agent Analyste)

    def insert_synthesis(
        self,
        content: str,
        alert_count: int = 0,
        macro_regime: str | None = None,
        model: str | None = None,
    ) -> str:
        self._ensure_connected()

        import uuid
        synthesis_id = str(uuid.uuid4())

        df = pd.DataFrame([{
            "synthesis_id": synthesis_id,
            "generated_at": pd.Timestamp.now(),
            "content": content,
            "alert_count": alert_count,
            "macro_regime": macro_regime,
            "model": model,
        }])

        self._conn.execute("""
            INSERT INTO syntheses
                (synthesis_id, generated_at, content, alert_count, macro_regime, model)
            SELECT synthesis_id, generated_at, content, alert_count, macro_regime, model
            FROM df
        """)

        logger.info("insert_synthesis terminé | synthesis_id=%s", synthesis_id)
        return synthesis_id

    def fetch_latest_synthesis(self) -> pd.DataFrame:

        #Retourne la synthèse la plus récente (1 ligne, ou vide si aucune).

        self._ensure_connected()
        return self._conn.execute("""
            SELECT * FROM syntheses
            ORDER BY generated_at DESC
            LIMIT 1
        """).df()

    def fetch_syntheses(self, limit: int = 20) -> pd.DataFrame:

        #Retourne les N synthèses les plus récentes.

        self._ensure_connected()
        return self._conn.execute(f"""
            SELECT * FROM syntheses
            ORDER BY generated_at DESC
            LIMIT {limit}
        """).df()
    
    # Décisions (Decision Engine)
 
    def insert_decisions(self, decisions: list, generated_at=None) -> int:
        self._ensure_connected()
 
        if not decisions:
            return 0
 
        import uuid
        timestamp = generated_at or pd.Timestamp.now()
 
        rows = [
            {
                "decision_id": str(uuid.uuid4()),
                "generated_at": timestamp,
                "ticker": d.ticker,
                "decision": d.decision.value,
                "confidence_score": d.confidence_score,
                "risk_score": d.risk_score,
                "momentum_score": d.momentum_score,
                "macro_score": d.macro_score,
                "reasons": " | ".join(d.reasons),
                "review_condition": d.review_condition,
            }
            for d in decisions
        ]
 
        df = pd.DataFrame(rows)
 
        self._conn.execute("""
            INSERT INTO decisions
                (decision_id, generated_at, ticker, decision, confidence_score,
                 risk_score, momentum_score, macro_score, reasons, review_condition)
            SELECT decision_id, generated_at, ticker, decision, confidence_score,
                   risk_score, momentum_score, macro_score, reasons, review_condition
            FROM df
        """)
 
        count = len(rows)
        logger.info("insert_decisions terminé | lignes=%d", count)
        return count
 
    def fetch_latest_decisions(self) -> pd.DataFrame:

        #Retourne toutes les décisions du run le plus récent (même generated_at).

        self._ensure_connected()
        return self._conn.execute("""
            SELECT * FROM decisions
            WHERE generated_at = (SELECT MAX(generated_at) FROM decisions)
            ORDER BY ticker
        """).df()
 
    def fetch_decisions_history(self, ticker: str | None = None, limit: int = 50) -> pd.DataFrame:
        
        #Retourne l'historique des décisions, optionnellement filtré par ticker.

        self._ensure_connected()
        if ticker:
            return self._conn.execute(
                "SELECT * FROM decisions WHERE ticker = $ticker "
                "ORDER BY generated_at DESC LIMIT $limit",
                {"ticker": ticker, "limit": limit},
            ).df()
        return self._conn.execute(
            "SELECT * FROM decisions ORDER BY generated_at DESC LIMIT $limit",
            {"limit": limit},
        ).df()
    
    # Utilitaires

    def table_info(self) -> pd.DataFrame:

        #Retourne le nombre de lignes par table — utile pour les tests.

        self._ensure_connected()

        tables = ["raw_prices", "raw_macro", "portfolio", "alerts"]
        rows = []
        for table in tables:
            try:
                count = self._conn.execute(
                    f"SELECT COUNT(*) as n FROM {table}"
                ).fetchone()[0]
                rows.append({"table": table, "rows": count})
            except Exception:
                rows.append({"table": table, "rows": -1})

        return pd.DataFrame(rows)

    def execute_query(self, query: str) -> pd.DataFrame:
        self._ensure_connected()
        return self._conn.execute(query).df()


    def _ensure_connected(self) -> None:
        if self._conn is None:
            raise RuntimeError(
                "DuckDBRepository non connecté. "
                "Appelle .connect() ou utilise le context manager."
            )

    @staticmethod
    def _validate_prices_schema(df: pd.DataFrame) -> None:
        required = {
            "date", "ticker", "close_price", "volume", "source"
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans df prices : {missing}")


    def __enter__(self) -> DuckDBRepository:
        self.connect()
        self.initialize()
        return self

    def __exit__(self, *_) -> None:
        self.close()

if __name__ == "__main__":
    import logging
    from datetime import date, timedelta
    from app.pipeline.data_collector import DataCollector

    logging.basicConfig(level=logging.INFO)

    # 1. Collecter les données de marché
    collector = DataCollector()
    df_market = collector.collect_market_data(
        start=date.today() - timedelta(days=30)
    )
    print(f"\n✓ Données collectées : {len(df_market)} lignes")

    # 2. Stocker dans DuckDB
    with DuckDBRepository() as repo:
        written = repo.upsert_prices(df_market)
        print(f"✓ Lignes écrites dans raw_prices : {written}")

        # 3. Vérifier le stockage
        print("\n--- Aperçu raw_prices ---")
        df_check = repo.fetch_prices(ticker="BNP.PA")
        print(df_check.tail(5))

        # 4. Derniers prix
        print("\n--- Derniers prix par ticker ---")
        df_latest = repo.fetch_latest_prices()
        print(df_latest[["ticker", "date", "close_price", "daily_return"]])

        # 5. Info tables
        print("\n--- Résumé des tables ---")
        print(repo.table_info())