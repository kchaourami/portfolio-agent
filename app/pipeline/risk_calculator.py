"""
risk_calculator.py
===================
Emplacement cible dans le repo : app/pipeline/risk_calculator.py

Agent Risk — partie CALCUL uniquement (pas de persistance ici).

Rôle :
- Lire les derniers indicateurs depuis mart_risk_signals et mart_portfolio_value
- Comparer chaque indicateur aux seuils configurés (settings.py)
- Retourner une liste de RiskBreach (dépassements détectés)

Les fonctions de check sont pures et testables avec des DataFrames en mémoire
(aucun appel DuckDB à l'intérieur). Seule run_risk_calculator() fait le pont
avec le repository — c'est le point d'entrée que l'orchestrateur (LangGraph)
appellera.

Aucune Alert n'est créée ou écrite en base ici — c'est le rôle de
alert_engine.py, qui consomme la liste de RiskBreach retournée.
""" 

from __future__ import annotations

import logging

import pandas as pd

from app.config.settings import settings
from app.pipeline.risk_models import AlertType, RiskBreach, Severity
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Noms qualifiés des tables dbt
# ---------------------------------------------------------------------------
# IMPORTANT : avec dbt-duckdb, le schéma effectif d'un modèle avec
# `+schema: marts` dans dbt_project.yml n'est PAS forcément "marts" tout
# court — le macro générique de dbt concatène généralement
# "<schéma par défaut>_<schéma custom>" (ex: "main_marts").
# Vérifiez le nom réel avec :
#   repo.execute_query(
#       "SELECT table_schema, table_name FROM information_schema.tables "
#       "WHERE table_name LIKE 'mart_%'"
#   )
# et ajustez les deux constantes ci-dessous si besoin.

MART_RISK_SIGNALS = "main_marts.mart_risk_signals"
MART_PORTFOLIO_VALUE = "main_marts.mart_portfolio_value"


# ---------------------------------------------------------------------------
# Sévérité
# ---------------------------------------------------------------------------

def _severity_below(value: float, threshold: float) -> Severity:
    """
    Sévérité pour les seuils 'plage basse' (drop, drawdown, sous-performance) :
    la valeur dépasse le seuil quand elle est INFÉRIEURE à celui-ci
    (les deux sont négatifs). Critique si la valeur dépasse 1.5x le seuil
    en intensité.
    """
    if value <= threshold * 1.5:
        return Severity.CRITICAL
    return Severity.WARNING


def _severity_above(value: float, threshold: float) -> Severity:
    """
    Sévérité pour les seuils 'plage haute' (volume, volatilité, concentration) :
    la valeur dépasse le seuil quand elle est SUPÉRIEURE à celui-ci.
    Critique si la valeur dépasse 1.5x le seuil.
    """
    if value >= threshold * 1.5:
        return Severity.CRITICAL
    return Severity.WARNING


# ---------------------------------------------------------------------------
# Checks — niveau ticker
# ---------------------------------------------------------------------------

def check_price_drop(
    df_latest: pd.DataFrame,
    threshold: float = settings.ALERT_PRICE_DROP_PCT,
) -> list[RiskBreach]:
    """PRICE_DROP : daily_return < seuil (défaut -3%)."""
    breaches: list[RiskBreach] = []

    for row in df_latest.itertuples():
        if pd.isna(row.daily_return):
            continue
        if row.daily_return < threshold:
            breaches.append(
                RiskBreach(
                    ticker=row.ticker,
                    alert_type=AlertType.PRICE_DROP,
                    severity=_severity_below(row.daily_return, threshold),
                    value=round(float(row.daily_return), 6),
                    threshold=threshold,
                    date=str(row.date),
                )
            )

    return breaches


def check_abnormal_volume(
    df_latest: pd.DataFrame,
    threshold: float = settings.ALERT_VOLUME_RATIO,
) -> list[RiskBreach]:
    """ABNORMAL_VOLUME : volume_ratio_20d > seuil (défaut 2.0x)."""
    breaches: list[RiskBreach] = []

    for row in df_latest.itertuples():
        if pd.isna(row.volume_ratio_20d):
            continue
        if row.volume_ratio_20d > threshold:
            breaches.append(
                RiskBreach(
                    ticker=row.ticker,
                    alert_type=AlertType.ABNORMAL_VOLUME,
                    severity=_severity_above(row.volume_ratio_20d, threshold),
                    value=round(float(row.volume_ratio_20d), 4),
                    threshold=threshold,
                    date=str(row.date),
                )
            )

    return breaches


def check_underperformance(
    df_latest: pd.DataFrame,
    threshold: float = settings.ALERT_UNDERPERF_PCT,
) -> list[RiskBreach]:
    """UNDERPERFORMANCE : relative_perf_5d < seuil vs CAC 40 (défaut -3%)."""
    breaches: list[RiskBreach] = []

    for row in df_latest.itertuples():
        if pd.isna(row.relative_perf_5d):
            continue
        if row.relative_perf_5d < threshold:
            breaches.append(
                RiskBreach(
                    ticker=row.ticker,
                    alert_type=AlertType.UNDERPERFORMANCE,
                    severity=_severity_below(row.relative_perf_5d, threshold),
                    value=round(float(row.relative_perf_5d), 6),
                    threshold=threshold,
                    date=str(row.date),
                )
            )

    return breaches


def check_high_volatility(
    df_latest: pd.DataFrame,
    threshold: float = settings.ALERT_VOLATILITY_PCT,
) -> list[RiskBreach]:
    """
    HIGH_VOLATILITY : vol_20d > seuil.

    Note méthodologique : la doc Alertes prévoit un seuil dynamique
    (percentile 90 historique). Pour le MVP on utilise un seuil absolu
    fixe (ALERT_VOLATILITY_PCT, défaut 0.02) — cohérent avec le commentaire
    déjà présent dans mart_risk_signals.sql. Le seuil dynamique par
    percentile est une amélioration à documenter pour la V2.
    """
    breaches: list[RiskBreach] = []

    for row in df_latest.itertuples():
        if pd.isna(row.vol_20d):
            continue
        if row.vol_20d > threshold:
            breaches.append(
                RiskBreach(
                    ticker=row.ticker,
                    alert_type=AlertType.HIGH_VOLATILITY,
                    severity=_severity_above(row.vol_20d, threshold),
                    value=round(float(row.vol_20d), 6),
                    threshold=threshold,
                    date=str(row.date),
                )
            )

    return breaches


# ---------------------------------------------------------------------------
# Checks — niveau portefeuille
# ---------------------------------------------------------------------------

def check_portfolio_drawdown(
    df_portfolio: pd.DataFrame,
    threshold: float = settings.ALERT_DRAWDOWN_PCT,
) -> RiskBreach | None:
    """
    PORTFOLIO_DRAWDOWN : drawdown du portefeuille global < seuil (défaut -5%).

    Limitation connue (à documenter dans le mémoire) : ceci est une
    APPROXIMATION — moyenne des drawdowns par ligne pondérée par le poids
    (weight) de chaque ligne. Ce n'est pas le drawdown réel du portefeuille,
    qui nécessiterait un historique de la valeur totale du portefeuille
    jour par jour pour calculer son propre plus haut glissant (rolling
    high) sur 252 jours, comme fait pour chaque ticker dans
    mart_risk_signals.sql. Amélioration V2 : construire un NAV synthétique
    du portefeuille et lui appliquer la même logique.
    """
    if df_portfolio.empty:
        logger.warning("check_portfolio_drawdown: portefeuille vide, aucun calcul")
        return None

    df = df_portfolio.dropna(subset=["drawdown", "weight"])
    if df.empty:
        return None

    weighted_drawdown = float((df["drawdown"] * df["weight"]).sum())

    if weighted_drawdown < threshold:
        return RiskBreach(
            ticker=None,
            alert_type=AlertType.PORTFOLIO_DRAWDOWN,
            severity=_severity_below(weighted_drawdown, threshold),
            value=round(weighted_drawdown, 6),
            threshold=threshold,
            date=str(df["price_date"].max()),
        )

    return None


def check_sector_concentration(
    df_portfolio: pd.DataFrame,
    threshold: float = settings.ALERT_SECTOR_CONCENTRATION_PCT,
) -> list[RiskBreach]:
    """
    SECTOR_CONCENTRATION : poids d'un secteur > seuil (défaut 30%).

    Nécessite une colonne 'sector' dans df_portfolio. mart_portfolio_value
    ne l'expose pas encore aujourd'hui (à ajouter via jointure avec
    mart_risk_signals/stg_prices). Retourne [] si la colonne est absente
    plutôt que de lever une exception, pour ne pas bloquer le pipeline.
    """
    if "sector" not in df_portfolio.columns:
        logger.warning(
            "check_sector_concentration: colonne 'sector' absente de "
            "mart_portfolio_value — vérification ignorée pour l'instant"
        )
        return []

    df = df_portfolio.dropna(subset=["sector", "weight"])
    if df.empty:
        return []

    sector_weights = df.groupby("sector")["weight"].sum()
    price_date = str(df["price_date"].max()) if "price_date" in df.columns else ""
    breaches: list[RiskBreach] = []

    for sector, weight in sector_weights.items():
        if weight > threshold:
            breaches.append(
                RiskBreach(
                    ticker=None,
                    alert_type=AlertType.SECTOR_CONCENTRATION,
                    severity=_severity_above(float(weight), threshold),
                    value=round(float(weight), 4),
                    threshold=threshold,
                    date=price_date,
                    label=str(sector),
                )
            )
            logger.info(
                "Concentration sectorielle détectée : %s = %.1f%%",
                sector,
                weight * 100,
            )

    return breaches


# ---------------------------------------------------------------------------
# Orchestration interne
# ---------------------------------------------------------------------------

def _latest_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """Garde uniquement la dernière date disponible pour chaque ticker."""
    if df.empty:
        return df
    return (
        df.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def evaluate_all(
    df_risk_signals: pd.DataFrame,
    df_portfolio: pd.DataFrame,
) -> list[RiskBreach]:
    """
    Exécute tous les checks et retourne la liste consolidée des breaches.

    Args:
        df_risk_signals : contenu de mart_risk_signals (historique complet)
        df_portfolio    : contenu de mart_portfolio_value (positions valorisées)

    Returns:
        Liste de RiskBreach, vide si aucun dépassement détecté.
    """
    df_latest = _latest_per_ticker(df_risk_signals)

    breaches: list[RiskBreach] = []
    breaches += check_price_drop(df_latest)
    breaches += check_abnormal_volume(df_latest)
    breaches += check_underperformance(df_latest)
    breaches += check_high_volatility(df_latest)
    breaches += check_sector_concentration(df_portfolio)

    portfolio_breach = check_portfolio_drawdown(df_portfolio)
    if portfolio_breach:
        breaches.append(portfolio_breach)

    logger.info("evaluate_all terminé | breaches détectées=%d", len(breaches))
    return breaches


def run_risk_calculator(repo: DuckDBRepository) -> list[RiskBreach]:
    """
    Point d'entrée utilisé par l'orchestrateur (LangGraph).

    Lit les marts dbt nécessaires depuis DuckDB et délègue le calcul à
    evaluate_all. Aucune écriture en base ici — voir alert_engine.py.
    """
    df_risk_signals = repo.execute_query(f"SELECT * FROM {MART_RISK_SIGNALS}")
    df_portfolio = repo.execute_query(f"SELECT * FROM {MART_PORTFOLIO_VALUE}")

    return evaluate_all(df_risk_signals, df_portfolio)


# ---------------------------------------------------------------------------
# Test rapide — python -m app.pipeline.risk_calculator
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with DuckDBRepository() as repo:
        breaches = run_risk_calculator(repo)

        print(f"\n✓ {len(breaches)} dépassement(s) détecté(s)\n")
        for b in breaches:
            print(
                f"  [{b.severity.value.upper()}] {b.alert_type.value} "
                f"| ticker={b.ticker or 'PORTFOLIO'} "
                f"| valeur={b.value} (seuil={b.threshold})"
            )