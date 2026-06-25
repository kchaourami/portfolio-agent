"""
decision_engine.py
====================
Emplacement cible : app/pipeline/decision_engine.py

Decision Engine — calcule, pour chaque ticker du portefeuille, un score
de risque, un score de momentum, un score macro, et en déduit une
décision structurée (BUY_WATCH / INCREASE / HOLD / WATCH / REDUCE /
SELL_SIGNAL). Entièrement déterministe — aucun appel LLM, exactement
comme risk_calculator.py et macro_regime.py.

Méthodologie — seuils identiques à ceux déjà utilisés pour les alertes
dans settings.py, pas de nouveaux seuils inventés pour l'occasion :

risk_score (0-100, cumulatif, plafonné) :
    drawdown < seuil ALERT_DRAWDOWN_PCT          → +30
    vol_20d > seuil ALERT_VOLATILITY_PCT          → +20
    relative_perf_5d < seuil ALERT_UNDERPERF_PCT  → +20
    daily_return < seuil ALERT_PRICE_DROP_PCT     → +20
    volume_ratio_20d > seuil ALERT_VOLUME_RATIO   → +10

momentum_score (0-100, 50 = neutre) :
    return_20d positif/négatif       → +/-25 autour de 50
    relative_perf_5d positif/négatif → +/-25 autour de 50

macro_score (0-100, 50 = neutre) :
    ajustement doux selon le régime macro, plus marqué pour les
    secteurs cycliques (Industrials, Consumer Cyclical, Financial
    Services) que pour les autres

Table de décision (combinaison risk/momentum) :
    risk_score >= 60                          → SELL_SIGNAL
    risk_score 30-59                          → REDUCE
    risk_score < 30 et momentum_score > 60    → BUY_WATCH (action) / INCREASE (ETF)
    risk_score < 30 et momentum_score 40-60   → HOLD
    risk_score < 30 et momentum_score < 40    → WATCH
"""

from __future__ import annotations

import logging

import pandas as pd

from app.config.settings import settings
from app.pipeline.decision_models import Decision, TickerDecision
from app.pipeline.macro_regime import MacroRegime, get_current_macro_regime
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seuils de la table de décision — distincts des seuils d'alerte (ceux-là
# définissent une échelle de score 0-100, pas un seuil métier direct)
# ---------------------------------------------------------------------------

RISK_SCORE_SELL_THRESHOLD = 60
RISK_SCORE_REDUCE_THRESHOLD = 30
MOMENTUM_BUY_THRESHOLD = 60
MOMENTUM_WATCH_THRESHOLD = 40

CYCLICAL_SECTORS = {"Industrials", "Consumer Cyclical", "Financial Services"}


# ---------------------------------------------------------------------------
# Scores
# ---------------------------------------------------------------------------

def _compute_risk_score(row: pd.Series) -> tuple[int, list[str]]:
    """Calcule le risk_score et la liste des raisons qui y ont contribué."""
    score = 0
    reasons: list[str] = []

    if pd.notna(row.get("drawdown")) and row["drawdown"] < settings.ALERT_DRAWDOWN_PCT:
        score += 30
        reasons.append(
            f"drawdown de {row['drawdown']*100:.2f}% "
            f"(seuil {settings.ALERT_DRAWDOWN_PCT*100:.2f}%)"
        )

    if pd.notna(row.get("vol_20d")) and row["vol_20d"] > settings.ALERT_VOLATILITY_PCT:
        score += 20
        reasons.append(
            f"volatilité 20j à {row['vol_20d']*100:.2f}% "
            f"(seuil {settings.ALERT_VOLATILITY_PCT*100:.2f}%)"
        )

    if pd.notna(row.get("relative_perf_5d")) and row["relative_perf_5d"] < settings.ALERT_UNDERPERF_PCT:
        score += 20
        reasons.append(
            f"sous-performance de {row['relative_perf_5d']*100:.2f}% vs CAC 40 sur 5j "
            f"(seuil {settings.ALERT_UNDERPERF_PCT*100:.2f}%)"
        )

    if pd.notna(row.get("daily_return")) and row["daily_return"] < settings.ALERT_PRICE_DROP_PCT:
        score += 20
        reasons.append(
            f"baisse journalière de {row['daily_return']*100:.2f}% "
            f"(seuil {settings.ALERT_PRICE_DROP_PCT*100:.2f}%)"
        )

    if pd.notna(row.get("volume_ratio_20d")) and row["volume_ratio_20d"] > settings.ALERT_VOLUME_RATIO:
        score += 10
        reasons.append(
            f"volume anormal à {row['volume_ratio_20d']:.1f}x la moyenne "
            f"(seuil {settings.ALERT_VOLUME_RATIO:.1f}x)"
        )

    return min(score, 100), reasons


def _compute_momentum_score(row: pd.Series) -> tuple[int, list[str]]:
    """
    Calcule le momentum_score (0-100, 50 = neutre) à partir de return_20d
    et relative_perf_5d — chaque indicateur déplace le score de +/-25
    points autour de 50, plafonné à [0, 100]. Retourne aussi les raisons
    associées, pour que les décisions pilotées par le momentum (INCREASE,
    BUY_WATCH, WATCH) restent pleinement explicables — y compris quand un
    facteur de risque isolé coexiste avec un momentum positif (ex: une
    baisse ponctuelle dans une tendance haussière de fond).
    """
    score = 50
    reasons: list[str] = []

    if pd.notna(row.get("return_20d")):
        score += 25 if row["return_20d"] > 0 else -25
        sens = "positif" if row["return_20d"] > 0 else "négatif"
        reasons.append(f"rendement 20j {sens} ({row['return_20d']*100:+.2f}%)")

    if pd.notna(row.get("relative_perf_5d")):
        score += 25 if row["relative_perf_5d"] > 0 else -25
        sens = "positive" if row["relative_perf_5d"] > 0 else "négative"
        reasons.append(f"performance relative 5j {sens} vs CAC 40 ({row['relative_perf_5d']*100:+.2f}%)")

    return max(0, min(100, score)), reasons


def _compute_macro_score(regime: MacroRegime, sector: str | None) -> int:
    """
    Ajustement doux selon le régime macro — 50 = neutre. Les secteurs
    cycliques sont plus sensibles au régime que les secteurs défensifs
    ou les ETF diversifiés (sector=None).
    """
    is_cyclical = sector in CYCLICAL_SECTORS

    if regime.regime == "restrictif":
        return 35 if is_cyclical else 45
    if regime.regime == "accommodant":
        return 65 if is_cyclical else 55
    return 50  # neutre ou indéterminé


# ---------------------------------------------------------------------------
# Décision
# ---------------------------------------------------------------------------

def _decide(risk_score: int, momentum_score: int, asset_type: str | None) -> tuple[Decision, int]:
    """Applique la table de décision et calcule le confidence_score associé."""
    if risk_score >= RISK_SCORE_SELL_THRESHOLD:
        return Decision.SELL_SIGNAL, risk_score

    if risk_score >= RISK_SCORE_REDUCE_THRESHOLD:
        return Decision.REDUCE, risk_score

    if momentum_score > MOMENTUM_BUY_THRESHOLD:
        confidence = min(100, abs(momentum_score - 50) * 2)
        decision = Decision.INCREASE if asset_type == "etf" else Decision.BUY_WATCH
        return decision, confidence

    if momentum_score < MOMENTUM_WATCH_THRESHOLD:
        return Decision.WATCH, 50

    return Decision.HOLD, 50


def _build_review_condition(decision: Decision, ticker: str) -> str:
    """Phrase indiquant ce qu'il faudrait observer pour reconsidérer la décision."""
    if decision in (Decision.SELL_SIGNAL, Decision.REDUCE):
        return f"Repasser à HOLD si les signaux de risque sur {ticker} redescendent sous les seuils d'alerte"
    if decision in (Decision.BUY_WATCH, Decision.INCREASE):
        return f"Reconsidérer si le momentum de {ticker} (return_20d, relative_perf_5d) redevient négatif"
    if decision == Decision.WATCH:
        return f"Repasser à HOLD ou REDUCE selon l'évolution du momentum sur {ticker}"
    return f"Aucune condition de révision urgente pour {ticker}"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def compute_decisions(repo: DuckDBRepository) -> list[TickerDecision]:
    """
    Point d'entrée pur calcul — calcule une décision structurée pour
    chaque ticker du portefeuille, à partir de mart_portfolio_value
    (qui contient déjà les indicateurs de risque les plus récents par
    ticker) et du régime macro courant. Ne persiste rien.
    """
    df = repo.execute_query("SELECT * FROM main_marts.mart_portfolio_value")

    if df.empty:
        return []

    regime = get_current_macro_regime(repo)

    decisions: list[TickerDecision] = []

    for _, row in df.iterrows():
        risk_score, risk_reasons = _compute_risk_score(row)
        momentum_score, momentum_reasons = _compute_momentum_score(row)
        macro_score = _compute_macro_score(regime, row.get("sector"))

        decision, confidence_score = _decide(
            risk_score, momentum_score, row.get("asset_type")
        )

        reasons = risk_reasons + momentum_reasons
        if not reasons:
            reasons = ["Aucun signal de risque ou de momentum significatif détecté"]

        decisions.append(
            TickerDecision(
                ticker=row["ticker"],
                decision=decision,
                confidence_score=confidence_score,
                risk_score=risk_score,
                momentum_score=momentum_score,
                macro_score=macro_score,
                reasons=reasons,
                review_condition=_build_review_condition(decision, row["ticker"]),
            )
        )

    return decisions


def run_decision_engine(repo: DuckDBRepository) -> list[TickerDecision]:
    """
    Point d'entrée utilisé par l'orchestrateur (LangGraph) — calcule les
    décisions ET les persiste dans la table `decisions`. Même principe
    que run_alert_engine (alert_engine.py) : calcul + persistance en un
    seul point d'entrée pour le graphe, séparé du calcul pur
    (compute_decisions) pour les usages qui n'ont pas besoin d'écrire
    en base (tests, scripts d'exploration).
    """
    decisions = compute_decisions(repo)
    if decisions:
        repo.insert_decisions(decisions)
        logger.info("run_decision_engine terminé | décisions=%d", len(decisions))
    return decisions


# ---------------------------------------------------------------------------
# Test rapide — python -m app.pipeline.decision_engine
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    with DuckDBRepository() as repo:
        df_check = repo.execute_query("SELECT COUNT(*) as n FROM main_marts.mart_portfolio_value")
        print(f"\nLignes dans mart_portfolio_value : {df_check['n'][0]}")

        decisions = run_decision_engine(repo)
        print(f"Décisions calculées et persistées : {len(decisions)}\n")

    if not decisions:
        print(" Aucune décision produite — vérifiez que mart_portfolio_value n'est pas vide.")

    for d in decisions:
        print(f"{d.ticker:10s} {d.decision.value:12s} ({d.decision_label_fr}) confiance={d.confidence_score}")
        print(f"    risk={d.risk_score} momentum={d.momentum_score} macro={d.macro_score}")
        for reason in d.reasons:
            print(f"    - {reason}")
        print(f"    Révision : {d.review_condition}\n")