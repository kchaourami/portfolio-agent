"""
decision_models.py
====================
Emplacement cible : app/pipeline/decision_models.py

Structures de données du Decision Engine — la couche qui transforme les
indicateurs déterministes (mart_risk_signals, mart_portfolio_value) en
décisions structurées par ticker, avant que l'Agent Analyste ne les
explique en langage naturel.

Ce module est ENTIÈREMENT déterministe (aucun appel LLM) — c'est pour
cette raison qu'il vit dans app/pipeline/ et non app/agents/, exactement
comme risk_calculator.py et macro_regime.py. Le LLM (Agent Analyste,
seul composant sous app/agents/) reçoit ces décisions déjà prises et les
explique, il ne les invente ni ne les recalcule.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class Decision(str, Enum):
    """
    Échelle de décision — volontairement à 5 niveaux, pas binaire
    achat/vente. Le code reste en anglais (convention standard en
    finance, utile pour la donnée/l'audit) — le libellé français
    d'affichage est dans DECISION_LABELS_FR ci-dessous.
    """

    BUY_WATCH = "BUY_WATCH"      # opportunité à surveiller (momentum positif, risque faible)
    INCREASE = "INCREASE"        # renforcer à étudier — réservé aux ETF/positions sous-pondérées
    HOLD = "HOLD"                # conserver, pas d'action prioritaire
    WATCH = "WATCH"              # signal faible ou mixte, à surveiller
    REDUCE = "REDUCE"            # réduire l'exposition à étudier
    SELL_SIGNAL = "SELL_SIGNAL"  # signal de vente simulé fort


# ---------------------------------------------------------------------------
# Libellés français — utilisés pour l'affichage et dans le prompt de
# l'Agent Analyste (qui reçoit les deux formes, code + libellé, pour
# pouvoir rédiger naturellement en français sans avoir à traduire
# lui-même le code — ce qui réduirait le risque de mauvaise traduction).
# ---------------------------------------------------------------------------

DECISION_LABELS_FR: dict[Decision, str] = {
    Decision.BUY_WATCH: "Opportunité à surveiller",
    Decision.INCREASE: "Renforcement à étudier",
    Decision.HOLD: "Conserver",
    Decision.WATCH: "Sous surveillance",
    Decision.REDUCE: "Réduction à étudier",
    Decision.SELL_SIGNAL: "Signal de vente",
}


def decision_label_fr(decision: Decision) -> str:
    """Libellé français d'affichage pour une décision donnée."""
    return DECISION_LABELS_FR[decision]


class TickerDecision(BaseModel):
    """
    Décision structurée pour un ticker donné — produite uniquement à
    partir de valeurs déjà calculées par les pipelines en amont. Chaque
    champ de score est sur une échelle 0-100 pour rester interprétable
    et comparable entre tickers.
    """

    ticker: str
    decision: Decision
    confidence_score: int          # 0-100, force du signal
    risk_score: int                # 0-100, niveau de risque détecté
    momentum_score: int            # 0-100, où 50 = neutre, >50 = momentum positif
    macro_score: int                # 0-100, où 50 = neutre, ajustement selon le régime macro
    reasons: list[str]              # phrases factuelles courtes, une par signal contributeur
    review_condition: str           # ce qu'il faudrait observer pour reconsidérer la décision

    @property
    def decision_label_fr(self) -> str:
        """Libellé français de la décision — pratique pour l'affichage direct."""
        return DECISION_LABELS_FR[self.decision]


class PortfolioDecisions(BaseModel):
    """Ensemble des décisions pour un run donné — une par ticker en portefeuille."""

    decisions: list[TickerDecision]
    generated_at: str