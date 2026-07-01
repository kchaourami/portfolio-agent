"""
Role :
Structures de données du Decision Engine — la couche qui transforme les
indicateurs déterministes (mart_risk_signals, mart_portfolio_value) en
décisions structurées par ticker, avant que l'Agent Analyste ne les
explique en langage naturel.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class Decision(str, Enum):
    
    #Échelle de décision à 5 niveaux

    BUY_WATCH = "BUY_WATCH"      # opportunité à surveiller (momentum positif, risque faible)
    INCREASE = "INCREASE"        # renforcer à étudier — réservé aux ETF/positions sous-pondérées
    HOLD = "HOLD"                # conserver, pas d'action prioritaire
    WATCH = "WATCH"              # signal faible ou mixte, à surveiller
    REDUCE = "REDUCE"            # réduire l'exposition à étudier
    SELL_SIGNAL = "SELL_SIGNAL"  # signal de vente simulé fort

DECISION_LABELS_FR: dict[Decision, str] = {
    Decision.BUY_WATCH: "Opportunité à surveiller",
    Decision.INCREASE: "Renforcement à étudier",
    Decision.HOLD: "Conserver",
    Decision.WATCH: "Sous surveillance",
    Decision.REDUCE: "Réduction à étudier",
    Decision.SELL_SIGNAL: "Signal de vente",
}


def decision_label_fr(decision: Decision) -> str:
    return DECISION_LABELS_FR[decision]


class TickerDecision(BaseModel):
    
    #Décision structurée pour un ticker donné — produite uniquement à partir de valeurs déjà calculées par les pipelines en amont. 

    ticker: str
    decision: Decision
    confidence_score: int        
    risk_score: int               
    momentum_score: int            
    macro_score: int                
    reasons: list[str]              
    review_condition: str         

    @property
    def decision_label_fr(self) -> str:
        return DECISION_LABELS_FR[self.decision]
