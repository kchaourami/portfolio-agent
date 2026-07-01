"""
Modèles de données partagés entre risk_calculator.py et alert_engine.py.
Ne contient aucune logique métier — uniquement les structures de données
utilisées pour faire transiter les signaux de risque détectés.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel

class AlertType(str, Enum):
    #Types d'alertes supportés (cf. doc Alertes — Système d'alertes).

    PRICE_DROP = "PRICE_DROP"
    PORTFOLIO_DRAWDOWN = "PORTFOLIO_DRAWDOWN"
    ABNORMAL_VOLUME = "ABNORMAL_VOLUME"
    UNDERPERFORMANCE = "UNDERPERFORMANCE"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    SECTOR_CONCENTRATION = "SECTOR_CONCENTRATION"


class Severity(str, Enum):
    #Niveau de sévérité d'une alerte.

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class RiskBreach(BaseModel):
    ticker: str | None        
    alert_type: AlertType
    severity: Severity
    value: float
    threshold: float
    date: str                  
    label: str | None = None   


class Alert(BaseModel):
    #Alerte persistée — cf. doc Alertes (Système d'alertes configurables).

    alert_id: str
    ticker: str | None
    alert_type: AlertType
    severity: Severity
    value: float
    threshold: float
    triggered_at: datetime
    message: str