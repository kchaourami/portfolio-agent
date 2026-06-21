"""
risk_models.py
==============
Emplacement cible dans le repo : app/pipeline/risk_models.py

Modèles de données partagés entre risk_calculator.py et alert_engine.py.
Ne contient aucune logique métier — uniquement les structures de données
utilisées pour faire transiter les signaux de risque détectés.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class AlertType(str, Enum):
    """Types d'alertes supportés (cf. doc Alertes — Système d'alertes)."""

    PRICE_DROP = "PRICE_DROP"
    PORTFOLIO_DRAWDOWN = "PORTFOLIO_DRAWDOWN"
    ABNORMAL_VOLUME = "ABNORMAL_VOLUME"
    UNDERPERFORMANCE = "UNDERPERFORMANCE"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    SECTOR_CONCENTRATION = "SECTOR_CONCENTRATION"


class Severity(str, Enum):
    """Niveau de sévérité d'une alerte."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class RiskBreach(BaseModel):
    """
    Dépassement de seuil détecté par risk_calculator.py.

    C'est une donnée intermédiaire — pas encore une Alert persistée.
    alert_engine.py transforme un RiskBreach en Alert (ajout de alert_id,
    message pré-généré, triggered_at) avant écriture en base via
    DuckDBRepository.insert_alerts().
    """

    ticker: str | None        # None pour les alertes portefeuille global
    alert_type: AlertType
    severity: Severity
    value: float
    threshold: float
    date: str                  # date de cotation associée (format ISO)
    label: str | None = None   # contexte additionnel (ex: nom du secteur
                                # pour SECTOR_CONCENTRATION) — optionnel,
                                # utilisé uniquement par alert_engine.py
                                # pour construire un message lisible.


class Alert(BaseModel):
    """
    Alerte persistée — cf. doc Alertes (Système d'alertes configurables).
    Correspond 1:1 aux colonnes de la table DuckDB `alerts`.
    """

    alert_id: str
    ticker: str | None
    alert_type: AlertType
    severity: Severity
    value: float
    threshold: float
    triggered_at: datetime
    message: str