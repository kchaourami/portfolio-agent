"""
Rôle :
- Transformer les RiskBreach (sortie de risk_calculator.py) en objets Alert complets
- Écrire les nouvelles alertes en base via DuckDBRepository.insert_alerts()

Aucun calcul de seuil ici — c'est le rôle de risk_calculator.py. Ce module
se contente de mettre en forme et de persister ce qui a déjà été détecté.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime

import pandas as pd

from app.pipeline.risk_calculator import run_risk_calculator
from app.pipeline.risk_models import Alert, AlertType, RiskBreach
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


# Génération de l'alert_id pour éviter les doublons

def _generate_alert_id(breach: RiskBreach) -> str:

    #ID déterministe basé sur (date, ticker, type, label).

    raw = f"{breach.date}_{breach.ticker or 'PORTFOLIO'}_{breach.alert_type.value}_{breach.label or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _build_message(breach: RiskBreach) -> str:
    #Construit un message court et factuel à partir d'un RiskBreach.

    if breach.alert_type == AlertType.PRICE_DROP:
        return (
            f"{breach.ticker} : baisse journalière de {_pct(breach.value)} "
            f"(seuil {_pct(breach.threshold)})"
        )

    if breach.alert_type == AlertType.ABNORMAL_VOLUME:
        return (
            f"{breach.ticker} : volume à {breach.value:.1f}x la moyenne 20j "
            f"(seuil {breach.threshold:.1f}x)"
        )

    if breach.alert_type == AlertType.UNDERPERFORMANCE:
        return (
            f"{breach.ticker} : sous-performance de {_pct(breach.value)} "
            f"vs CAC 40 sur 5j (seuil {_pct(breach.threshold)})"
        )

    if breach.alert_type == AlertType.HIGH_VOLATILITY:
        return (
            f"{breach.ticker} : volatilité 20j à {_pct(breach.value)} "
            f"(seuil {_pct(breach.threshold)})"
        )

    if breach.alert_type == AlertType.PORTFOLIO_DRAWDOWN:
        return f"Portefeuille : drawdown de {_pct(breach.value)} (seuil {_pct(breach.threshold)})"

    if breach.alert_type == AlertType.SECTOR_CONCENTRATION:
        sector = breach.label or "secteur inconnu"
        return (
            f"Concentration sectorielle ({sector}) à {_pct(breach.value)} "
            f"(seuil {_pct(breach.threshold)})"
        )

    return f"{breach.alert_type.value} détecté | valeur={breach.value} | seuil={breach.threshold}"


# Construction des Alert

def build_alert(breach: RiskBreach) -> Alert:
    #Transforme un RiskBreach en Alert complète, prête à être persistée.
    return Alert(
        alert_id=_generate_alert_id(breach),
        ticker=breach.ticker,
        alert_type=breach.alert_type,
        severity=breach.severity,
        value=breach.value,
        threshold=breach.threshold,
        triggered_at=datetime.now(),
        message=_build_message(breach),
    )


def build_alerts(breaches: list[RiskBreach]) -> list[Alert]:
    #Transforme une liste de RiskBreach en liste d'Alert.
    return [build_alert(b) for b in breaches]

def persist_alerts(alerts: list[Alert], repo: DuckDBRepository) -> int:

    #ecrire les alertes en base.

    if not alerts:
        logger.info("persist_alerts: aucune alerte à écrire")
        return 0

    df = pd.DataFrame(
        [
            {
                "alert_id": a.alert_id,
                "ticker": a.ticker,
                "alert_type": a.alert_type.value,
                "severity": a.severity.value,
                "value": a.value,
                "threshold": a.threshold,
                "message": a.message,
                "triggered_at": a.triggered_at,
            }
            for a in alerts
        ]
    )

    return repo.insert_alerts(df)

# Orchestration

def run_alert_engine(repo: DuckDBRepository) -> list[Alert]:

    #Point d'entrée utilisé par l'orchestrateur (LangGraph).
    #Enchaîne : calcul des breaches (risk_calculator) → mise en forme (Alert)
    #→ persistance (DuckDB). Retourne les Alert traitées (créées ou déjà
    #existantes en base) pour permettre un logging immédiat côté orchestrateur.

    breaches = run_risk_calculator(repo)
    alerts = build_alerts(breaches)
    written = persist_alerts(alerts, repo)

    logger.info(
        "run_alert_engine terminé | alertes traitées=%d | écrites=%d",
        len(alerts),
        written,
    )
    return alerts

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with DuckDBRepository() as repo:
        alerts = run_alert_engine(repo)

        print(f"\n✓ {len(alerts)} alerte(s) traitée(s)\n")
        for a in alerts:
            print(f"  [{a.severity.value.upper()}] {a.message}")