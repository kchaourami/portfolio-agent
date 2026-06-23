"""
graph.py
=========
Emplacement cible : app/orchestration/graph.py

Orchestrateur LangGraph — enchaîne les pipelines déterministes déjà
construits :

    data_node ──> macro_node ──> dbt_run_node ──> alert_node   ──┐
                                                 └─> regime_node  ──┴──> END

Chaque nœud est une fonction Python simple qui appelle un pipeline déjà
existant — AUCUNE logique métier nouvelle ici, et surtout aucun appel LLM.
Seul un futur nœud "analyst_node" (à venir) fera un appel à l'API Anthropic.

data_node et macro_node sont SÉQUENTIELS (pas parallèles) — les deux font
des appels réseau vers des API gratuites sans SLA garanti, et la parallé-
lisation s'est révélée moins fiable en test réel (plus de timeouts). Une
fois dbt_run_node terminé, alert_node et regime_node, eux, tournent en
parallèle sans risque puisqu'ils ne lisent que DuckDB en local.
"""

from __future__ import annotations

import logging
import operator
import subprocess
from pathlib import Path
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from app.config.settings import settings
from app.pipeline.alert_engine import run_alert_engine
from app.pipeline.data_collector import DataCollector
from app.pipeline.macro_collector import run_macro_collector
from app.pipeline.macro_regime import MacroRegime, get_current_macro_regime
from app.pipeline.risk_models import Alert
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


class PipelineState(TypedDict, total=False):
    """
    État partagé entre les nœuds du graphe.

    'errors' utilise un reducer (Annotated[..., operator.add]) parce que
    alert_node et regime_node tournent en PARALLÈLE — sans reducer, si les
    deux échouaient en même temps, le second écraserait l'erreur du
    premier au lieu de l'accumuler. Avec operator.add, LangGraph concatène
    les listes retournées par chaque nœud automatiquement.
    """

    market_rows: int
    macro_rows: int
    dbt_success: bool
    alerts: list[Alert]
    macro_regime: MacroRegime | None
    errors: Annotated[list[str], operator.add]


# ---------------------------------------------------------------------------
# Nœuds
# ---------------------------------------------------------------------------

def data_node(state: PipelineState) -> PipelineState:
    """Collecte les prix de marché (yfinance) et les écrit en base."""
    logger.info("[graph] data_node démarré")
    try:
        collector = DataCollector()
        df = collector.collect_market_data()
        with DuckDBRepository() as repo:
            written = repo.upsert_prices(df)
        return {"market_rows": written}
    except Exception as exc:
        logger.exception("[graph] data_node a échoué")
        return {"market_rows": 0, "errors": [f"data_node: {exc}"]}


def macro_node(state: PipelineState) -> PipelineState:
    """Collecte les indicateurs macro (BCE, INSEE/Eurostat) et les écrit en base."""
    logger.info("[graph] macro_node démarré")
    try:
        with DuckDBRepository() as repo:
            written = run_macro_collector(repo)
        return {"macro_rows": written}
    except Exception as exc:
        logger.exception("[graph] macro_node a échoué")
        return {"macro_rows": 0, "errors": [f"macro_node: {exc}"]}


def dbt_run_node(state: PipelineState) -> PipelineState:
    """
    Lance `dbt run` — recalcule staging + marts à partir des données
    fraîchement collectées. Ne s'exécute qu'une fois data_node ET
    macro_node terminés (synchronisation native LangGraph).
    """
    logger.info("[graph] dbt_run_node démarré")
    dbt_dir = Path(settings.DBT_PROJECT_DIR)

    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logger.error("[graph] dbt run a échoué :\n%s", result.stdout + result.stderr)
        return {"dbt_success": False, "errors": [f"dbt_run_node: {result.stdout[-500:]}"]}

    logger.info("[graph] dbt run terminé avec succès")
    return {"dbt_success": True}


def alert_node(state: PipelineState) -> PipelineState:
    """Calcule les risques et persiste les alertes — seulement si dbt a réussi."""
    logger.info("[graph] alert_node démarré")

    if not state.get("dbt_success"):
        logger.warning("[graph] alert_node ignoré (dbt run a échoué)")
        return {"alerts": []}

    try:
        with DuckDBRepository() as repo:
            alerts = run_alert_engine(repo)
        return {"alerts": alerts}
    except Exception as exc:
        logger.exception("[graph] alert_node a échoué")
        return {"alerts": [], "errors": [f"alert_node: {exc}"]}


def regime_node(state: PipelineState) -> PipelineState:
    """Calcule le régime macro — seulement si dbt a réussi."""
    logger.info("[graph] regime_node démarré")

    if not state.get("dbt_success"):
        logger.warning("[graph] regime_node ignoré (dbt run a échoué)")
        return {"macro_regime": None}

    try:
        with DuckDBRepository() as repo:
            regime = get_current_macro_regime(repo)
        return {"macro_regime": regime}
    except Exception as exc:
        logger.exception("[graph] regime_node a échoué")
        return {"macro_regime": None, "errors": [f"regime_node: {exc}"]}


# ---------------------------------------------------------------------------
# Construction du graphe
# ---------------------------------------------------------------------------

def build_graph():
    """Construit et compile le graphe LangGraph (data → macro → dbt → alert/regime en parallèle)."""
    graph = StateGraph(PipelineState)

    graph.add_node("data_node", data_node)
    graph.add_node("macro_node", macro_node)
    graph.add_node("dbt_run_node", dbt_run_node)
    graph.add_node("alert_node", alert_node)
    graph.add_node("regime_node", regime_node)

    # data_node puis macro_node en SÉQUENTIEL (pas en parallèle) : les deux
    # font des appels réseau externes vers des API gratuites sans SLA
    # garanti (yfinance, BCE, INSEE). En parallèle, on a observé en test
    # réel (23/06/2026, 2 runs) plus de timeouts/échecs de métadonnées
    # qu'en séquentiel — la contention réseau simultanée semble dégrader
    # la fiabilité plus qu'elle n'apporte de gain de vitesse significatif
    # pour un run quotidien. alert_node/regime_node restent en parallèle
    # ci-dessous : ils ne lisent que DuckDB en local, aucun risque réseau.
    graph.add_edge(START, "data_node")
    graph.add_edge("data_node", "macro_node")
    graph.add_edge("macro_node", "dbt_run_node")

    # Fan-out vers les 2 branches finales (parallèle — sûr ici, pas de réseau)
    graph.add_edge("dbt_run_node", "alert_node")
    graph.add_edge("dbt_run_node", "regime_node")

    graph.add_edge("alert_node", END)
    graph.add_edge("regime_node", END)

    return graph.compile()


# ---------------------------------------------------------------------------
# Test rapide — python -m app.orchestration.graph
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    app_graph = build_graph()
    final_state = app_graph.invoke({})

    print("\n=== Résultat du run ===")
    print(f"Lignes marché collectées : {final_state.get('market_rows')}")
    print(f"Lignes macro collectées  : {final_state.get('macro_rows')}")
    print(f"dbt run réussi           : {final_state.get('dbt_success')}")
    print(f"Alertes traitées         : {len(final_state.get('alerts', []))}")

    regime = final_state.get("macro_regime")
    print(f"Régime macro             : {regime.regime if regime else 'N/A'}")

    if final_state.get("errors"):
        print(f"\n⚠️  Erreurs rencontrées :")
        for err in final_state["errors"]:
            print(f"  - {err}")