"""
graph.py
=========
Emplacement cible : app/orchestration/graph.py

Orchestrateur LangGraph — enchaîne les pipelines déterministes déjà
construits :

    data_node ──> macro_node ──> dbt_run_node ──┬─> alert_node    ──┐
                                                 ├─> regime_node   ──┼──> analyst_node ──> END
                                                 └─> decision_node ──┘

Règle importante :
- data_node, macro_node, dbt_run_node, alert_node, regime_node et
  decision_node sont déterministes.
- analyst_node est le seul nœud qui appelle le LLM.
- decision_node tourne en parallèle d'alert_node/regime_node — il ne
  dépend que de dbt_run_node (lit mart_portfolio_value directement, pas
  la table alerts) et ne fait aucun appel réseau, donc la parallélisation
  est sûre (même raisonnement que pour alert_node/regime_node).

NOTE — séquencement avec l'étape 6 (à venir) : ce nœud calcule ET
persiste les décisions (table `decisions`), mais elles ne sont pas
encore transmises à l'Agent Analyste ici (analyst_node ne passe pas
encore `decisions` à run_analyst_agent) — c'est l'objet de la prochaine
étape, qui consiste à modifier analyst_node + run_analyst_agent pour
relayer state["decisions"] jusqu'au prompt.
"""

from __future__ import annotations

import logging
import operator
import subprocess
from pathlib import Path
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from app.agents.analyst.analyst_agent import run_analyst_agent
from app.config.settings import settings
from app.pipeline.alert_engine import run_alert_engine
from app.pipeline.data_collector import DataCollector
from app.pipeline.decision_engine import run_decision_engine
from app.pipeline.decision_models import TickerDecision
from app.pipeline.macro_collector import run_macro_collector
from app.pipeline.macro_regime import MacroRegime, get_current_macro_regime
from app.pipeline.risk_models import Alert
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


class PipelineState(TypedDict, total=False):
    """
    État partagé entre les nœuds du graphe.

    errors utilise un reducer car alert_node, regime_node et
    decision_node tournent en parallèle. Avec operator.add, LangGraph
    concatène les erreurs au lieu de les écraser.
    """

    market_rows: int
    macro_rows: int
    dbt_success: bool
    alerts: list[Alert]
    macro_regime: MacroRegime | None
    decisions: list[TickerDecision]
    analyst_report: str | None
    errors: Annotated[list[str], operator.add]


# ---------------------------------------------------------------------------
# Nœuds
# ---------------------------------------------------------------------------

def data_node(state: PipelineState) -> PipelineState:
    """Collecte les prix de marché avec yfinance et les écrit dans DuckDB."""
    logger.info("[graph] data_node démarré")

    try:
        collector = DataCollector()
        df = collector.collect_market_data()

        with DuckDBRepository() as repo:
            written = repo.upsert_prices(df)

        logger.info("[graph] data_node terminé | lignes=%s", written)
        return {"market_rows": written}

    except Exception as exc:
        logger.exception("[graph] data_node a échoué")
        return {
            "market_rows": 0,
            "errors": [f"data_node: {exc}"],
        }


def macro_node(state: PipelineState) -> PipelineState:
    """Collecte les indicateurs macro et les écrit dans DuckDB."""
    logger.info("[graph] macro_node démarré")

    try:
        with DuckDBRepository() as repo:
            written = run_macro_collector(repo)

        logger.info("[graph] macro_node terminé | lignes=%s", written)
        return {"macro_rows": written}

    except Exception as exc:
        logger.exception("[graph] macro_node a échoué")
        return {
            "macro_rows": 0,
            "errors": [f"macro_node: {exc}"],
        }


def dbt_run_node(state: PipelineState) -> PipelineState:
    """
    Lance dbt run.

    Recalcule les vues staging et les tables marts à partir des données
    fraîchement collectées dans DuckDB.
    """
    logger.info("[graph] dbt_run_node démarré")

    dbt_dir = Path(settings.DBT_PROJECT_DIR)

    try:
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "."],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            output = result.stdout + result.stderr
            logger.error("[graph] dbt run a échoué :\n%s", output)

            return {
                "dbt_success": False,
                "errors": [f"dbt_run_node: {output[-800:]}"],
            }

        logger.info("[graph] dbt run terminé avec succès")
        return {"dbt_success": True}

    except Exception as exc:
        logger.exception("[graph] dbt_run_node a échoué")
        return {
            "dbt_success": False,
            "errors": [f"dbt_run_node: {exc}"],
        }


def alert_node(state: PipelineState) -> PipelineState:
    """Calcule les risques et persiste les alertes si dbt a réussi."""
    logger.info("[graph] alert_node démarré")

    if not state.get("dbt_success"):
        logger.warning("[graph] alert_node ignoré car dbt run a échoué")
        return {"alerts": []}

    try:
        with DuckDBRepository() as repo:
            alerts = run_alert_engine(repo)

        logger.info("[graph] alert_node terminé | alertes=%s", len(alerts))
        return {"alerts": alerts}

    except Exception as exc:
        logger.exception("[graph] alert_node a échoué")
        return {
            "alerts": [],
            "errors": [f"alert_node: {exc}"],
        }


def regime_node(state: PipelineState) -> PipelineState:
    """Calcule le régime macro si dbt a réussi."""
    logger.info("[graph] regime_node démarré")

    if not state.get("dbt_success"):
        logger.warning("[graph] regime_node ignoré car dbt run a échoué")
        return {"macro_regime": None}

    try:
        with DuckDBRepository() as repo:
            regime = get_current_macro_regime(repo)

        logger.info(
            "[graph] regime_node terminé | régime=%s",
            regime.regime if regime else "N/A",
        )

        return {"macro_regime": regime}

    except Exception as exc:
        logger.exception("[graph] regime_node a échoué")
        return {
            "macro_regime": None,
            "errors": [f"regime_node: {exc}"],
        }


def decision_node(state: PipelineState) -> PipelineState:
    """
    Calcule les décisions structurées par ticker (Decision Engine) et les
    persiste dans la table `decisions`, si dbt a réussi.

    Tourne en parallèle d'alert_node/regime_node : ne dépend que de
    dbt_run_node (lit mart_portfolio_value directement, jamais la table
    alerts), aucun appel réseau — parallélisation sûre, même raisonnement
    que pour alert_node/regime_node.
    """
    logger.info("[graph] decision_node démarré")

    if not state.get("dbt_success"):
        logger.warning("[graph] decision_node ignoré car dbt run a échoué")
        return {"decisions": []}

    try:
        with DuckDBRepository() as repo:
            decisions = run_decision_engine(repo)

        logger.info("[graph] decision_node terminé | décisions=%s", len(decisions))
        return {"decisions": decisions}

    except Exception as exc:
        logger.exception("[graph] decision_node a échoué")
        return {
            "decisions": [],
            "errors": [f"decision_node: {exc}"],
        }


def analyst_node(state: PipelineState) -> PipelineState:
    """
    Génère la synthèse finale via l'Agent Analyste.

    Ce nœud est le seul endroit du graphe où un appel LLM peut être fait.
    Il s'exécute après alert_node, regime_node et decision_node.

    NOTE : decisions n'est pas encore transmis à run_analyst_agent ici —
    c'est l'objet de la prochaine étape (modifier run_analyst_agent pour
    accepter et utiliser ce paramètre).
    """
    logger.info("[graph] analyst_node démarré")

    if not state.get("dbt_success"):
        logger.warning("[graph] analyst_node ignoré car dbt run a échoué")
        return {"analyst_report": None}

    try:
        with DuckDBRepository() as repo:
            report = run_analyst_agent(
                repo=repo,
                alerts=state.get("alerts", []),
                decisions=state.get("decisions", []), 
            )

        logger.info("[graph] analyst_node terminé")
        return {"analyst_report": report}

    except Exception as exc:
        logger.exception("[graph] analyst_node a échoué")
        return {
            "analyst_report": None,
            "errors": [f"analyst_node: {exc}"],
        }


# ---------------------------------------------------------------------------
# Construction du graphe
# ---------------------------------------------------------------------------

def build_graph():
    """
    Construit et compile le graphe LangGraph.

    Flux :
        data → macro → dbt → (alert / regime / decision) → analyst
    """
    graph = StateGraph(PipelineState)

    graph.add_node("data_node", data_node)
    graph.add_node("macro_node", macro_node)
    graph.add_node("dbt_run_node", dbt_run_node)
    graph.add_node("alert_node", alert_node)
    graph.add_node("regime_node", regime_node)
    graph.add_node("decision_node", decision_node)
    graph.add_node("analyst_node", analyst_node)

    # Partie séquentielle : appels réseau externes
    graph.add_edge(START, "data_node")
    graph.add_edge("data_node", "macro_node")
    graph.add_edge("macro_node", "dbt_run_node")

    # Fan-out après dbt : 3 branches locales en parallèle
    graph.add_edge("dbt_run_node", "alert_node")
    graph.add_edge("dbt_run_node", "regime_node")
    graph.add_edge("dbt_run_node", "decision_node")

    # Fan-in : analyst_node attend les 3 branches
    graph.add_edge(["alert_node", "regime_node", "decision_node"], "analyst_node")

    graph.add_edge("analyst_node", END)

    return graph.compile()


# ---------------------------------------------------------------------------
# Test rapide — python -m app.orchestration.graph
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    app_graph = build_graph()
    final_state = app_graph.invoke({"errors": []})

    print("\n=== Résultat du run ===")
    print(f"Lignes marché collectées : {final_state.get('market_rows')}")
    print(f"Lignes macro collectées  : {final_state.get('macro_rows')}")
    print(f"dbt run réussi           : {final_state.get('dbt_success')}")
    print(f"Alertes traitées         : {len(final_state.get('alerts', []))}")
    print(f"Décisions calculées      : {len(final_state.get('decisions', []))}")

    regime = final_state.get("macro_regime")
    print(f"Régime macro             : {regime.regime if regime else 'N/A'}")

    print("\n=== Synthèse Agent Analyste ===")
    analyst_report = final_state.get("analyst_report")
    print(analyst_report or "Aucune synthèse générée.")

    if final_state.get("errors"):
        print("\nErreurs rencontrées :")
        for err in final_state["errors"]:
            print(f"  - {err}")