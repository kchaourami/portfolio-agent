"""
prompt_builder.py
==================
Emplacement cible : app/agents/analyst/prompt_builder.py

Construit le prompt structuré de l'Agent Analyste. AUCUNE donnée brute
n'est jamais transmise au LLM : tout ce qui suit a déjà été calculé de
façon déterministe par les pipelines (mart_portfolio_value, alerts,
macro_regime.py, decision_engine.py). Ce module ne fait aucun appel
LLM — uniquement de la mise en forme de texte.

CHANGEMENT IMPORTANT (Decision Engine) : la partie 5 du prompt ne demande
plus au LLM de GÉNÉRER des recommandations — les décisions
(BUY_WATCH/HOLD/WATCH/REDUCE/SELL_SIGNAL/INCREASE) sont désormais déjà
calculées par decision_engine.py (entièrement déterministe) et fournies
dans le bloc [DÉCISIONS PROPOSÉES]. Le rôle du LLM se limite à les
EXPLIQUER en langage naturel, jamais à les recalculer ou les contredire.
"""

from __future__ import annotations

import pandas as pd

from app.pipeline.decision_engine import compute_decisions
from app.pipeline.decision_models import TickerDecision
from app.pipeline.macro_regime import MacroRegime, get_current_macro_regime
from app.pipeline.risk_models import Alert
from app.storage.duckdb_repository import DuckDBRepository

PROMPT_TEMPLATE = """[PORTEFEUILLE]
Valeur totale : {total_value} €
Performance du jour : {daily_perf}%
Comparaison CAC 40 : {relative_perf}%
Drawdown actuel : {drawdown}%

[COMPOSITION]
{composition_list}

[SIGNAUX DÉTECTÉS]
{signals_list}

[DÉCISIONS PROPOSÉES]
{decisions_list}

[CONTEXTE MACRO]
Régime : {macro_regime}
Taux BCE : {ecb_rate}% | Inflation FR : {inflation}% | EUR/USD : {eurusd}
Principaux drivers : {macro_drivers}

[INSTRUCTION]
Produis une analyse en 5 parties :
1. Résumé (2 phrases max)
2. Signaux à surveiller (bullet points, données uniquement)
3. Contexte macro et son impact potentiel sur le portefeuille
4. Niveau de vigilance global : Vert / Orange / Rouge avec justification
5. Explication des décisions : pour chaque décision listée dans
   [DÉCISIONS PROPOSÉES], explique en 1-2 phrases pourquoi elle a été
   prise, en t'appuyant uniquement sur les raisons fournies pour ce
   ticker. Tu peux nuancer la FORMULATION de la recommandation en langage naturel orienté
   achat/vente (ex: "alléger progressivement la position", "profiter
   de la dynamique pour renforcer") — mais jamais le SENS de la
   décision elle-même.

Règles absolues :
— Ne cite que des données fournies dans ce contexte, jamais d'inventions
— Reste factuel, concis, professionnel
— Formule en français
— Ne nomme un ticker ou un secteur que s'il apparaît déjà explicitement
  dans [COMPOSITION] ou [SIGNAUX DÉTECTÉS] — n'invente jamais une
  caractéristique du portefeuille qui ne figure dans aucun des deux
— Les décisions (BUY_WATCH / HOLD / WATCH / REDUCE / SELL_SIGNAL /
  INCREASE) sont déjà calculées dans [DÉCISIONS PROPOSÉES] par un
  moteur de règles déterministe. Tu peux nuancer leur FORMULATION
  (intensité, vocabulaire d'achat/vente), mais jamais leur SENS : ne
  recalcule jamais une décision, ne la contredis jamais, et ne propose
  jamais une catégorie de décision différente de celle fournie (ex:
  jamais suggérer un achat pour une décision REDUCE ou SELL_SIGNAL)"""


# ---------------------------------------------------------------------------
# Formatage — chaque fonction gère le cas "donnée manquante" explicitement
# (jamais de valeur par défaut inventée, on affiche "N/A")
# ---------------------------------------------------------------------------

def _fmt_pct(value: float | None) -> str:
    """Formate un ratio décimal (0.031) en pourcentage signé (+3.10)."""
    return f"{value * 100:+.2f}" if value is not None else "N/A"


def _fmt_pct_already(value: float | None) -> str:
    """Formate une valeur déjà en %, pas en décimal (cas de macro_regime)."""
    return f"{value:.2f}" if value is not None else "N/A"


def _fmt_eur(value: float | None) -> str:
    """Formate un montant en euros avec séparateur de milliers (espace)."""
    return f"{value:,.2f}".replace(",", " ") if value is not None else "N/A"


# ---------------------------------------------------------------------------
# Agrégats portefeuille — moyennes pondérées par le poids de chaque ligne
# ---------------------------------------------------------------------------

def compute_portfolio_summary(df_portfolio: pd.DataFrame) -> dict:
    """
    Calcule les agrégats nécessaires au bloc [PORTEFEUILLE] du prompt.
    Retourne None pour chaque champ si la donnée sous-jacente est absente
    — jamais une valeur inventée pour combler un trou.
    """
    if df_portfolio.empty:
        return {"total_value": None, "daily_perf": None, "relative_perf": None, "drawdown": None}

    def _weighted(col: str) -> float | None:
        sub = df_portfolio.dropna(subset=[col, "weight"])
        if sub.empty:
            return None
        return float((sub[col] * sub["weight"]).sum())

    return {
        "total_value": float(df_portfolio["market_value"].sum()),
        "daily_perf": _weighted("daily_return"),
        "relative_perf": _weighted("relative_perf_5d"),
        "drawdown": _weighted("drawdown"),
    }


def format_signals(alerts: list[Alert]) -> str:
    """
    Formate la liste des signaux actifs — réutilise le champ `message`
    déjà construit par alert_engine.py (déjà factuel, déjà en français,
    pas besoin de reformater le contenu).
    """
    if not alerts:
        return "Aucun signal actif."
    return "\n".join(f"- {alert.message}" for alert in alerts)


def format_composition(df_portfolio: pd.DataFrame) -> str:
    """
    Liste ticker / secteur / poids pour chaque ligne du portefeuille,
    triée par poids décroissant — permet à l'agent de relier une alerte
    de secteur à un ticker précis sans inventer.

    Distingue explicitement "vrai ETF sans secteur" (asset_type='etf')
    de "action dont le secteur est temporairement indisponible"
    (yfinance échoue régulièrement sur la récupération des métadonnées,
    observé à plusieurs reprises dans ce projet) — les deux cas
    affichaient à tort "(ETF)" auparavant, ce qui aurait pu laisser
    croire à l'agent qu'une action comme AIR.PA ou BNP.PA était un ETF.
    """
    if df_portfolio.empty:
        return "Aucune position en portefeuille."

    df = df_portfolio.dropna(subset=["weight"]).sort_values("weight", ascending=False)

    lines = []
    for _, row in df.iterrows():
        if pd.notna(row.get("sector")):
            sector_label = row["sector"]
        elif row.get("asset_type") == "etf":
            sector_label = "ETF (non sectorisé)"
        else:
            sector_label = "secteur non disponible"

        weight_pct = row["weight"] * 100
        lines.append(f"- {row['ticker']} ({sector_label}) : {weight_pct:.1f}% du portefeuille")

    return "\n".join(lines)


def format_decisions(decisions: list[TickerDecision]) -> str:
    """
    Formate les décisions structurées du Decision Engine — code anglais
    (référence standard) + libellé français + niveau de confiance +
    raisons. L'agent doit EXPLIQUER ces décisions, jamais en inventer.
    """
    if not decisions:
        return "Aucune décision disponible."

    lines = []
    for d in decisions:
        reasons_str = "; ".join(d.reasons)
        lines.append(
            f"- {d.ticker} : {d.decision.value} ({d.decision_label_fr}) "
            f"— confiance {d.confidence_score}/100 — {reasons_str}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Construction du prompt
# ---------------------------------------------------------------------------

def build_analyst_prompt(
    df_portfolio: pd.DataFrame,
    alerts: list[Alert],
    regime: MacroRegime,
    decisions: list[TickerDecision] | None = None,
) -> str:
    """Assemble le prompt structuré complet à partir des contextes déjà calculés."""
    summary = compute_portfolio_summary(df_portfolio)
    decisions = decisions or []

    return PROMPT_TEMPLATE.format(
        total_value=_fmt_eur(summary["total_value"]),
        daily_perf=_fmt_pct(summary["daily_perf"]),
        relative_perf=_fmt_pct(summary["relative_perf"]),
        drawdown=_fmt_pct(summary["drawdown"]),
        composition_list=format_composition(df_portfolio),
        signals_list=format_signals(alerts),
        decisions_list=format_decisions(decisions),
        macro_regime=regime.regime,
        ecb_rate=_fmt_pct_already(regime.ecb_rate),
        inflation=_fmt_pct_already(regime.inflation_fr),
        eurusd=f"{regime.eurusd:.4f}" if regime.eurusd is not None else "N/A",
        macro_drivers="; ".join(regime.drivers) if regime.drivers else "Aucun driver disponible",
    )


def build_prompt_from_db(repo: DuckDBRepository) -> tuple[str, list[Alert]]:
    """
    Point d'entrée pratique : lit tout depuis DuckDB (mart_portfolio_value,
    alertes non lues, régime macro) et calcule les décisions à la volée
    (compute_decisions — pur calcul, ne persiste rien ici ; la
    persistance a lieu via decision_node dans le graphe orchestré).
    """
    df_portfolio = repo.execute_query("SELECT * FROM main_marts.mart_portfolio_value")

    alerts_df = repo.fetch_alerts(unread_only=True)
    alerts = [
        Alert(
            alert_id=row["alert_id"],
            ticker=row["ticker"],
            alert_type=row["alert_type"],
            severity=row["severity"],
            value=row["value"],
            threshold=row["threshold"],
            triggered_at=row["triggered_at"],
            message=row["message"],
        )
        for _, row in alerts_df.iterrows()
    ]

    regime = get_current_macro_regime(repo)
    decisions = compute_decisions(repo)

    prompt = build_analyst_prompt(df_portfolio, alerts, regime, decisions)
    return prompt, alerts


# ---------------------------------------------------------------------------
# Test rapide — python -m app.agents.analyst.prompt_builder
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with DuckDBRepository() as repo:
        prompt, alerts = build_prompt_from_db(repo)
        print(prompt)
        print(f"\n--- {len(alerts)} alerte(s) incluse(s) dans ce prompt ---")