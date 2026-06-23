"""
prompt_builder.py
==================
Emplacement cible : app/agents/analyst/prompt_builder.py

Construit le prompt structuré de l'Agent Analyste — format exact défini
dans la doc "LLM : Construction des prompts". AUCUNE donnée brute n'est
jamais transmise au LLM : tout ce qui suit a déjà été calculé de façon
déterministe par les pipelines (mart_portfolio_value, alerts,
macro_regime.py). Ce module ne fait aucun appel LLM — uniquement de la
mise en forme de texte.
"""

from __future__ import annotations

import pandas as pd

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
5. Recommandations : 2 à 4 recommandations concrètes et actionnables,
   directement liées aux signaux et chiffres ci-dessus (ex: réduire
   l'exposition à un titre ou secteur en alerte, surveiller un
   indicateur précis, ajuster l'allocation au vu du contexte macro
   et apres vous me dites il vaut mieux vendre ou acheter, etc.).
    Chaque recommandation doit être justifiée par un ou plusieurs chiffres précis du contexte ci-dessus —

Règles absolues :
— Ne cite que des données fournies dans ce contexte, jamais d'inventions
— Reste factuel, concis, professionnel
— Formule en français"""


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
# (même principe d'approximation que check_portfolio_drawdown dans
# risk_calculator.py : pas un vrai NAV historique, mais cohérent et
# documenté comme tel — cf. limitation déjà actée dans le mémoire)
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
    triée par poids décroissant.
 
    Sert à donner à l'agent la correspondance ticker → secteur dont il a
    besoin pour relier une alerte SECTOR_CONCENTRATION (qui ne mentionne
    que le nom du secteur) à un ticker précis dans ses recommandations —
    sans ce bloc, l'agent n'aurait aucune base pour deviner quel ticker
    cause la concentration, et risquerait soit de ne pas répondre à la
    question, soit pire, d'inventer un ticker au hasard.
    """
    if df_portfolio.empty:
        return "Aucune position en portefeuille."
 
    df = df_portfolio.dropna(subset=["weight"]).sort_values("weight", ascending=False)
 
    lines = []
    for _, row in df.iterrows():
        sector = row["sector"] if pd.notna(row["sector"]) else "Non classé (ETF)"
        weight_pct = row["weight"] * 100
        lines.append(f"- {row['ticker']} ({sector}) : {weight_pct:.1f}% du portefeuille")
 
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Construction du prompt
# ---------------------------------------------------------------------------

def build_analyst_prompt(
    df_portfolio: pd.DataFrame,
    alerts: list[Alert],
    regime: MacroRegime,
) -> str:
    """Assemble le prompt structuré complet à partir des 3 contextes déjà calculés."""
    summary = compute_portfolio_summary(df_portfolio)

    return PROMPT_TEMPLATE.format(
        total_value=_fmt_eur(summary["total_value"]),
        daily_perf=_fmt_pct(summary["daily_perf"]),
        relative_perf=_fmt_pct(summary["relative_perf"]),
        drawdown=_fmt_pct(summary["drawdown"]),
        composition_list=format_composition(df_portfolio),
        signals_list=format_signals(alerts),
        macro_regime=regime.regime,
        ecb_rate=_fmt_pct_already(regime.ecb_rate),
        inflation=_fmt_pct_already(regime.inflation_fr),
        eurusd=f"{regime.eurusd:.4f}" if regime.eurusd is not None else "N/A",
        macro_drivers="; ".join(regime.drivers) if regime.drivers else "Aucun driver disponible",
    )


def build_prompt_from_db(repo: DuckDBRepository) -> tuple[str, list[Alert]]:
    """
    Point d'entrée pratique : lit tout depuis DuckDB (mart_portfolio_value,
    alertes non lues, régime macro) et construit le prompt complet.
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
    prompt = build_analyst_prompt(df_portfolio, alerts, regime)
    return prompt, alerts


# ---------------------------------------------------------------------------
# Test rapide — python -m app.agents.analyst.prompt_builder
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with DuckDBRepository() as repo:
        prompt, alerts = build_prompt_from_db(repo) 
        print(prompt)
        print(f"\n--- {len(alerts)} alerte(s) incluse(s) dans ce prompt ---")