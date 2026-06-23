"""
analyst_agent.py
==================
Emplacement cible : app/agents/analyst/analyst_agent.py

Agent Analyste — le SEUL agent du projet qui fait un appel LLM. Reçoit un
prompt structuré déjà construit par prompt_builder.py (aucune donnée
brute) et produit une synthèse en langage naturel.

Règle absolue (cf. doc LLM : Construction des prompts) : le LLM ne reçoit
jamais de données brutes et ne doit jamais inventer de chiffres — tout ce
qu'il commente a déjà été calculé de façon déterministe par les pipelines
en amont (Agent Data, Agent Risk, Agent Macro).

---------------------------------------------------------------------------
CHOIX TECHNIQUE — Gemini, API generateContent (pas Interactions API)
---------------------------------------------------------------------------
Package requis : `google-genai` (pip install google-genai). Le package
`google-generativeai` est déprécié, à ne plus utiliser.

Gemini propose deux façons d'appeler le modèle :
- Interactions API : nouvelle, optimisée pour les workflows agentiques
  multi-tours avec état côté serveur — actuellement en bêta/preview
- generateContent : l'API "classique", stable, recommandée explicitement
  par Google pour les déploiements de production

Notre besoin est un seul appel sans état (un prompt → une réponse), donc
generateContent est le bon choix — pas besoin de la gestion de
conversation multi-tours qu'apporte l'Interactions API.
"""

from __future__ import annotations

import logging
import time

from google import genai
from google.genai import types

from app.agents.analyst.prompt_builder import build_prompt_from_db
from app.config.settings import settings
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Tu es l'Agent Analyste d'un système de surveillance de portefeuille "
    "boursier français. Tu reçois un contexte structuré déjà calculé en "
    "amont — ne cite jamais de donnée qui ne figure pas explicitement "
    "dans ce contexte, et n'invente jamais de chiffre. Réponds en "
    "français, de façon factuelle, concise et professionnelle, en "
    "suivant exactement le format demandé dans la section [INSTRUCTION] "
    "du message."
)

NOT_ADVICE_DISCLAIMER = (
    "Cette synthèse est générée automatiquement à partir de données de "
    "marché et constitue un conseil en investissement personnalisé."
)


def generate_synthesis(
    prompt: str,
    history: list[dict] | None = None,
    retries: int = 2,
    backoff_seconds: float = 5.0,
) -> str:
    """
    Appelle l'API Gemini (generateContent), avec re-essai en cas d'erreur
    serveur transitoire (ex: 503 "high demand" — observé en test réel le
    23/06/2026 sur gemini-3.5-flash, modèle tout juste sorti et donc
    probablement sous forte charge). Le SDK Google retente déjà une fois
    en interne (tenacity), mais pas toujours assez longtemps pour absorber
    un pic de charge — cette couche ajoute une pause plus longue.

    Args:
        prompt          : le message de ce tour (le prompt structuré pour
                          le premier tour)
        history         : historique de conversation, voir docstring du
                          module — None pour un appel simple (cas actuel)
        retries         : nombre de re-essais après le premier échec
        backoff_seconds : pause de base entre tentatives (croissante)
    """
    if not settings.GEMINI_API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY manquante — ajoutez-la dans .env "
            "(GEMINI_API_KEY=AIza...)"
        )

    client = genai.Client(api_key=settings.GEMINI_API_KEY)
    contents = (history or []) + [{"role": "user", "parts": [{"text": prompt}]}]

    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = client.models.generate_content(
                model=settings.GEMINI_MODEL,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                ),
            )
            return response.text
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Tentative %d/%d échouée (Gemini) : %s",
                attempt + 1,
                retries + 1,
                exc,
            )
            if attempt < retries:
                time.sleep(backoff_seconds * (attempt + 1))

    logger.exception("Appel API Gemini échoué après %d tentative(s)", retries + 1)
    raise RuntimeError(f"Erreur API Gemini après {retries + 1} tentative(s) : {last_exc}") from last_exc


def run_analyst_agent(repo: DuckDBRepository) -> str:
    """
    Point d'entrée utilisé par l'orchestrateur (LangGraph).

    Construit le prompt depuis DuckDB, appelle le LLM, et ajoute le
    bandeau DEMO_MODE à la PRÉSENTATION (pas dans le prompt envoyé au
    LLM, qui reste exactement celui spécifié dans la doc de référence).
    """
    prompt, alerts = build_prompt_from_db(repo)
    logger.info(
        "Prompt construit (%d caractères, %d alerte(s) incluse(s))",  
        len(prompt),
        len(alerts),                                  
    )

    synthesis = generate_synthesis(prompt)

    if alerts:                                        
        alert_ids = [a.alert_id for a in alerts]
        repo.mark_alerts_read(alert_ids)
        logger.info("%d alerte(s) marquée(s) comme lue(s)", len(alert_ids))

    if settings.DEMO_MODE:
        synthesis = f" {settings.DEMO_DISCLAIMER}\n\n{synthesis}"

    synthesis = f"{synthesis}\n\n---\n_{NOT_ADVICE_DISCLAIMER}_"

    return synthesis


# ---------------------------------------------------------------------------
# Test rapide — python -m app.agents.analyst.analyst_agent
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with DuckDBRepository() as repo:
        synthesis = run_analyst_agent(repo)

    print("\n" + "=" * 60)
    print(synthesis)
    print("=" * 60)