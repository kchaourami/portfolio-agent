"""
macro_collector.py
====================
Emplacement cible dans le repo : app/pipeline/macro_collector.py

Agent Data (macro) — pipeline déterministe de collecte des indicateurs
macroéconomiques nécessaires au contexte de l'Agent Analyste :
  - ECB_RATE     : taux de refinancement principal de la BCE (MRO)
  - EURUSD       : taux de change EUR/USD
  - INFLATION_FR : inflation France en glissement annuel (%)

Sources actuelles (toutes deux des API SDMX 2.1 ouvertes, AUCUNE clé requise) :
  - BCE Data Portal           → ECB_RATE, EURUSD
  - Eurostat (HICP France)    → INFLATION_FR

Normalise tout au schéma commun de raw_macro : (date, series_key, value,
source, fetched_at) — consommé ensuite par dbt (stg_macro.sql, inchangé).

---------------------------------------------------------------------------
NOTE MÉTHODOLOGIQUE — changement de source pour INFLATION_FR
---------------------------------------------------------------------------
L'API officielle INSEE (api.insee.fr) reste la source visée à terme, mais
son flux d'authentification OAuth2 (client_credentials) est actuellement
défaillant côté serveur (testé avec 2 clients HTTP différents, sur 2
réseaux différents — cf. ticket envoyé au support INSEE). En attendant
une réponse, INFLATION_FR est calculée à partir de l'IPCH (indice
harmonisé européen, Eurostat) plutôt que l'IPC national INSEE — légère
différence méthodologique à documenter dans le mémoire, mais l'API
Eurostat est ouverte, stable, et à jour.

Les fonctions pour l'API INSEE officielle sont conservées plus bas
(préfixées INSEE_, désactivées) pour réactivation rapide si le support
débloque la situation.
"""

from __future__ import annotations

import io
import logging
from datetime import date

import httpx
import pandas as pd

from app.config.settings import settings
from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Identifiants de séries — vérifiés manuellement, à ne pas deviner
# ---------------------------------------------------------------------------

ECB_BASE_URL = "https://data-api.ecb.europa.eu/service/data"
ECB_RATE_FLOW = "FM"
ECB_RATE_KEY = "D.U2.EUR.4F.KR.MRR_FR.LEV"     # Taux de refinancement principal (MRO)
ECB_FX_FLOW = "EXR"
ECB_FX_KEY = "D.USD.EUR.SP00.A"                 # EUR/USD, cours de référence quotidien

EUROSTAT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"
# ATTENTION : PRC_HICP_MANR (utilisé initialement) est ARCHIVÉ ET FIGÉ depuis
# décembre 2025 — Eurostat a basculé vers une nouvelle nomenclature (ECOICOP
# version 2, obligatoire depuis janvier 2026). Le dataset actif équivalent
# est PRC_HICP_MINR. Le code unité "RCH_A" (taux annuel direct), confirmé
# fonctionnel sur l'ancien MANR, renvoie une erreur 400 sur MINR — code
# unité probablement différent sur ce nouveau dataset, non confirmé.
# On utilise donc "I25" (indice brut, base 2025=100), confirmé fonctionnel
# par la documentation du package R 'hicp', et on calcule le glissement
# annuel nous-mêmes (même logique que prévu pour l'INSEE).
EUROSTAT_INFLATION_FLOW = "PRC_HICP_MINR"
EUROSTAT_INFLATION_KEY = "M.I25.TOTAL.FR"       # Indice mensuel, base 2025, tous postes (COICOP18=TOTAL), France
# Historique : la dimension s'appelait "COICOP" avec le code "CP00" sous
# l'ancienne nomenclature (ECOICOP v1, dataset PRC_HICP_MANR, frozen).
# Sous ECOICOP v2 (PRC_HICP_MINR), la dimension est renommée "COICOP18"
# (alignement UN COICOP 2018) et le code "tous postes" devient "TOTAL" —
# confirmé par le message d'erreur 400 reçu en test, qui nommait
# explicitement la dimension "COICOP18".


# ---------------------------------------------------------------------------
# Fetch générique SDMX 2.1 — BCE et Eurostat exposent toutes les deux ce
# format CSV standard, donc une seule fonction sert pour les deux sources.
# ---------------------------------------------------------------------------

def fetch_sdmx_csv(
    base_url: str,
    flow_ref: str,
    key: str,
    last_n: int = 1,
    format_param: str = "csvdata",
) -> pd.DataFrame:
    """
    Récupère les N dernières observations d'une série SDMX 2.1 conforme
    (BCE Data Portal ou Eurostat), via le format CSV natif — aucune clé,
    aucune authentification requise pour ces deux sources.

    Args:
        base_url     : racine de l'API (ECB_BASE_URL ou EUROSTAT_BASE_URL)
        flow_ref     : identifiant du dataflow (ex: "EXR", "PRC_HICP_MANR")
        key          : clé de la série (ex: "D.USD.EUR.SP00.A", "M..CP00.FR")
        last_n       : nombre d'observations les plus récentes
        format_param : valeur du paramètre 'format' — diffère selon la
                       source : "csvdata" pour la BCE, "SDMX-CSV" pour
                       Eurostat (ce n'est PAS un standard SDMX universel,
                       chaque implémentation choisit son propre identifiant
                       de format CSV — vérifié séparément pour chaque API).

    Returns:
        DataFrame avec colonnes (TIME_PERIOD, OBS_VALUE)
    """
    url = f"{base_url}/{flow_ref}/{key}"
    params = {"format": format_param, "lastNObservations": last_n}

    response = httpx.get(url, params=params, timeout=15)

    if response.status_code >= 400:
        raise RuntimeError(
            f"Erreur {response.status_code} pour {response.url} — "
            f"corps de la réponse : {response.text[:1000]}"
        )

    df = pd.read_csv(io.StringIO(response.text))
    return df[["TIME_PERIOD", "OBS_VALUE"]]


def fetch_eurostat_inflation_yoy(
    last_n: int = 13,
) -> pd.DataFrame:
    """
    Calcule l'inflation France en glissement annuel (% sur 12 mois) à partir
    de l'indice HICP brut (unit=I25, confirmé fonctionnel). Récupère 13 mois
    d'historique pour comparer le dernier point au même mois de l'année
    précédente — comportement déterministe, pas une donnée inventée.

    Même logique que prévu initialement pour l'INSEE (cf. fonction réservée
    plus bas dans ce fichier) — réutilisable telle quelle si on bascule un
    jour vers l'API INSEE officielle.

    Returns:
        DataFrame à une ligne : (TIME_PERIOD, OBS_VALUE), où OBS_VALUE est
        déjà le taux de glissement annuel en %.
    """
    df = fetch_sdmx_csv(
        EUROSTAT_BASE_URL,
        EUROSTAT_INFLATION_FLOW,
        EUROSTAT_INFLATION_KEY,
        last_n=last_n,
        format_param="SDMX-CSV",
    )
    df = df.sort_values("TIME_PERIOD").reset_index(drop=True)

    if len(df) < 13:
        raise ValueError(
            f"Historique insuffisant pour calculer le glissement annuel "
            f"({len(df)} mois reçus, 13 nécessaires)"
        )

    latest = df.iloc[-1]
    year_ago = df.iloc[0]
    yoy_pct = round((float(latest["OBS_VALUE"]) / float(year_ago["OBS_VALUE"]) - 1) * 100, 4)

    return pd.DataFrame([{"TIME_PERIOD": latest["TIME_PERIOD"], "OBS_VALUE": yoy_pct}])


# ---------------------------------------------------------------------------
# Normalisation au schéma commun raw_macro
# ---------------------------------------------------------------------------

def _to_raw_macro(df: pd.DataFrame, series_key: str, source: str) -> pd.DataFrame:
    """Convertit (TIME_PERIOD, OBS_VALUE) vers le schéma raw_macro."""
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["TIME_PERIOD"]).dt.date
    out["series_key"] = series_key
    out["value"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    out["source"] = source
    out["fetched_at"] = date.today()
    return out.dropna(subset=["value"])


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def collect_macro_data() -> pd.DataFrame:
    """
    Collecte les 3 indicateurs macro nécessaires au prompt de l'Agent
    Analyste. Chaque source est isolée dans son propre try/except — l'échec
    d'une source ne bloque pas les autres.
    """
    frames: list[pd.DataFrame] = []

    try:
        df_rate = fetch_sdmx_csv(ECB_BASE_URL, ECB_RATE_FLOW, ECB_RATE_KEY)
        frames.append(_to_raw_macro(df_rate, "ECB_RATE", "bce"))
    except Exception as exc:
        logger.warning("Échec collecte ECB_RATE : %s", exc)

    try:
        df_fx = fetch_sdmx_csv(ECB_BASE_URL, ECB_FX_FLOW, ECB_FX_KEY)
        frames.append(_to_raw_macro(df_fx, "EURUSD", "bce"))
    except Exception as exc:
        logger.warning("Échec collecte EURUSD : %s", exc)

    try:
        df_inflation = fetch_eurostat_inflation_yoy()
        frames.append(_to_raw_macro(df_inflation, "INFLATION_FR", "eurostat"))
    except Exception as exc:
        logger.warning("Échec collecte INFLATION_FR : %s", exc)

    if not frames:
        logger.error("Aucune donnée macro collectée — toutes les sources ont échoué")
        return pd.DataFrame(columns=["date", "series_key", "value", "source", "fetched_at"])

    return pd.concat(frames, ignore_index=True)


def run_macro_collector(repo: DuckDBRepository) -> int:
    """Point d'entrée utilisé par l'orchestrateur (LangGraph)."""
    df = collect_macro_data()
    if df.empty:
        return 0
    return repo.upsert_macro(df)


# ---------------------------------------------------------------------------
# Test rapide — python -m app.pipeline.macro_collector
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with DuckDBRepository() as repo:
        written = run_macro_collector(repo)
        print(f"\n✓ {written} ligne(s) macro écrite(s)\n")
        print(repo.fetch_latest_macro())


# ===========================================================================
# RÉSERVE — API officielle INSEE (BDM), DÉSACTIVÉE
# ===========================================================================
# Conservé pour réactivation rapide si le support INSEE débloque le flux
# OAuth2 client_credentials. Non appelé par collect_macro_data() ci-dessus.
#
# Pour réactiver : remplacer l'appel Eurostat dans collect_macro_data() par
# fetch_insee_inflation_yoy(), et s'assurer que settings.INSEE_API_KEY /
# le client_secret sont disponibles.
#
# from datetime import datetime
#
# INSEE_BASE_URL = "https://api.insee.fr/series/BDM/data/SERIES_BDM"
# INSEE_TOKEN_URL = "https://api.insee.fr/token"
# INSEE_INFLATION_IDBANK = "011814056"  # IPC France, base 2025, hors tabac
#
#
# def _insee_get_token(client_id: str, client_secret: str) -> str:
#     response = httpx.post(
#         INSEE_TOKEN_URL,
#         auth=(client_id, client_secret),
#         data={"grant_type": "client_credentials"},
#         timeout=15,
#     )
#     response.raise_for_status()
#     return response.json()["access_token"]
#
#
# def fetch_insee_series(idbank: str, last_n: int = 1) -> pd.DataFrame:
#     token = _insee_get_token(settings.INSEE_CLIENT_ID, settings.INSEE_CLIENT_SECRET)
#     url = f"{INSEE_BASE_URL}/{idbank}"
#     headers = {"Authorization": f"Bearer {token}"}
#     params = {"lastNObservations": last_n}
#     response = httpx.get(url, headers=headers, params=params, timeout=15)
#     response.raise_for_status()
#     # Schéma de réponse propre à l'INSEE (DataSet > Series > Serie > Obs)
#     # à parser une fois confirmé via Swagger "Try it out" — voir schéma
#     # OpenAPI récupéré le 21/06/2026 (composants Observation / Serie /
#     # DataSet / Header) pour la structure exacte.
#     raise NotImplementedError("Parsing à finaliser une fois l'auth débloquée")
#
#
# def fetch_insee_inflation_yoy(idbank: str = INSEE_INFLATION_IDBANK) -> pd.DataFrame:
#     df = fetch_insee_series(idbank, last_n=13)
#     df = df.sort_values("TIME_PERIOD").reset_index(drop=True)
#     latest = df.iloc[-1]
#     year_ago = df.iloc[0]
#     yoy_pct = (float(latest["OBS_VALUE"]) / float(year_ago["OBS_VALUE"]) - 1) * 100
#     return pd.DataFrame([{"TIME_PERIOD": latest["TIME_PERIOD"], "OBS_VALUE": yoy_pct}])