"""
Agent Data (macro) — pipeline déterministe de collecte des indicateurs
macroéconomiques nécessaires au contexte de l'Agent Analyste :
  - ECB_RATE     : taux de refinancement principal de la BCE (MRO)
  - EURUSD       : taux de change EUR/USD
  - INFLATION_FR : inflation France en glissement annuel (%)

Sources :
  - BCE Data Portal  → ECB_RATE, EURUSD (API SDMX 2.1, CSV, aucune clé)
  - INSEE BDM        → INFLATION_FR, SOURCE PRINCIPALE (API SDMX, XML,
                        aucune authentification requise — confirmé par le
                        support INSEE le 23/06/2026, cf. note ci-dessous)
  - Eurostat (HICP)  → INFLATION_FR, REPLI automatique si INSEE échoue

Normalise tout au schéma commun de raw_macro : (date, series_key, value,
source, fetched_at) — consommé ensuite par dbt (stg_macro.sql, inchangé).

"""

from __future__ import annotations

import io
import logging
import time
import xml.etree.ElementTree as ET
from datetime import date

import httpx
import pandas as pd

from app.storage.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)

def _get_with_retry(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float = 15,
    retries: int = 1,
    backoff_seconds: float = 2.0,
) -> httpx.Response:
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            return httpx.get(url, params=params, headers=headers, timeout=timeout)
        except httpx.TransportError as exc:
            last_exc = exc
            logger.warning(
                "Tentative %d/%d échouée pour %s : %s",
                attempt + 1,
                retries + 1,
                url,
                exc,
            )
            if attempt < retries:
                time.sleep(backoff_seconds)

    raise last_exc  # toutes les tentatives ont échoué




ECB_BASE_URL = "https://data-api.ecb.europa.eu/service/data"
ECB_RATE_FLOW = "FM"
ECB_RATE_KEY = "D.U2.EUR.4F.KR.MRR_FR.LEV"     
ECB_FX_FLOW = "EXR"
ECB_FX_KEY = "D.USD.EUR.SP00.A"              

INSEE_BASE_URL = "https://api.insee.fr/series/BDM/data/SERIES_BDM"
INSEE_INFLATION_IDBANK = "011814056"           
INSEE_NS = {"message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"}

EUROSTAT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"
EUROSTAT_INFLATION_FLOW = "PRC_HICP_MINR"
EUROSTAT_INFLATION_KEY = "M.I25.TOTAL.FR"       


# Fetch générique CSV — BCE et Eurostat

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
    """

    url = f"{base_url}/{flow_ref}/{key}"
    params = {"format": format_param, "lastNObservations": last_n}

    response = _get_with_retry(url, params=params, timeout=15)

    if response.status_code >= 400:
        raise RuntimeError(
            f"Erreur {response.status_code} pour {response.url} — "
            f"corps de la réponse : {response.text[:1000]}"
        )

    df = pd.read_csv(io.StringIO(response.text))
    return df[["TIME_PERIOD", "OBS_VALUE"]]


def fetch_eurostat_inflation_yoy(last_n: int = 13) -> pd.DataFrame:
    
    #Calcule l'inflation France (IPCH, glissement annuel) à partir de l'indice HICP brut Eurostat.
    
    df = fetch_sdmx_csv(
        EUROSTAT_BASE_URL,
        EUROSTAT_INFLATION_FLOW,
        EUROSTAT_INFLATION_KEY,
        last_n=last_n,
        format_param="SDMX-CSV",
    )
    return _compute_yoy(df)


# Fetch INSEE 

def fetch_insee_series(idbank: str, last_n: int = 1) -> pd.DataFrame:
    
    #Récupère les N dernières observations d'une série INSEE BDM.

    url = f"{INSEE_BASE_URL}/{idbank}"
    params = {"lastNObservations": last_n}

    response = _get_with_retry(url, params=params, timeout=15)
    response.raise_for_status()

    return _parse_insee_xml(response.text)


def _parse_insee_xml(xml_text: str) -> pd.DataFrame:
    
    #Parse la réponse XML SDMX-ML (StructureSpecificData) de l'API INSEE BDM.

    root = ET.fromstring(xml_text)
    dataset = root.find("message:DataSet", INSEE_NS)

    if dataset is None:
        raise ValueError("Structure XML INSEE inattendue : <message:DataSet> introuvable")

    rows: list[dict] = []
    for series in dataset.findall("Series"):
        for obs in series.findall("Obs"):
            rows.append(
                {
                    "TIME_PERIOD": obs.get("TIME_PERIOD"),
                    "OBS_VALUE": obs.get("OBS_VALUE"),
                }
            )

    if not rows:
        raise ValueError("Aucune observation trouvée dans la réponse INSEE")

    return pd.DataFrame(rows)


def fetch_insee_inflation_yoy(idbank: str = INSEE_INFLATION_IDBANK) -> pd.DataFrame:
    
    #Calcule l'inflation France en glissement annuel (% sur 12 mois) à partir de l'indice IPC brut INSEE. 
    
    df = fetch_insee_series(idbank, last_n=13)
    return _compute_yoy(df)


def _compute_yoy(df: pd.DataFrame) -> pd.DataFrame:
    
    #Calcule un glissement annuel (%) à partir de 13 observations mensuelles triées 

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

# Normalisation au schéma commun raw_macro

def _to_raw_macro(df: pd.DataFrame, series_key: str, source: str) -> pd.DataFrame:
    #Convertit (TIME_PERIOD, OBS_VALUE) vers le schéma raw_macro.
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["TIME_PERIOD"]).dt.date
    out["series_key"] = series_key
    out["value"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    out["source"] = source
    out["fetched_at"] = date.today()
    return out.dropna(subset=["value"])


# Orchestration

def collect_macro_data() -> pd.DataFrame:
    
   #Collecte les 3 indicateurs macro nécessaires au prompt de l'Agent Analyste. Chaque source est isolée dans son propre try/except.

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
        df_inflation = fetch_insee_inflation_yoy()
        frames.append(_to_raw_macro(df_inflation, "INFLATION_FR", "insee"))
    except Exception as exc:
        logger.warning(
            "Échec collecte INFLATION_FR via INSEE (%s) — tentative via Eurostat (repli)",
            exc,
        )
        try:
            df_inflation = fetch_eurostat_inflation_yoy()
            frames.append(_to_raw_macro(df_inflation, "INFLATION_FR", "eurostat"))
        except Exception as exc2:
            logger.warning(
                "Échec collecte INFLATION_FR : aucune source disponible "
                "(INSEE: %s | Eurostat: %s)",
                exc,
                exc2,
            )

    if not frames:
        logger.error("Aucune donnée macro collectée — toutes les sources ont échoué")
        return pd.DataFrame(columns=["date", "series_key", "value", "source", "fetched_at"])

    return pd.concat(frames, ignore_index=True)


def run_macro_collector(repo: DuckDBRepository) -> int:

    #Point d'entrée utilisé par l'orchestrateur (LangGraph).

    df = collect_macro_data()
    if df.empty:
        return 0
    return repo.upsert_macro(df)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with DuckDBRepository() as repo:
        written = run_macro_collector(repo)
        print(f"\n✓ {written} ligne(s) macro écrite(s)\n")
        print(repo.fetch_latest_macro())