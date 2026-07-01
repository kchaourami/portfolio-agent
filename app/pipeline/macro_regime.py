"""
Rôle :
- Lire le contexte macro courant (stg_macro : ecb_rate, eurusd, inflation_fr
  et leurs dates respectives)
- Calculer un taux réel approximatif (ecb_rate - inflation_fr) et en déduire
  un régime classé (restrictif / neutre / accommodant)

On utilise le "taux réel" (taux nominal BCE - inflation), un concept
macroéconomique standard pour juger si la politique monétaire est
restrictive ou accommodante par rapport à l'inflation courante :
  taux réel > +1 point  → RESTRICTIF   (la politique freine l'économie)
  taux réel < -1 point  → ACCOMMODANT  (la politique stimule l'économie)
  sinon                 → NEUTRE
Limite méthodologique connue : c'est une approximation simplifiée
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from pydantic import BaseModel

from app.storage.duckdb_repository import DuckDBRepository

REAL_RATE_RESTRICTIVE_THRESHOLD = 1.0
REAL_RATE_ACCOMMODATIVE_THRESHOLD = -1.0


class MacroRegime(BaseModel):
    """
    Contexte macro structuré — alimente directement le bloc
    [CONTEXTE MACRO] du prompt de l'Agent Analyste.
    """

    regime: str                           
    ecb_rate: float | None
    ecb_rate_date: date | None
    eurusd: float | None
    eurusd_date: date | None
    inflation_fr: float | None
    inflation_fr_date: date | None
    real_rate: float | None             
    drivers: list[str]                   


# Classification

def classify_regime(real_rate: float | None) -> str:

    #Classifie le régime à partir du taux réel.

    if real_rate is None:
        return "indéterminé"
    if real_rate > REAL_RATE_RESTRICTIVE_THRESHOLD:
        return "restrictif"
    if real_rate < REAL_RATE_ACCOMMODATIVE_THRESHOLD:
        return "accommodant"
    return "neutre"


def _fmt_date(d) -> str:
    
    #Formate une date en YYYY-MM-DD

    if d is None:
        return "date inconnue"
    return str(d)[:10]


def _build_drivers(
    ecb_rate: float | None,
    ecb_rate_date,
    eurusd: float | None,
    eurusd_date,
    inflation_fr: float | None,
    inflation_fr_date,
    real_rate: float | None,
) -> list[str]:
    
    #Construit des phrases factuelles courtes à partir des valeurs disponibles

    drivers: list[str] = []

    if ecb_rate is not None and inflation_fr is not None and real_rate is not None:
        drivers.append(
            f"Taux BCE à {ecb_rate:.2f}% vs inflation France à {inflation_fr:.2f}% "
            f"→ taux réel d'environ {real_rate:+.2f} pt"
        )
    elif ecb_rate is not None:
        drivers.append(f"Taux BCE à {ecb_rate:.2f}% (au {_fmt_date(ecb_rate_date)})")

    if eurusd is not None:
        drivers.append(f"EUR/USD à {eurusd:.4f} (au {_fmt_date(eurusd_date)})")

    if inflation_fr is not None:
        drivers.append(
            f"Inflation France à {inflation_fr:.2f}% — dernière donnée "
            f"disponible au {_fmt_date(inflation_fr_date)} (publication mensuelle, "
            f"délai habituel)"
        )

    return drivers


# Orchestration

def _safe(value):
    #Convertit NaN/NaT (pandas) en None — sinon retourne la valeur telle quelle.
    return None if pd.isna(value) else value


def get_current_macro_regime(repo: DuckDBRepository) -> MacroRegime:

    #Point d'entrée utilisé par l'orchestrateur (LangGraph) et par le constructeur de prompt de l'Agent Analyste.

    df = repo.execute_query("SELECT * FROM main_staging.stg_macro")

    if df.empty:
        return MacroRegime(
            regime="indéterminé",
            ecb_rate=None, ecb_rate_date=None,
            eurusd=None, eurusd_date=None,
            inflation_fr=None, inflation_fr_date=None,
            real_rate=None,
            drivers=["Aucune donnée macro disponible"],
        )

    row = df.iloc[0]

    ecb_rate = _safe(row["ecb_rate"])
    ecb_rate_date = _safe(row["ecb_rate_date"])
    eurusd = _safe(row["eurusd"])
    eurusd_date = _safe(row["eurusd_date"])
    inflation_fr = _safe(row["inflation_fr"])
    inflation_fr_date = _safe(row["inflation_fr_date"])

    real_rate = (
        round(float(ecb_rate) - float(inflation_fr), 4)
        if ecb_rate is not None and inflation_fr is not None
        else None
    )

    regime = classify_regime(real_rate)
    drivers = _build_drivers(
        ecb_rate, ecb_rate_date, eurusd, eurusd_date,
        inflation_fr, inflation_fr_date, real_rate,
    )

    return MacroRegime(
        regime=regime,
        ecb_rate=ecb_rate, ecb_rate_date=ecb_rate_date,
        eurusd=eurusd, eurusd_date=eurusd_date,
        inflation_fr=inflation_fr, inflation_fr_date=inflation_fr_date,
        real_rate=real_rate,
        drivers=drivers,
    )


if __name__ == "__main__":
    with DuckDBRepository() as repo:
        regime = get_current_macro_regime(repo)
        print(regime.model_dump_json(indent=2))