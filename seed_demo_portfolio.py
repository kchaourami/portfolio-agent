"""
Role : 
Insère un portefeuille DÉMO réaliste et diversifié — ce portefeuille
représente une allocation crédible de type PEA particulier : un cœur en
ETF monde, des satellites sectoriels équilibrés, aucune ligne dominante.

"""

from datetime import date

import pandas as pd

from app.storage.duckdb_repository import DuckDBRepository

# Allocation cible (~25 000 euros, profil diversifié) :
#   CW8.PA   (ETF monde)        ~40% — coeur du portefeuille
#   PAEEM.PA (ETF émergents)    ~10% — diversification géographique
#   MC.PA    (LVMH, luxe)       ~13%
#   SAN.PA   (Sanofi, santé)    ~12%
#   AIR.PA   (Airbus, indus.)   ~12%
#   OR.PA    (L'Oréal, conso.)   ~9%
#   BNP.PA   (banque)            ~5% — volontairement limité, pas de concentration

DEMO_POSITIONS = pd.DataFrame(
    [
        {"ticker": "CW8.PA",   "quantity": 20,  "purchase_price": 480.0, "purchase_date": date(2025, 12, 15), "label": "Démo — ETF Monde (cœur de portefeuille)"},
        {"ticker": "PAEEM.PA", "quantity": 147, "purchase_price": 16.5,  "purchase_date": date(2025, 12, 15), "label": "Démo — ETF Émergents"},
        {"ticker": "MC.PA",    "quantity": 5,   "purchase_price": 640.0, "purchase_date": date(2025, 12, 15), "label": "Démo — Luxe (LVMH)"},
        {"ticker": "SAN.PA",   "quantity": 33,  "purchase_price": 88.0,  "purchase_date": date(2025, 12, 15), "label": "Démo — Santé (Sanofi)"},
        {"ticker": "AIR.PA",   "quantity": 19,  "purchase_price": 155.0, "purchase_date": date(2025, 12, 15), "label": "Démo — Aéronautique (Airbus)"},
        {"ticker": "OR.PA",    "quantity": 6,   "purchase_price": 375.0, "purchase_date": date(2025, 12, 15), "label": "Démo — Consommation (L'Oréal)"},
        {"ticker": "BNP.PA",   "quantity": 21,  "purchase_price": 58.0,  "purchase_date": date(2025, 12, 15), "label": "Démo — Banque (BNP Paribas)"},
    ]
)


if __name__ == "__main__":
    with DuckDBRepository() as repo:
        written = repo.upsert_portfolio(DEMO_POSITIONS)
        print(f" {written} position inséré dans 'portfolio'\n")
        print(repo.fetch_portfolio())
        print(
            "\n*  Rappel : ceci est un portefeuille fictif "
            "pas une vraie position d'investissement."
        )