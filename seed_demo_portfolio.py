"""
seed_demo_portfolio.py
========================
Script ponctuel — à placer à la racine du projet, à côté de
seed_test_portfolio.py et preflight_check.py.

Insère un portefeuille DÉMO réaliste et diversifié — destiné aux
démonstrations, captures d'écran, et au mémoire. Contrairement à
seed_test_portfolio.py (qui force volontairement une concentration
excessive sur BNP.PA pour valider SECTOR_CONCENTRATION), ce portefeuille
représente une allocation crédible de type PEA particulier : un cœur en
ETF monde, des satellites sectoriels équilibrés, aucune ligne dominante.

Les alertes qui apparaîtront avec ce portefeuille seront de vrais signaux
de marché (PRICE_DROP, UNDERPERFORMANCE...), pas des artefacts de test —
plus convaincant pour une démonstration.

 Quantités calculées à partir de prix approximatifs (pas les cours exacts
du jour) — c'est un portefeuille FICTIF, la précision des montants n'a pas
besoin d'être exacte. Ajustez si besoin après avoir vérifié les vrais cours
via data_collector.py.

Usage : python seed_demo_portfolio.py
"""

from datetime import date

import pandas as pd

from app.storage.duckdb_repository import DuckDBRepository

# Allocation cible (~25 000€, profil cœur-satellites diversifié) :
#   CW8.PA   (ETF monde)        ~40% — cœur du portefeuille
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
        print(f"✓ {written} position(s) démo insérée(s) dans 'portfolio'\n")
        print(repo.fetch_portfolio())
        print(
            "\n*  Rappel : ceci est un portefeuille FICTIF de démonstration, "
            "pas une vraie position d'investissement."
        )