# Matrice des sources de données

## 1. yfinance — Market Data MVP

- Agent alimenté : Data, Risk
- Données récupérées : OHLCV, volume, historique, benchmark CAC 40
- Accès : librairie Python
- Fréquence : quotidienne
- Valeur agent : calcul des rendements, volatilité, drawdown, volume anormal
- Limites : source non officielle, usage prototype uniquement
- Décision : retenue pour MVP

## 2. INSEE BDM — Macro France

- Agent alimenté : Macro
- Données récupérées : inflation, chômage, production industrielle
- Accès : API REST SDMX
- Fréquence : mensuelle / trimestrielle
- Valeur agent : contexte macro France
- Décision : retenue pour V1 macro simple

## Project Overview

Portfolio Agent est un prototype d'agent d'analyse de portefeuille : collecte de données de marché, stockage dans DuckDB, transformations via dbt, et agents métier pour le calcul de risque et la génération d'alertes.

## Quickstart

- Copier le template d'env : `Copy-Item .env.example .env` (PowerShell) ou `cp .env.example .env` (bash)
- Installer les dépendances : `pip install -r requirements.txt`
- Collecter des données (exemple) : `python -m app.pipeline.data_collector`
- Initialiser la base DuckDB : exécuter `python -m app.storage.duckdb_repository` pour un test rapide
- Lancer dbt (dans `dbt_project`) : `dbt deps && dbt seed --profiles-dir . && dbt run && dbt test`

Voir `docs/usage.md` pour plus de détails.

## Contributing

Respecte la convention de commits et ajoute une `issue` si tu modifies l'architecture. Voir `docs/CONTRIBUTING.md`.
## 3. BCE Data Portal — Macro zone euro

- Agent alimenté : Macro
- Données récupérées : taux BCE, EUR/USD, inflation zone euro
- Accès : API REST SDMX
- Valeur agent : régime monétaire et sensibilité taux
- Décision : retenue pour V1 macro simple