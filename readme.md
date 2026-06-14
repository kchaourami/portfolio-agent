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

## 3. BCE Data Portal — Macro zone euro

- Agent alimenté : Macro
- Données récupérées : taux BCE, EUR/USD, inflation zone euro
- Accès : API REST SDMX
- Valeur agent : régime monétaire et sensibilité taux
- Décision : retenue pour V1 macro simple