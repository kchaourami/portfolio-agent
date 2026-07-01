# Agent IA d'aide à la décision portefeuille

Agent IA appliqué à la finance de marché. Il surveille un portefeuille d'actions et ETF français, détecte les signaux de risque, génère des décisions structurées d'aide à la décision, et produit une synthèse en langage naturel via un LLM.
 Le système ne passe aucun ordre automatiquement — il produit des signaux, des alertes et des recommandations à partir de données calculées de façon déterministe.

---

## Fonctionnalités

- Collecte des prix de marché via yfinance
- Collecte d'indicateurs macroéconomiques (BCE, INSEE, Eurostat en repli)
- Stockage dans DuckDB, transformations via dbt
- Calcul d'indicateurs de risque : rendement, volatilité, drawdown, volume anormal, performance relative vs CAC 40
- Génération d'alertes de risque configurables
- Calcul de décisions structurées par ticker (BUY_WATCH / INCREASE / HOLD / WATCH / REDUCE / SELL_SIGNAL)
- Synthèse finale en langage naturel via un Agent Analyste (Gemini)
- Portefeuille fictif de démonstration

---

## Architecture

### Pipeline de données

```
DataCollector (yfinance)          MacroCollector (BCE / INSEE)
        ↓                                  ↓
   raw_prices                          raw_macro
        ↓                                  ↓
              dbt run (stg_* → mart_*)
                         ↓
              mart_risk_signals
              mart_portfolio_value
                         ↓
          Risk Engine → Alert Engine
          Decision Engine
          Macro Regime
                         ↓
                  Agent Analyste (LLM)
                         ↓
                   Synthèse finale
```

### Orchestration LangGraph

```
data_node
    ↓
macro_node
    ↓
dbt_run_node
    ↓
alert_node ──┐
regime_node──┼── (parallèle, lecture DuckDB locale uniquement)
decision_node┘
    ↓
analyst_node  (seul nœud avec appel LLM)
    ↓
END
```

Fichier : `app/orchestration/graph.py`

---

## Structure du projet

```
portfolio-agent/
│
├── app/
│   ├── config/
│   │   └── settings.py              # Configuration centralisée
│   │
│   ├── pipeline/
│   │   ├── providers/
│   │   │   ├── base.py              # Interface abstraite des providers
│   │   │   └── yahoo_provider.py    # Implémentation yfinance
│   │   ├── data_collector.py        # Collecte et normalisation des prix
│   │   ├── macro_collector.py       # Collecte BCE / INSEE / Eurostat
│   │   ├── macro_regime.py          # Calcul du régime macro
│   │   ├── risk_calculator.py       # Calcul des indicateurs de risque
│   │   ├── risk_models.py           # Structures de données (Alert, RiskBreach)
│   │   ├── alert_engine.py          # Mise en forme et persistance des alertes
│   │   ├── decision_models.py       # Structures de données (Decision, TickerDecision)
│   │   └── decision_engine.py       # Calcul des décisions par ticker
│   │
│   ├── agents/
│   │   └── analyst/
│   │       ├── prompt_builder.py    # Construction du prompt structuré
│   │       └── analyst_agent.py     # Appel LLM et persistance de la synthèse
│   │
│   ├── orchestration/
│   │   └── graph.py                 # Graphe LangGraph (pipeline complet)
│   │
│   ├── seed_demo_portfolio.py   # Insertion du portefeuille fictif
│   │   
│   │
│   └── storage/
│       └── duckdb_repository.py     # Couche d'accès à DuckDB
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_prices.sql
│   │   │   ├── stg_benchmark.sql
│   │   │   └── stg_macro.sql
│   │   └── marts/
│   │       ├── mart_risk_signals.sql
│   │       └── mart_portfolio_value.sql
│   └── tests/
│       ├── test_risk_signals_ranges.sql
│       └── test_decision_scores_ranges.sql
│
├── data/
│   └── portfolio.duckdb
│
├── requirements.txt
├── .env
└── README.md
```

---

## Prérequis

- Python 3.11
- Environnement virtuel `.venv`
- Clé API Gemini (pour l'Agent Analyste)

Installation :

```bash
pip install -r requirements.txt
```

Configuration :

```bash
cp .env .env   # Linux / macOS
Copy-Item .env .env  # Windows PowerShell
```

Variables à renseigner dans `.env` :

```env
GEMINI_API_KEY=votre_cle_ici
GEMINI_MODEL=gemini-2.5-flash
```

---

## Démarrage rapide

### 1. Initialiser le portefeuille fictif

À lancer une seule fois pour insérer les positions de démonstration dans DuckDB :

```bash
python -m app.scripts.seed_demo_portfolio
```

### 2. Lancer le pipeline complet

```bash
python -m app.orchestration.graph
```

Cette commande exécute toute la chaîne : collecte marché, collecte macro, transformation dbt, calcul des alertes et décisions, synthèse de l'Agent Analyste.

---

## Commandes dbt

Depuis le dossier `dbt_project` :

```bash
cd dbt_project
dbt debug --profiles-dir .        # Vérifier la configuration
dbt run --profiles-dir .          # Construire les modèles
dbt test --profiles-dir .         # Lancer les tests qualité
dbt ls --resource-type model --profiles-dir .   # Lister les modèles
```

---

## Tables DuckDB

### Tables brutes

| Table | Contenu |
|---|---|
| `main.raw_prices` | Prix de marché quotidiens par ticker |
| `main.raw_macro` | Indicateurs macroéconomiques (BCE, INSEE) |
| `main.portfolio` | Positions du portefeuille |
| `main.alerts` | Alertes de risque générées |
| `main.decisions` | Décisions du Decision Engine par run |
| `main.syntheses` | Synthèses textuelles de l'Agent Analyste |

### Vues staging dbt

| Vue | Rôle |
|---|---|
| `main_staging.stg_prices` | Nettoyage et filtrage des prix |
| `main_staging.stg_benchmark` | Isolation du CAC 40 |
| `main_staging.stg_macro` | Consolidation du contexte macro en une ligne |

### Tables marts dbt

| Table | Rôle |
|---|---|
| `main_marts.mart_risk_signals` | Indicateurs de risque calculés par ticker et par date |
| `main_marts.mart_portfolio_value` | Valorisation des positions avec indicateurs de risque |

---

## Sources de données

| Source | Données collectées | Rôle |
|---|---|---|
| yfinance | Prix OHLCV, volumes, CAC 40 | Source principale (marché) |
| BCE Data Portal | Taux BCE, EUR/USD | Source principale (macro) |
| INSEE BDM | Inflation France (IPC, glissement annuel) | Source principale (inflation) |
| Eurostat (HICP) | Inflation France | Repli automatique si INSEE échoue |

L'accès à l'API INSEE BDM ne nécessite pas d'authentification (confirmé par le support INSEE le 23/06/2026). Eurostat est activé automatiquement en cas d'échec de l'appel INSEE.

---

## Tests qualité dbt

15 tests au total, dont 2 tests sur mesure :

- `test_risk_signals_ranges.sql` : vérifie que `daily_return`, `vol_20d`, `drawdown` et `volume_ratio_20d` sont dans des plages financièrement réalistes
- `test_decision_scores_ranges.sql` : vérifie que les 4 scores du Decision Engine sont entre 0 et 100, et que la décision appartient à l'échelle autorisée

Convention dbt : un test passe s'il retourne 0 ligne. Toute ligne retournée est une anomalie à investiguer.

---

## Portefeuille de démonstration

| Ticker | Société | Type | Secteur |
|---|---|---|---|
| CW8.PA | Amundi MSCI World | ETF | — |
| PAEEM.PA | Amundi MSCI Emerging Markets | ETF | — |
| AIR.PA | Airbus | Action | Industrials |
| MC.PA | LVMH | Action | Consumer Cyclical |
| SAN.PA | Sanofi | Action | Healthcare |
| OR.PA | L'Oréal | Action | Consumer Defensive |
| BNP.PA | BNP Paribas | Action | Financial Services |

Benchmark de référence : CAC 40 (`^FCHI`)

---

## Échelle de décision

| Décision | Libellé | Condition |
|---|---|---|
| `BUY_WATCH` | Opportunité à surveiller | Risque faible, momentum positif (action) |
| `INCREASE` | Renforcement à étudier | Risque faible, momentum positif (ETF) |
| `HOLD` | Conserver | Risque faible, momentum neutre |
| `WATCH` | Sous surveillance | Risque faible, momentum faible |
| `REDUCE` | Réduction à étudier | Risque modéré (score 30–59) |
| `SELL_SIGNAL` | Signal de vente | Risque élevé (score ≥ 60) |

Les décisions sont calculées de façon déterministe par `decision_engine.py` — le LLM les explique, il ne les calcule pas.