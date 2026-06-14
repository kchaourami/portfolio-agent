## Usage

Prérequis : un environnement Python (virtualenv ou .venv) et les dépendances listées dans `requirements.txt`.

Installer les dépendances :

```bash
pip install -r requirements.txt
```

Initialiser l'environnement :

```powershell
Copy-Item .env.example .env
# puis remplir .env
```

Collecte de données (exemple) :

```bash
python -m app.pipeline.data_collector
```

Test rapide DuckDB :

```bash
python -m app.storage.duckdb_repository
```

dbt (depuis le dossier `dbt_project`) :

```bash
cd dbt_project
# dbt doit être configuré via profiles.yml
dbt deps
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Lancer les agents (exemples) :

```bash
python -m app.agents.risk.risk_calculator
python -m app.agents.analyst.analyst_agent
```
