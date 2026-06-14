## Architecture — Portfolio Agent

```mermaid
flowchart LR
  subgraph Collectors
    A[DataCollector (market)]
    B[MacroCollector]
  end

  A -->|OHLCV, meta| D(DuckDB)
  B -->|Macro series| D

  D --> E[dbt models]
  E --> F[Mart: portfolio_value]
  E --> G[Mart: risk_signals]

  F --> H[Risk Agent]
  G --> I[Analyst Agent]

  style D fill:#f9f,stroke:#333,stroke-width:1px
```

Composants:
- `app.pipeline` : collecteurs et providers (yfinance)
- `app.storage` : couche DuckDB (`duckdb_repository.py`)
- `dbt_project` : transformations et marts
- `app.agents` : logique métier (risk, analyst)

Flux principal: collecte → stockage raw → transformations dbt → agents métier
