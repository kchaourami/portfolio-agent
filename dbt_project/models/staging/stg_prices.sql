/*
stg_prices
==========
Vue de staging sur raw_prices.

Rôle :
  - Renommer / caster les colonnes si nécessaire
  - Exclure le benchmark (^FCHI) des prix du portefeuille
  - Filtrer les lignes sans close_price valide
  - NE PAS calculer d'indicateurs ici — c'est le rôle des marts

Source : raw_prices (table écrite par DataCollector)
Consommé par : mart_risk_signals, mart_portfolio
*/

select
    date,
    ticker,
    isin,
    company_name,
    asset_type,
    sector,
    close_price,
    volume,
    daily_return,
    source

from main.raw_prices

where
    close_price is not null
    and close_price > 0
    and ticker != '^FCHI'     -- benchmark traité séparément dans stg_benchmark