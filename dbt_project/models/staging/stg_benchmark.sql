/*
Vue de staging isolant uniquement le benchmark (^FCHI — CAC 40).

Rôle :
  - Séparer proprement le benchmark des actifs du portefeuille
  - Calculer le daily_return du benchmark pour la comparaison relative
  - Consommé par mart_risk_signals pour le calcul de relative_perf

Source : raw_prices
*/

select
    date,
    ticker,
    close_price                                     as benchmark_close,

    close_price / lag(close_price) over (
        order by date
    ) - 1                                           as benchmark_return

from main.raw_prices

where
    ticker = '^FCHI'
    and close_price is not null
    and close_price > 0