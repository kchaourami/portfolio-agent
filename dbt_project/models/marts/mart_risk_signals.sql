/*
Table principale consommée par l'Agent Risk.

Calcule pour chaque (ticker, date) :
  - return_5d          : rendement glissant sur 5 jours
  - return_20d         : rendement glissant sur 20 jours
  - vol_20d            : volatilité glissante sur 20 jours (écart-type des daily_return)
  - vol_5d             : volatilité courte — détecte les pics soudains
  - drawdown           : recul depuis le plus haut historique (rolling max)
  - volume_ratio_20d   : volume du jour / volume moyen 20j — détecte les anomalies
  - relative_perf_5d   : surperformance ou sous-performance vs CAC 40 sur 5 jours

Seuils d'alerte (commentés pour référence — appliqués dans risk_calculator.py) :
  vol_20d > 0.02           → volatilité élevée
  drawdown < -0.05         → drawdown significatif
  volume_ratio_20d > 2.0   → volume anormal
  relative_perf_5d < -0.03 → sous-performance vs benchmark

Source   : stg_prices (JOIN) stg_benchmark
*/

with

-- 1. Base : daily_return recalculé proprement par ticker
prices_with_return as (
    select
        date,
        ticker,
        isin,
        company_name,
        asset_type,
        sector,
        close_price,
        volume,
        source,

        -- Rendement journalier recalculé ici pour garantir la cohérence
        close_price / nullif(
            lag(close_price) over (
                partition by ticker
                order by date
            ),
            0
        ) - 1 as daily_return

    from {{ ref('stg_prices') }}
),

--2. Indicateurs glissants calculés par ticker
indicators as (
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
        source,

        -- Rendement 5 jours : variation du prix sur les 5 derniers jours
        close_price / nullif(
            lag(close_price, 5) over (
                partition by ticker order by date
            ),
            0
        ) - 1                                               as return_5d,

        -- Rendement 20 jours
        close_price / nullif(
            lag(close_price, 20) over (
                partition by ticker order by date
            ),
            0
        ) - 1                                               as return_20d,

        -- Volatilité 5 jours (écart-type des daily_return sur 5 jours)
        stddev(daily_return) over (
            partition by ticker
            order by date
            rows between 4 preceding and current row
        )                                                   as vol_5d,

        -- Volatilité 20 jours — indicateur de risque principal
        stddev(daily_return) over (
            partition by ticker
            order by date
            rows between 19 preceding and current row
        )                                                   as vol_20d,

        -- Plus haut historique glissant sur 252 jours (environ 1 an)
        max(close_price) over (
            partition by ticker
            order by date
            rows between 251 preceding and current row
        )                                                   as rolling_high_252d,

        -- Volume moyen sur 20 jours
        avg(volume) over (
            partition by ticker
            order by date
            rows between 19 preceding and current row
        )                                                   as avg_volume_20d

    from prices_with_return
),

-- 3. Drawdown et volume ratio — calculés depuis indicators
with_drawdown as (
    select
        *,

        -- Drawdown : recul par rapport au plus haut 252j
        close_price / nullif(rolling_high_252d, 0) - 1     as drawdown,

        -- Volume ratio : combien de fois le volume du jour est supérieur à la moyenne 20j
        case
            when avg_volume_20d > 0
            then volume / avg_volume_20d
            else null
        end                                                 as volume_ratio_20d

    from indicators
),

-- 4. Join avec le benchmark pour la performance relative
final as (
    select
        p.date,
        p.ticker,
        p.isin,
        p.company_name,
        p.asset_type,
        p.sector,
        p.close_price,
        p.volume,
        p.source,

        -- Indicateurs de rendement
        round(p.daily_return, 6)                            as daily_return,
        round(p.return_5d, 6)                               as return_5d,
        round(p.return_20d, 6)                              as return_20d,

        -- Indicateurs de risque
        round(p.vol_5d, 6)                                  as vol_5d,
        round(p.vol_20d, 6)                                 as vol_20d,
        round(p.drawdown, 6)                                as drawdown,
        round(p.volume_ratio_20d, 4)                        as volume_ratio_20d,

        -- Performance relative vs benchmark (5 jours)
        round(
            p.return_5d - b.benchmark_return_5d,
            6
        )                                                   as relative_perf_5d,

        -- Score de signal brut : nombre d'indicateurs en zone d'alerte
        (
            case when p.drawdown       < -0.05   then 1 else 0 end
          + case when p.vol_20d        > 0.02    then 1 else 0 end
          + case when p.volume_ratio_20d > 2.0   then 1 else 0 end
          + case when p.return_5d      < -0.05   then 1 else 0 end
        )                                                   as signal_count

    from with_drawdown p

    left join (
        -- Rendement 5j du benchmark — agrégé par date
        select
            date,
            benchmark_close / nullif(
                lag(benchmark_close, 5) over (order by date),
                0
            ) - 1   as benchmark_return_5d
        from {{ ref('stg_benchmark') }}
    ) b on p.date = b.date
)

select * from final
order by ticker, date