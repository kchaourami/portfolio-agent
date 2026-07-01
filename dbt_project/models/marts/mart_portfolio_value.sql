/*
Table de valorisation du portefeuille.

Joint les prix de marché (mart_risk_signals) avec les positions
de l'utilisateur (portfolio) pour calculer :
  - market_value  : valeur de marché actuelle de chaque ligne
  - pnl           : profit/loss latent en euros
  - pnl_pct       : profit/loss latent en pourcentage
  - weight        : poids de la ligne dans le portefeuille total
  - sector        : secteur de l'actif (ajouté pour SECTOR_CONCENTRATION)
  - relative_perf_5d : sous/sur-performance vs CAC 40 (ajouté pour le
                        prompt de l'Agent Analyste — "Comparaison CAC 40")
  - return_20d, volume_ratio_20d, asset_type : ajoutés pour le Decision
                        Engine (decision_engine.py) — momentum 20j,
                        volume anormal, et distinction action/ETF

Consommé par : Agent Analyste, decision_engine.py, dashboard Streamlit,
risk_calculator.py

Note : si la table portfolio est vide (pas encore de positions saisies),
ce modèle retourne un DataFrame vide — comportement attendu.
*/

with

-- Dernier prix connu pour chaque ticker
latest_prices as (
    select distinct on (ticker)
        ticker,
        date        as price_date,
        close_price as latest_close,
        sector,
        asset_type,
        daily_return,
        return_5d,
        return_20d,
        relative_perf_5d,
        vol_20d,
        drawdown,
        volume_ratio_20d,
        signal_count

    from {{ ref('mart_risk_signals') }}
    order by ticker, date desc
),

-- Jointure avec les positions
portfolio_valued as (
    select
        p.ticker,
        p.quantity,
        p.purchase_price,
        p.purchase_date,
        p.label,

        lp.price_date,
        lp.latest_close,
        lp.sector,
        lp.asset_type,
        lp.daily_return,
        lp.return_5d,
        lp.return_20d,
        lp.relative_perf_5d,
        lp.vol_20d,
        lp.drawdown,
        lp.volume_ratio_20d,
        lp.signal_count,

        -- Valeur de marché : quantité × prix actuel
        round(p.quantity * lp.latest_close, 2)          as market_value,

        -- PnL latent en euros
        round(
            p.quantity * (lp.latest_close - p.purchase_price),
            2
        )                                               as pnl,

        -- PnL latent en %
        round(
            lp.latest_close / nullif(p.purchase_price, 0) - 1,
            6
        )                                               as pnl_pct

    from main.portfolio p
    left join latest_prices lp on p.ticker = lp.ticker
),

-- Calcul du poids dans le portefeuille total
total as (
    select sum(market_value) as total_value
    from portfolio_valued
),

final as (
    select
        pv.*,
        round(
            pv.market_value / nullif(t.total_value, 0),
            4
        )                                               as weight

    from portfolio_valued pv
    cross join total t
)

select * from final
order by market_value desc