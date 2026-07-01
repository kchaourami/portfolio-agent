/*
Vue de staging sur raw_macro — retourne une seule ligne représentant le
"contexte macro actuel" : la dernière valeur connue de chaque indicateur,
indépendamment de la date exacte de cette valeur.

Source : raw_macro (table écrite par macro_collector.py)
Consommé par : mart de l'Agent Macro / prompt [CONTEXTE MACRO]
*/

with latest_per_series as (
    select
        series_key,
        date,
        value,
        row_number() over (
            partition by series_key
            order by date desc
        ) as rn

    from main.raw_macro
)

select
    max(case when series_key = 'ECB_RATE' then value end)     as ecb_rate,
    max(case when series_key = 'ECB_RATE' then date end)      as ecb_rate_date,

    max(case when series_key = 'EURUSD' then value end)       as eurusd,
    max(case when series_key = 'EURUSD' then date end)        as eurusd_date,

    max(case when series_key = 'INFLATION_FR' then value end) as inflation_fr,
    max(case when series_key = 'INFLATION_FR' then date end)  as inflation_fr_date

from latest_per_series
where rn = 1