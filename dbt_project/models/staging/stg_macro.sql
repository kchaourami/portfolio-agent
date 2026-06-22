/*
stg_macro
=========
Emplacement cible : dbt_project/models/staging/stg_macro.sql

Vue de staging sur raw_macro — retourne UNE seule ligne représentant le
"contexte macro actuel" : la dernière valeur connue de chaque indicateur,
indépendamment de la date exacte de cette valeur.

Pourquoi pas un simple GROUP BY date (version précédente, incorrecte) :
ECB_RATE et EURUSD sont mis à jour quotidiennement, INFLATION_FR
mensuellement avec plusieurs mois de retard de publication — ces 3 séries
n'ont presque jamais la même date la plus récente. Grouper par date exacte
produit des lignes presque vides.

Chaque indicateur est accompagné de sa propre date d'observation
(ecb_rate_date, eurusd_date, inflation_fr_date) — important pour la
transparence : l'Agent Analyste (ou le mémoire) doit pouvoir voir que
inflation_fr peut dater de plusieurs mois, sans la présenter comme une
valeur du jour.

Source : raw_macro (table écrite par macro_collector.py)
Consommé par : futur mart de l'Agent Macro / prompt [CONTEXTE MACRO]
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