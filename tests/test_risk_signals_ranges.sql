/*
Test custom : vérifie que les indicateurs calculés sont dans des plages réalistes.

Convention dbt : ce test DOIT retourner 0 ligne pour passer.
S'il retourne des lignes, ce sont des anomalies à investiguer.

Seuils choisis :
  daily_return  : entre -50% et +50% (jamais atteint en conditions normales)
  vol_20d       : entre 0 et 100% annualisé
  drawdown      : toujours ≤ 0 (un drawdown positif est impossible)
  volume_ratio  : toujours positif
*/

select
    date,
    ticker,
    daily_return,
    vol_20d,
    drawdown,
    volume_ratio_20d,
    'valeur_hors_plage' as anomalie

from {{ ref('mart_risk_signals') }}

where
    -- daily_return impossible (sauf circuit breaker extrême)
    (daily_return < -0.50 or daily_return > 0.50)

    -- vol_20d ne peut pas être négatif
    or (vol_20d is not null and vol_20d < 0)

    -- drawdown doit être ≤ 0 par définition mathématique
    or (drawdown is not null and drawdown > 0.001)   -- tolérance 0.1% pour arrondi

    -- volume_ratio ne peut pas être négatif
    or (volume_ratio_20d is not null and volume_ratio_20d < 0)